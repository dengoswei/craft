#include "raft.h"
#include "raft_impl.h"

using namespace std;

namespace {

using namespace raft;

std::unique_ptr<Message> buildPropMsg(
        uint64_t prev_index, gsl::array_view<gsl::cstring_view<>> entries)
{
    auto msg = make_unique<Message>();
    assert(nullptr != msg);

    msg->set_index(prev_index);
    msg->set_type(MessageType::MsgProp);
    for (auto idx = size_t{0}; idx < entries.length(); ++idx) {
        auto entry = msg->add_entries();
        assert(nullptr != entry);

        entry->set_type(EntryType::EntryNormal);
        entry->set_data(entries[idx].data(), entries[idx].size());
    }

    assert(static_cast<size_t>(msg->entries_size()) == entries.size());
    assert(0 < msg->entries_size());
    return msg;
}

}

namespace raft {


Raft::Raft(
        uint64_t logid, 
        uint64_t selfid, 
        const std::set<uint64_t>& group_ids, 
        int election_timeout, 
        RaftCallBack callback)
    : logid_(logid)
    , selfid_(selfid)
    , raft_impl_(make_unique<RaftImpl>(
                logid, selfid, group_ids, election_timeout))
    , callback_(callback)
{
    assert(nullptr != raft_impl_);
}

Raft::~Raft() = default;


raft::ErrorCode Raft::Step(const raft::Message& msg)
{
    uint64_t meta_seq = 0ull;
    uint64_t log_idx = 0ull;
    uint64_t log_seq = 0ull;

    unique_ptr<HardState> hs;
    vector<unique_ptr<Entry>> vec_entries;
    vector<unique_ptr<Message>> vec_rsp;

    uint64_t prev_commited_index = 0ull;
    // 1.
    {
        lock_guard<mutex> lock(raft_mutex_);

        assert(nullptr != raft_impl_);
        prev_commited_index = raft_impl_->getCommitedIndex();

        auto rsp_msg_type = raft_impl_->step(msg);
        vec_rsp = raft_impl_->produceRsp(msg, rsp_msg_type);

        tie(meta_seq, log_idx, log_seq) = raft_impl_->getStoreSeq();
        if (0ull != meta_seq) {
            hs = raft_impl_->getCurrentHardState();
            assert(nullptr != hs);
        }

        if (0ull != log_idx) {
            vec_entries = raft_impl_->getLogEntriesAfter(log_idx);
        }
        else {
            assert(0ull == log_seq);
        }
    }

    // 2.
    int ret = 0;
    if (0ull != meta_seq || 0 != log_seq) {
        // strictly inc store_seq make-sure no dirty data
        ret = callback_.write(
                meta_seq, move(hs), log_seq, move(vec_entries));
        if (0 != ret) {
            logdebug("selfid %" PRIu64 " callback_.write meta_seq %"
                    PRIu64 " hs %p"
                    " log_seq %" PRIu64 " vec_entries.size %zu ret %d", 
                    raft_impl_->getSelfId(), meta_seq, hs.get(), 
                    log_seq, vec_entries.size(), ret);
            return raft::ErrorCode::STORAGE_WRITE_ERROR;
        }
    }

    // 3.
    assert(0 == ret);
    if (false == vec_rsp.empty()) {
        ret = callback_.send(move(vec_rsp));
        if (0 != ret) {
            logdebug("selfid %" PRIu64 " callback_.send vec_rsp.size %zu", 
                    raft_impl_->getSelfId(), vec_rsp.size());
        }
    }

    // 4. 
    bool update = false;
    {
        lock_guard<mutex> lock(raft_mutex_);
        raft_impl_->commitedStoreSeq(meta_seq, log_idx, log_seq);
        assert(prev_commited_index <= raft_impl_->getCommitedIndex());
        if (prev_commited_index < raft_impl_->getCommitedIndex()) {
            update = true;
        }
    }

    if (update) {
        raft_cv_.notify_all();
    }

    return raft::ErrorCode::OK;
}

std::tuple<raft::ErrorCode, uint64_t>
Raft::Propose(
        uint64_t prev_index,
        gsl::array_view<gsl::cstring_view<>> entries)
{
    assert(size_t{0} < entries.length());

    // 1. pack msg
    auto prop_msg = buildPropMsg(prev_index, entries);
    assert(nullptr != prop_msg);

    // prop_lock: keep prop in order
    lock_guard<mutex> prop_lock(raft_prop_mutex_);
    // 2.
    {
        lock_guard<mutex> lock(raft_mutex_);
        if (raft_impl_->getSelfId() != raft_impl_->getLeader()) {
            return make_tuple(raft::ErrorCode::NOT_LEADER, 0ull);
        }

        uint64_t last_index = raft_impl_->getLastLogIndex();
        if (0ull != prev_index && prev_index != last_index) {
            return make_tuple(raft::ErrorCode::OCCUPY, 0ull);
        }

        assert(0ull == prev_index || prev_index == last_index);
        prop_msg->set_index(last_index);
        prop_msg->set_logid(raft_impl_->getLogId());
        prop_msg->set_term(raft_impl_->getTerm());
        prop_msg->set_to(raft_impl_->getSelfId());
    }

    auto ret = Step(*prop_msg);
    if (ErrorCode::OK != ret) {
        logerr("Step ret %d", ret);
        return make_tuple(ret, 0ull);
    }

    return make_tuple(raft::ErrorCode::OK, prop_msg->index() + 1ull);
}

std::tuple<raft::ErrorCode, uint64_t, std::unique_ptr<Entry>> 
Raft::Get(uint64_t index)
{
    assert(0ull < index);
    uint64_t commited_index = 0ull;
    {
        lock_guard<mutex> lock(raft_mutex_);
        commited_index = raft_impl_->getCommitedIndex();
        if (0ull != commited_index && 
                index > raft_impl_->getLastLogIndex()) {
            return make_tuple(raft::ErrorCode::INVALID_INDEX, 0ull, nullptr);
        }

        if (index > commited_index) {
            return make_tuple(
                    raft::ErrorCode::UNCOMMITED_INDEX, 
                    commited_index, nullptr);
        }
    }

    auto entry = callback_.read(index);
    if (nullptr == entry) {
        return make_tuple(raft::ErrorCode::STORAGE_READ_ERROR, 0ull, nullptr);
    }

    return make_tuple(raft::ErrorCode::OK, commited_index, move(entry));
}

std::tuple<raft::ErrorCode, uint64_t>
Raft::TrySet(uint64_t index, gsl::array_view<gsl::cstring_view<>> entries)
{
    return Propose(index, entries);
}


void Raft::Wait(uint64_t index)
{
    unique_lock<mutex> lock(raft_mutex_);
    if (index <= raft_impl_->getCommitedIndex()) {
        return ;
    }

    raft_cv_.wait(lock, [&]() -> bool {
        return index <= raft_impl_->getCommitedIndex();
    });
}

bool Raft::WaitFor(uint64_t index, std::chrono::milliseconds timeout)
{
    unique_lock<mutex> lock(raft_mutex_);
    if (index <= raft_impl_->getCommitedIndex()) {
        return true;
    }

    auto time_point = chrono::system_clock::now() + timeout;
    return raft_cv_.wait_until(lock, time_point, 
            [&]() -> bool {
                return index <= raft_impl_->getCommitedIndex();
            });

}


raft::ErrorCode Raft::TryToBecomeLeader()
{
    Message msg_null;
    msg_null.set_type(MessageType::MsgNull);
    msg_null.set_logid(GetLogId());
    msg_null.set_to(GetSelfId());
    {
        lock_guard<mutex> lock(raft_mutex_);
        assert(nullptr != raft_impl_);
        if (raft_impl_->getSelfId() == raft_impl_->getLeader()) {
            // already a leader
            return raft::ErrorCode::OK;
        }

        assert(RaftRole::LEADER != raft_impl_->getRole());
        raft_impl_->makeElectionTimeout(chrono::system_clock::now());

        msg_null.set_term(raft_impl_->getTerm());
    }

    return Step(msg_null); 
}

raft::ErrorCode Raft::MakeTimeoutHeartbeat()
{
    Message msg_null;
    msg_null.set_type(MessageType::MsgNull);
    msg_null.set_logid(GetLogId());
    msg_null.set_to(GetSelfId());
    {
        lock_guard<mutex> lock(raft_mutex_);
        assert(nullptr != raft_impl_);
        if (raft_impl_->getSelfId() != raft_impl_->getLeader()) {
            // not a leader
            return raft::ErrorCode::NOT_LEADER;
        }

        assert(RaftRole::LEADER == raft_impl_->getRole());
        raft_impl_->makeHeartbeatTimeout(chrono::system_clock::now());

        msg_null.set_term(raft_impl_->getTerm());
    }

    return Step(msg_null);
}

bool Raft::IsFollower() 
{
    return checkRole(RaftRole::FOLLOWER);
}

bool Raft::IsLeader()
{
    return checkRole(RaftRole::LEADER);
}

bool Raft::IsCandidate()
{
    return checkRole(RaftRole::CANDIDATE);
}

bool Raft::checkRole(raft::RaftRole role)
{
    lock_guard<mutex> lock(raft_mutex_);
    assert(nullptr != raft_impl_);
    return role == raft_impl_->getRole();
}

uint64_t Raft::GetTerm()
{
    lock_guard<mutex> lock(raft_mutex_);
    assert(nullptr != raft_impl_);
    return raft_impl_->getTerm();
}

} // namespace raft;


