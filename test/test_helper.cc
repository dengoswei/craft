#include "test_helper.h"
#include "raft_impl.h"
#include "raft.h"
#include "raft.pb.h"
#include "utils.h"



using namespace std;
using namespace raft;

namespace test {

uint64_t LOGID = 1ull;
std::set<uint64_t> GROUP_IDS{1ull, 2ull, 3ull};


std::vector<std::unique_ptr<raft::Message>>
    apply(std::map<uint64_t, std::unique_ptr<raft::RaftImpl>>& map_raft, 
          const std::vector<std::unique_ptr<raft::Message>>& vec_input_msg)
{
    vector<unique_ptr<Message>> vec_msg;
    for (const auto& msg : vec_input_msg) {
        assert(nullptr != msg);
        assert(map_raft.end() != map_raft.find(msg->to()));
        auto& raft = map_raft[msg->to()];
        assert(nullptr != raft);
        
        MessageType rsp_msg_type = MessageType::MsgNull;
        TickTime ta("%s msg type %d", __func__, static_cast<int>(msg->type()));
        {
            TickTime t("step msg type %d entries_size %d",
                    static_cast<int>(msg->type()), 
                    msg->entries_size());
            rsp_msg_type = raft->step(*msg);
        }

        vector<unique_ptr<Message>> vec_rsp_msg;
        {
            TickTime t("produceRsp msg type %d rsp_msg_type %d", 
                    static_cast<int>(msg->type()), 
                    static_cast<int>(rsp_msg_type));
            vec_rsp_msg = raft->produceRsp(*msg, rsp_msg_type);
        }
        for (auto& rsp_msg : vec_rsp_msg) {
            assert(nullptr != rsp_msg);
            assert(msg->to() == rsp_msg->from());
            vec_msg.emplace_back(move(rsp_msg));
            assert(nullptr == rsp_msg);
        }
    }

    return vec_msg;
}

void
apply_until(
        std::map<uint64_t, std::unique_ptr<raft::RaftImpl>>& map_raft, 
        std::vector<std::unique_ptr<raft::Message>>&& vec_msg)
{
    while (false == vec_msg.empty()) {
        for (auto& msg : vec_msg) {
            assert(nullptr != msg);
            logdebug("TEST-INFO msg from %" PRIu64 
                    " to %" PRIu64 " term %" PRIu64 " type %d", 
                    msg->from(), msg->to(), msg->term(), 
                    static_cast<int>(msg->type()));
        }

        TickTime t("vec_msg.size %zu", vec_msg.size());
        vec_msg = apply(map_raft, vec_msg);
    }
}

std::map<uint64_t, std::unique_ptr<raft::RaftImpl>>
    build_rafts(
            uint64_t logid, 
            const std::set<uint64_t>& group_ids, 
            int min_timeout, int max_timeout)
{
    assert(0 < min_timeout);
    assert(min_timeout <= max_timeout);
    map<uint64_t, unique_ptr<RaftImpl>> map_raft;
    for (auto id : group_ids) {
        auto election_timeout = random_int(min_timeout, max_timeout);
        assert(0 < election_timeout);

        auto raft = make_unique<RaftImpl>(
                logid, id, group_ids, election_timeout);
        assert(nullptr != raft);
        assert(RaftRole::FOLLOWER == raft->getRole());
        assert(map_raft.end() == map_raft.find(id));
        map_raft[id] = move(raft);
        assert(nullptr == raft);
    }

    return map_raft;
}

void init_leader(
        const uint64_t logid, 
        const uint64_t leader_id, 
        std::map<uint64_t, std::unique_ptr<raft::RaftImpl>>& map_raft)
{
    assert(map_raft.end() != map_raft.find(leader_id));
    // 1. timeout
    for (auto& id_raft : map_raft) {
        assert(nullptr != id_raft.second);
        assert(logid == id_raft.second->getLogId());
        assert(RaftRole::FOLLOWER == id_raft.second->getRole());
        assert(0ull == id_raft.second->getTerm());
        
        auto tp = chrono::system_clock::now();
        if (leader_id == id_raft.second->getSelfId()) {
            id_raft.second->makeElectionTimeout(tp);
        }
        else {
            id_raft.second->updateActiveTime(tp);
        }
    }

    vector<unique_ptr<Message>> vec_msg;
    {
        auto fake_msg = make_unique<Message>();
        fake_msg->set_logid(logid);
        fake_msg->set_type(MessageType::MsgNull);
        fake_msg->set_to(leader_id);
        vec_msg.emplace_back(move(fake_msg));
    }

    assert(size_t{1} == vec_msg.size());
    apply_until(map_raft, move(vec_msg));
    assert(true == vec_msg.empty());

    for (const auto& id_raft : map_raft) {
        assert(nullptr != id_raft.second);
        if (leader_id == id_raft.first) {
            assert(RaftRole::LEADER == id_raft.second->getRole());
        }
        else {
            assert(RaftRole::FOLLOWER == id_raft.second->getRole());
        }

        assert(1 == id_raft.second->getTerm());
    }
}

std::tuple<
    uint64_t,
    std::set<uint64_t>, 
    std::map<uint64_t, std::unique_ptr<raft::RaftImpl>>>
comm_init(uint64_t leader_id, 
        int min_election_timeout, int max_election_timeout)
{
    auto map_raft = build_rafts(
            LOGID, GROUP_IDS, min_election_timeout, max_election_timeout);
    assert(map_raft.size() == GROUP_IDS.size());
    assert(map_raft.end() != map_raft.find(leader_id));

    init_leader(LOGID, leader_id, map_raft);
    return make_tuple(LOGID, GROUP_IDS, move(map_raft));
}

std::unique_ptr<raft::Raft>
build_raft(
        uint64_t logid, 
        uint64_t selfid, 
        const std::set<uint64_t>& group_ids, 
        int min_election_timeout, int max_election_timeout, 
        raft::RaftCallBack callback)
{
    assert(0 < min_election_timeout);
    assert(min_election_timeout <= max_election_timeout);

    assert(group_ids.end() != group_ids.find(selfid));
    auto election_timeout = 
        random_int(min_election_timeout, max_election_timeout);
    assert(0 < election_timeout);

    return make_unique<Raft>(
            logid, selfid, group_ids, election_timeout, callback);
}

void init_leader(
        const uint64_t logid, 
        const uint64_t leader_id, 
        SendHelper& sender, 
        std::map<uint64_t, std::unique_ptr<raft::Raft>>& map_raft)
{
    assert(map_raft.end() != map_raft.find(leader_id));

    // 1. timeout
    for (auto& id_raft : map_raft) {
        auto& raft = id_raft.second;
        assert(nullptr != raft);
        assert(logid == raft->GetLogId());
        assert(id_raft.first == raft->GetSelfId());

        auto tp = chrono::system_clock::now();
        if (leader_id == raft->GetSelfId()) {
            auto ret_code = raft->TryToBecomeLeader();
            assert(raft::ErrorCode::OK == ret_code);
        } 
    }

    sender.apply_until(map_raft);
    for (const auto& id_raft : map_raft) {
        auto& raft = id_raft.second;
        assert(nullptr != raft);
        if (leader_id == raft->GetSelfId()) {
            assert(true == raft->IsLeader());
        }
        else {
            assert(true == raft->IsFollower());
        }
    }
}

std::tuple<
    uint64_t, 
    std::set<uint64_t>, 
    std::map<uint64_t, std::unique_ptr<StorageHelper>>, 
    std::map<uint64_t, std::unique_ptr<raft::Raft>>>
comm_init(
        uint64_t leader_id, 
        SendHelper& sender, 
        int min_election_timeout, int max_election_timeout)
{
    map<uint64_t, unique_ptr<StorageHelper>> map_store;
    map<uint64_t, unique_ptr<Raft>> map_raft;

    auto logid = LOGID;
    auto group_ids = GROUP_IDS;
    for (auto id : group_ids) {
        assert(map_store.end() == map_store.find(id));
        map_store[id] = make_unique<StorageHelper>();
        assert(nullptr != map_store[id]);

        StorageHelper* store = map_store[id].get();
        assert(nullptr != store);

        RaftCallBack callback;
        callback.read = [=](uint64_t log_index) {
            assert(nullptr != store);
            return store->read(log_index);
        };

        callback.write = [=](
                unique_ptr<HardState>&& hs, 
                vector<unique_ptr<Entry>>&& vec_entries) {
            assert(nullptr != store);
            return store->write(move(hs), move(vec_entries));
        };

        callback.send = [&](vector<unique_ptr<Message>>&& vec_msg) {
            for (auto& msg : vec_msg) {
                sender.send(move(msg));
            }
            return 0;
        };

        auto raft = build_raft(logid, id, group_ids, 50, 100, callback);
        assert(nullptr != raft);
        assert(map_raft.end() == map_raft.find(id));
        map_raft[id] = move(raft);
    }

    assert(map_raft.size() == map_store.size());
    assert(map_raft.size() == GROUP_IDS.size());
    assert(map_raft.end() != map_raft.find(leader_id));

    init_leader(logid, leader_id, sender, map_raft);
    return make_tuple(logid, move(group_ids), move(map_store), move(map_raft));
}


int StorageHelper::write(
        std::unique_ptr<raft::HardState>&& hs)
{
    // hold lock;
    if (nullptr != hs) {
        if (max_meta_seq_ < hs->seq()) {
            max_meta_seq_ = hs->seq();
            meta_info_ = move(hs);
//            logdebug("meta_info_ term %" PRIu64 
//                    " vote %" PRIu64 " commit %u" PRIu64 
//                    " seq %" PRIu64 " max_meta_seq_ %" PRIu64, 
//                    meta_info_->term(), meta_info_->vote(), 
//                    meta_info_->commit(), meta_info_->seq(), 
//                    max_meta_seq_);
        }
    }
    return 0;
}

int StorageHelper::write(
        std::vector<std::unique_ptr<raft::Entry>>&& vec_entries)
{
    // hold lock;
    if (false == vec_entries.empty()) {
        assert(nullptr != vec_entries.front());
        for (auto& entry : vec_entries) {
            assert(nullptr != entry);
            if (max_log_seq_ <= entry->seq()) {
                // TODO
//                logdebug("seq %" PRIu64 " index %" PRIu64 " term %" PRIu64, 
//                        entry->seq(), entry->index(), entry->term());
                max_log_seq_ = entry->seq();
                log_entries_[entry->index()] = move(entry);
                assert(nullptr == entry);
            }
        }
    }

    return 0;
}

int StorageHelper::write(
        std::unique_ptr<raft::HardState>&& hs, 
        std::vector<std::unique_ptr<raft::Entry>>&& vec_entries)
{
    lock_guard<mutex> lock(mutex_);
    auto ret = write(move(hs));
    if (0 != ret) {
        return ret;
    }

    return write(move(vec_entries));
}

std::unique_ptr<raft::Entry> StorageHelper::read(uint64_t log_index)
{
    if (0ull == log_index) {
        return nullptr;
    }

    lock_guard<mutex> lock(mutex_);
    if (log_entries_.end() == log_entries_.find(log_index)) {
        return nullptr;
    }

    auto& entry = log_entries_[log_index];
    assert(nullptr != entry);
    assert(entry->index() == log_index);
//    logdebug("seq %" PRIu64 " index %" PRIu64 " term %" PRIu64, 
//            entry->seq(), entry->index(), entry->term());
    return make_unique<Entry>(*entry);
}

void SendHelper::send(std::unique_ptr<raft::Message>&& msg)
{
    assert(nullptr != msg);
    logdebug("msg: from %" PRIu64 " to %" PRIu64 " msg_type %d", 
            msg->from(), msg->to(), static_cast<int>(msg->type()));
    lock_guard<mutex> lock(msg_queue_mutex_);
    msg_queue_.push_back(move(msg));
}

size_t SendHelper::apply(
        std::map<uint64_t, std::unique_ptr<raft::Raft>>& map_raft)
{
    deque<unique_ptr<Message>> prev_msg_queue;
    {
        lock_guard<mutex> lock(msg_queue_mutex_);
        prev_msg_queue.swap(msg_queue_);
    }
    for (auto& msg : prev_msg_queue) {
        assert(nullptr != msg);
        assert(map_raft.end() != map_raft.find(msg->to()));
        auto& raft = map_raft[msg->to()];
        assert(nullptr != raft);

        auto ret_code = raft->Step(*msg);
        assert(ErrorCode::OK == ret_code);
    }
    return prev_msg_queue.size();
}

void SendHelper::apply_until(
        std::map<uint64_t, std::unique_ptr<raft::Raft>>& map_raft)
{
    auto check_empty = [&]() -> bool {
        lock_guard<mutex> lock(msg_queue_mutex_);
        return msg_queue_.empty();
    };

    while (false == check_empty()) {
        size_t apply_count = apply(map_raft);
        logdebug("apply_count %zu msg_queue_.size %zu", 
                apply_count, msg_queue_.size());
    }
}

bool SendHelper::empty() 
{
    lock_guard<mutex> lock(msg_queue_mutex_);
    return msg_queue_.empty();
}


std::unique_ptr<raft::Message> 
buildMsgProp(
        uint64_t logid, uint64_t leader_id, 
        uint64_t term, uint64_t prev_index, int entries_size)
{ 
    assert(0ull < leader_id);
    assert(0ull < term);
    assert(0ull <= prev_index);
    assert(0 < entries_size);
    auto prop_msg = make_unique<Message>();
    assert(nullptr != prop_msg);

    prop_msg->set_logid(logid);
    prop_msg->set_type(MessageType::MsgProp);
    prop_msg->set_to(leader_id);
    prop_msg->set_term(term);
    prop_msg->set_index(prev_index);

    RandomStrGen<100, 200> str_gen;
    for (auto i = 0; i < entries_size; ++i) {
        auto entry = prop_msg->add_entries();
        assert(nullptr != entry);

        entry->set_type(EntryType::EntryNormal);
        entry->set_data(str_gen.Next());
    }

    return prop_msg;
}

std::vector<std::unique_ptr<raft::Message>>
batchBuildMsgProp(
        uint64_t logid, uint64_t leader_id, 
        uint64_t term, uint64_t prev_index, 
        int batch_size, int entries_size)
{
    assert(0 < batch_size);
    assert(0 < entries_size);
    vector<unique_ptr<Message>> vec_msg;
    vec_msg.reserve(batch_size);
    for (auto iter_time = 0; iter_time < batch_size; ++iter_time) {
        auto prop_msg = buildMsgProp(
                logid, leader_id, term, prev_index, entries_size);    
        assert(nullptr != prop_msg);
        assert(prev_index == prop_msg->index());
        assert(entries_size == prop_msg->entries_size());
        prev_index += entries_size;
        vec_msg.emplace_back(move(prop_msg));
    }

    assert(vec_msg.size() == static_cast<size_t>(batch_size));
    return vec_msg;
}


std::unique_ptr<raft::Message>
buildMsgNull(uint64_t to_id, uint64_t logid, uint64_t term)
{
    auto msg_null = make_unique<Message>();
    
    msg_null->set_type(MessageType::MsgNull);
    msg_null->set_term(term);
    msg_null->set_logid(logid);
    msg_null->set_to(to_id);

    return msg_null;
}



} // namespace test


