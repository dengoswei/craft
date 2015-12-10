#include <algorithm>
#include "raft_impl.h"


using namespace std;

namespace {

using namespace raft;

inline void assert_role(const RaftImpl& raft_impl, RaftRole expected_role)
{
    hassert(raft_impl.getRole() == expected_role, 
            "expected role %d but %d", static_cast<int>(expected_role), 
            static_cast<int>(raft_impl.getRole()));
}

inline void assert_term(RaftImpl& raft_impl, uint64_t term)
{
    hassert(raft_impl.getTerm() == term, 
            "expected term %" PRIu64 " but term %" PRIu64, 
            raft_impl.getTerm(), term);
}


// true_cnt, false_cnt
std::tuple<int, int> countVotes(const std::map<uint64_t, bool>& votes)
{
    int true_cnt = 0;
    int false_cnt = 0;
    for (const auto& v : votes) {
        if (v.second) {
            ++true_cnt;
        } else {
            ++false_cnt;
        }
    }

    return make_tuple(true_cnt, false_cnt);
}

bool hasReplicateOnMajority(
        uint64_t new_match_index, 
        const std::map<uint64_t, uint64_t>& match_indexes)
{
    auto major_count = 0;
    for (const auto& id_match_idx : match_indexes) {
        if (id_match_idx.second >= new_match_index) {
            ++major_count;
        }
    }

    // major_count + 1 >= (match_indexes.size() / 2 + 1)
    return major_count >= (match_indexes.size() / 2);
}

gsl::array_view<const Entry> make_entries(const raft::Message& msg)
{
    return gsl::array_view<const Entry>{
        0 == msg.entries_size() ? nullptr : &msg.entries(0), 
        static_cast<size_t>(msg.entries_size())
    };
}

gsl::array_view<const Entry> 
shrinkEntries(uint64_t conflict_index, gsl::array_view<const Entry> entries)
{
    if (size_t(0) == entries.length() || 0ull == conflict_index) {
        return entries;
    }

    assert(size_t(0) < entries.length());
    uint64_t base_index = entries[0].index();
    assert(conflict_index >= base_index);
    size_t idx = conflict_index - base_index;
    if (idx >= entries.length()) {
        return gsl::array_view<const Entry>{nullptr};
    }

    return entries.sub(idx);
}

inline uint64_t getBaseLogIndex(std::deque<std::unique_ptr<Entry>>& logs)
{
    if (true == logs.empty()) {
        return 0ull;
    }

    assert(nullptr != logs.front());
    assert(0ull < logs.front()->index());
    return logs.front()->index();
}

size_t truncateLogs(
        std::deque<std::unique_ptr<Entry>>& logs, uint64_t truncate_index)
{
    uint64_t base_index = getBaseLogIndex(logs);
    size_t idx = truncate_index - base_index;
    if (idx >= logs.size()) {
        return size_t{0};
    }

    size_t prev_size = logs.size();
    logs.erase(logs.begin() + idx, logs.end());
    return prev_size - logs.size();
}


} // namespace 


namespace raft {

const uint64_t META_INDEX = 0ull;
const size_t MAX_BATCH_SIZE = 10;

namespace candidate {

MessageType onTimeout(
        RaftImpl& raft_impl, 
        std::chrono::time_point<std::chrono::system_clock> time_now);

} // namespace candidate


namespace follower {

MessageType onTimeout(
            RaftImpl& raft_impl, 
            std::chrono::time_point<std::chrono::system_clock> time_now)
{
    assert_role(raft_impl, RaftRole::FOLLOWER);
    // raft paper: 
    // Followers 5.2
    // if election timeout elapses without receiving AppendEntries
    // RPC from current leader or granting vote to candidate:
    // convert to candidate
    raft_impl.becomeCandidate();
    raft_impl.updateActiveTime(time_now);
    
    return MessageType::MsgVote;
}

MessageType onStepMessage(RaftImpl& raft_impl, const Message& msg)
{
    assert_role(raft_impl, RaftRole::FOLLOWER);
    assert_term(raft_impl, msg.term());

    auto time_now = std::chrono::system_clock::now();
    auto rsp_msg_type = MessageType::MsgNull;
    switch (msg.type()) {

        case MessageType::MsgVote:
            // candidate requestVote msg
            //
            // raft paper: 5.1 & 5.4
            // if votedFor is null or candidateid, and candidate's log is 
            // at least as up-to-date as receiver's log, grant vote
            //
            // MORE DETAIL:
            // - each server will vote for at most one candidate in a
            //   given term, on a first-come-first-served basis;
            // - the voter denies its vote if its own log is more up-to-date
            //   then that of the candidate.
            if (raft_impl.isUpToDate(msg.log_term(), msg.index())) {
                // CAN'T RESET vote_for_
                if (0ull == raft_impl.getVoteFor()) {
                    raft_impl.setVoteFor(false, msg.from());
                    raft_impl.assignStoreSeq(META_INDEX);
                }
                assert(0ull != raft_impl.getVoteFor());
            }

            // check getVoteFor() != msg.from() => reject!
            rsp_msg_type = MessageType::MsgVoteResp;
            raft_impl.updateActiveTime(time_now);
            logdebug("selfid %" PRIu64 
                    " handle MsgVote", raft_impl.getSelfId());
            break;

        case MessageType::MsgHeartbeat:
        {
            // leader heart beat msg
            // => Heartbeat with same term always from active leader
            raft_impl.setLeader(false, msg.from());
            assert(msg.from() == raft_impl.getLeader());

            rsp_msg_type = MessageType::MsgHeartbeatResp;
            raft_impl.updateActiveTime(time_now);
            logdebug("selfid %" PRIu64 
                    " recv heartbeat from leader %" PRIu64, 
                    raft_impl.getSelfId(), msg.from());
        }
            break;
        case MessageType::MsgApp:
        {
            // leader appendEntry msg
            // => AppendEntries always from active leader!
            raft_impl.setLeader(false, msg.from());
            assert(msg.from() == raft_impl.getLeader());

            auto entries = make_entries(msg);
            assert(static_cast<size_t>(
                        msg.entries_size()) == entries.length());

            raft_impl.appendEntries(
                    msg.index(), msg.log_term(), msg.commit(), entries);

            raft_impl.updateActiveTime(time_now);

            if (0 != msg.entries_size()) { 
                rsp_msg_type = MessageType::MsgAppResp;
            }
            // 0 == msg.entries_size() => indicate already up-to-date
        }
            break;

        default:
            logdebug("IGNORE: recv msg type %d", static_cast<int>(msg.type()));
            // TODO ?: MsgProp: redirect to leader ?
            break;
    }

    return rsp_msg_type;
}



} // namespace follower


namespace candidate {

MessageType onTimeout(
        RaftImpl& raft_impl, 
        std::chrono::time_point<std::chrono::system_clock> time_now)
{
    assert_role(raft_impl, RaftRole::CANDIDATE);

    // raft paper:
    // Candidates 5.2
    // On conversion to candidate or election timeout elapse, start election:
    raft_impl.beginVote();
    raft_impl.updateActiveTime(time_now);
    return MessageType::MsgVote;
}

MessageType onStepMessage(RaftImpl& raft_impl, const Message& msg)
{
    assert_role(raft_impl, RaftRole::CANDIDATE);
    assert_term(raft_impl, msg.term());

    auto rsp_msg_type = MessageType::MsgNull;
    switch (msg.type()) {
        case MessageType::MsgVoteResp:
            // collect vote resp
            raft_impl.updateVote(msg.from(), !msg.reject());
            if (raft_impl.isMajorVoteYes()) {
                // step as leader: TODO ? 
                raft_impl.becomeLeader();
                // => immidicate send out headbeat ?
                // raft paper:
                // Rules for Servers, Leaders:
                // upon election: send initial empty AppendEntries RPCs
                // (heartbeat) to each server; repeat during idle periods
                // to prevent election timeouts 5.2
                rsp_msg_type = MessageType::MsgHeartbeat;
            }

            logdebug("selfid %" PRIu64 
                    " handle MsgVoteResp", raft_impl.getSelfId());
            break;
        default:
            logdebug("IGNORE: recv msg type %d", static_cast<int>(msg.type()));
            // TODO: ?
            break;
    }

    return rsp_msg_type;
}

} // namespace candidate


namespace leader {
 
MessageType onTimeout(
        RaftImpl& raft_impl, 
        std::chrono::time_point<std::chrono::system_clock> time_now)
{
    assert_role(raft_impl, RaftRole::LEADER);
    raft_impl.updateActiveTime(time_now);
    return MessageType::MsgNull;
}

MessageType onStepMessage(RaftImpl& raft_impl, const Message& msg)
{
    assert_role(raft_impl, RaftRole::LEADER);
    assert_term(raft_impl, msg.term());

    auto rsp_msg_type = MessageType::MsgNull;
    switch (msg.type()) {
        case MessageType::MsgProp:
        {
            // client prop
            auto entries = make_entries(msg);
            assert(static_cast<size_t>(
                        msg.entries_size()) == entries.length());

            assert(0 == 
                    raft_impl.checkAndAppendEntries(msg.index(), entries));
            rsp_msg_type = MessageType::MsgApp;
        }
            break;

        case MessageType::MsgAppResp:
        {
            // collect appendEntry resp
            // TODO: update commited!
            bool update = raft_impl.updateReplicateState(
                    msg.from(), msg.reject(), msg.reject_hint(), msg.index());
            if (update) {
                rsp_msg_type = MessageType::MsgApp;
            }

            logdebug("selfid(leader) %" PRIu64 " MsgAppResp msg.from %" PRIu64
                    " msg.index %" PRIu64 " reject %d rsp_msg_type %d", 
                    raft_impl.getSelfId(), msg.from(), msg.index(), 
                    static_cast<int>(msg.reject()), 
                    static_cast<int>(rsp_msg_type));
        }
            break;

        case MessageType::MsgHeartbeatResp:
        {
            // collect heartbeat resp

            bool update = raft_impl.updateReplicateState(
                    msg.from(), msg.reject(), msg.reject_hint(), msg.index());
            if (true == update) {
                rsp_msg_type = MessageType::MsgHeartbeat;
            }
            // TODO: logdebug
        }
            break;

        default:
            logdebug("IGNORE: recv msg type %d", static_cast<int>(msg.type()));
            // TODO: ?
            break;
    }

    return rsp_msg_type;
}
    
} // namespace leader

RaftImpl::RaftImpl(
        uint64_t logid, uint64_t selfid, 
        std::set<uint64_t> peer_ids, int election_timeout)
    : logid_(logid)
    , selfid_(selfid)
    , election_timeout_(election_timeout)
    , active_time_(chrono::system_clock::now())
{
    assert(size_t{3} <= peer_ids.size());
    for (auto id : peer_ids) {
        if (id == selfid_) {
            continue;
        }

        peer_ids_.insert(id);
    }

    assert(peer_ids_.end() == peer_ids_.find(selfid_));
    assert(0 < election_timeout_.count());
    becomeFollower();
}

RaftImpl::~RaftImpl() = default;

MessageType RaftImpl::CheckTerm(uint64_t msg_term)
{
    // raft paper: rules for servers: 5.1
    // => If RPC request or response contains term T > currentTerm: 
    //    set currentTerm = T, convert to follower;
    if (msg_term != term_) {
        if (msg_term > term_) {
            becomeFollower();
            setTerm(msg_term);
            setVoteFor(true, 0ull); // reset vote_for_ in new term;
            assignStoreSeq(META_INDEX);
            return MessageType::MsgNull;
        }

        assert(msg_term < term_);
        return MessageType::MsgInvalidTerm;
    }

    assert(msg_term == term_);
    return MessageType::MsgNull;
}

MessageType
RaftImpl::CheckTimout(
        std::chrono::time_point<std::chrono::system_clock> time_now)
{
    if (active_time_ + election_timeout_ < time_now) {
        // TIME OUT:
        assert(nullptr != timeout_handler_);
        return timeout_handler_(*this, time_now);
    }

    return MessageType::MsgNull;
}


MessageType RaftImpl::step(const Message& msg)
{
    assert(msg.logid() == logid_);
    assert(msg.to() == selfid_);

    // 1. check term
    auto rsp_msg_type = CheckTerm(msg.term());
    if (MessageType::MsgNull != rsp_msg_type) {
        assert(MessageType::MsgInvalidTerm == rsp_msg_type);
        return rsp_msg_type;
    }

    assert(msg.term() == term_);
    // 2. check timeout
    rsp_msg_type = CheckTimout(chrono::system_clock::now());
    if (MessageType::MsgNull != rsp_msg_type) {
        assert(MessageType::MsgVote == rsp_msg_type);
        return rsp_msg_type;
    }

    // 3. step message
    return step_handler_(*this, msg);
}

std::vector<std::unique_ptr<Message>> 
RaftImpl::produceRsp(
        const Message& req_msg, MessageType rsp_msg_type)
{
    assert(req_msg.logid() == logid_);
    assert(req_msg.to() == selfid_);

    vector<unique_ptr<Message>> vec_msg;
    if (MessageType::MsgNull == rsp_msg_type) {
        return vec_msg;
    }

    Message msg_template;
    msg_template.set_type(rsp_msg_type);
    msg_template.set_logid(logid_);
    msg_template.set_from(selfid_);
    msg_template.set_term(term_);
    msg_template.set_to(req_msg.from());
    switch (rsp_msg_type) {

    case MessageType::MsgVote:
        // raft paper:
        // RequestVote RPC Arguments:
        // - term
        // - candidicateId
        // - lastLogIndex
        // - lastLogTerm
        msg_template.set_index(getLastLogIndex());
        msg_template.set_log_term(getLastLogTerm());
        for (auto peer_id : peer_ids_) {
            vec_msg.emplace_back(make_unique<Message>(msg_template));
            auto& rsp_msg = vec_msg.back();
            assert(nullptr != rsp_msg);
            rsp_msg->set_to(peer_id);
        }
        assert(vec_msg.size() == peer_ids_.size());

        logdebug("MsgVote term %" PRIu64 " candidate %" PRIu64 
                " lastLogIndex %" PRIu64 " lastLogTerm %" PRIu64, 
                term_, selfid_, msg_template.index(), 
                msg_template.log_term());
        break;

    case MessageType::MsgVoteResp:
    {
        // raft paper:
        // RequestVote RPC Results:
        // - term 
        // - voteGranted
        assert(0ull != req_msg.from());

        vec_msg.emplace_back(make_unique<Message>(msg_template));
        auto& rsp_msg = vec_msg.back();
        assert(nullptr != rsp_msg);
        rsp_msg->set_reject(req_msg.from() != getVoteFor());
        logdebug("MsgVoteResp term %" PRIu64 " req_msg.from %" PRIu64 
                " getVoteFor %" PRIu64 " reject %d", 
                term_, req_msg.from(), getVoteFor(), 
                static_cast<int>(rsp_msg->reject()));
    }
        break;
    
    case MessageType::MsgApp:
    {
        // req_msg.type() == MessageType::MsgProp
        if (0ull == req_msg.from()) {
            if (MessageType::MsgProp == req_msg.type()) {
                vec_msg = batchBuildMsgAppUpToDate(MAX_BATCH_SIZE);
            }
            else {
                vec_msg = batchBuildMsgApp(MAX_BATCH_SIZE);
            }
        }
        else {
            assert(0ull != req_msg.from());
            // catch-up mode maybe
            assert(MessageType::MsgAppResp == req_msg.type());
            auto rsp_msg = buildMsgApp(
                    req_msg.from(), req_msg.index(), MAX_BATCH_SIZE);
            if (nullptr != rsp_msg) {
                vec_msg.emplace_back(move(rsp_msg));
                assert(size_t{1} == vec_msg.size());
            }
            assert(nullptr == rsp_msg);
            logdebug("MsgApp selfid %" PRIu64 " req_msg.from %" PRIu64 
                    " req_msg.index %" PRIu64 " vec_msg.size %zu", 
                    selfid_, req_msg.from(), req_msg.index(), vec_msg.size());
        }
    }
        break;

    case MessageType::MsgAppResp:
    {
        // req_msg.type() == MessageType::MsgApp
        // raft paper:
        // AppendEntries RPC, Results:
        // - reply false if term < currentTerm
        // - reply false if log don't contain an entry at prevLogIndex
        //   whose term matchs prevLogTerm
        assert(0ull != req_msg.from());

        vec_msg.emplace_back(make_unique<Message>(msg_template));
        auto& rsp_msg = vec_msg.back();
        assert(nullptr != rsp_msg);

        // TODO: reject hint ?
        rsp_msg->set_reject(!isMatch(req_msg.index(), req_msg.log_term()));
        if (false == rsp_msg->reject()) {
            // set index to next_index
            if (0 < req_msg.entries_size()) {
                rsp_msg->set_index(
                        req_msg.entries(
                            req_msg.entries_size() - 1).index() + 1ull);
            }
            else {
                rsp_msg->set_index(req_msg.index() + 1ull);
            }
        }

        logdebug("MsgAppResp term %" PRIu64 " req_msg.from(leader) %" 
                PRIu64 " prev_index %" PRIu64 " prev_log_term %" PRIu64 
                " entries_size %d reject %d next_index %" PRIu64, 
                term_, req_msg.from(), req_msg.index(), req_msg.log_term(), 
                req_msg.entries_size(), static_cast<int>(rsp_msg->reject()), 
                rsp_msg->index());
    }
        break;

    case MessageType::MsgHeartbeat:
    {
        // TODO: 
        // better way to probe the next_indexes_ & match_indexes_ 
        //
        // MsgHeartbeat => empty AppendEntries RPCs
        if (MessageType::MsgHeartbeatResp == req_msg.type()) {
            // 1 : 1
            assert(MessageType::MsgHeartbeatResp == req_msg.type());
            assert(true == req_msg.reject());

            auto hb_msg = buildMsgHeartbeat(
                    req_msg.from(), next_indexes_[req_msg.from()]);
            assert(nullptr != hb_msg);
            vec_msg.emplace_back(move(hb_msg));
            assert(nullptr == hb_msg);
        }
        else {
            // broad cast
            vec_msg = batchBuildMsgHeartbeat();
            assert(vec_msg.size() == peer_ids_.size());
        }
    }
        break;

    case MessageType::MsgHeartbeatResp:
    {
        // rsp to leader
        assert(req_msg.from() == getLeader());
        
        vec_msg.emplace_back(make_unique<Message>(msg_template));
        auto& rsp_msg = vec_msg.back();
        assert(nullptr != rsp_msg);

        rsp_msg->set_reject(!isMatch(req_msg.index(), req_msg.log_term()));
        if (false == rsp_msg->reject()) {
            rsp_msg->set_index(req_msg.index() + 1ull);
        }
        logdebug("MsgHeartbeatResp term %" PRIu64 " req_msg.from(leader) %"
                PRIu64 " prev_index %" PRIu64 " prev_log_term %" PRIu64 
                " reject %d next_index %" PRIu64 , 
                term_, req_msg.from(), req_msg.index(), req_msg.log_term(), 
                static_cast<int>(rsp_msg->reject()), rsp_msg->index());
    }
        break;

    case MessageType::MsgNull:
        // DO NOTHING ?
        break;

    default:
        hassert(false, "invalid rsp_msg_type %d", 
                static_cast<int>(rsp_msg_type));
        break;
    }

    return vec_msg;
}

std::unique_ptr<Message>
RaftImpl::buildMsgApp(
        uint64_t peer_id, uint64_t index, size_t max_batch_size)
{
    // raft paper
    // AppendEntries RPC Arguments:
    // - leader term
    // - leader id
    // - prevLogIndex
    // - prevLogTerm
    // - entries[]
    // - leaderCommit
    assert(size_t{0} < max_batch_size);
    assert(0ull < index);
    auto app_msg = make_unique<Message>();
    assert(nullptr != app_msg);

    app_msg->set_type(MessageType::MsgApp);
    app_msg->set_logid(logid_);
    app_msg->set_term(term_);
    app_msg->set_from(selfid_);
    app_msg->set_to(peer_id);
    app_msg->set_commit(commited_index_);

    uint64_t base_index = getBaseLogIndex();
    uint64_t last_index = getLastLogIndex();
    if (index < base_index || index > last_index) {
        if (index < base_index) {
            // report: not in mem
            ids_not_in_mem_.insert(peer_id);
            return nullptr;
        }

        app_msg->set_index(0ull); // only case
        app_msg->set_log_term(getLogTerm(0ull)); // 0ull
        return app_msg;
    }

    assert(size_t{0} <= last_index - index + 1ull);
    app_msg->set_index(index - 1ull);
    app_msg->set_log_term(getLogTerm(index - 1ull));
    app_msg->set_commit(commited_index_);
    assert(size_t{0} <= last_index - index + 1ull);
    
    max_batch_size = min<size_t>(
            max_batch_size, last_index - index + 1ull);
    for (auto i = size_t{0}; i < max_batch_size; ++i) {
        auto entry = app_msg->add_entries();
        assert(nullptr != entry);

        *entry = *logs_[index + i - base_index];
    }

    assert(max_batch_size == static_cast<size_t>(app_msg->entries_size()));
    return app_msg;
}

std::vector<std::unique_ptr<Message>>
RaftImpl::batchBuildMsgAppUpToDate(size_t max_batch_size) 
{
    assert_role(*this, RaftRole::LEADER);
    assert(size_t{0} < max_batch_size);

    vector<std::unique_ptr<Message>> vec_msg;
    const uint64_t prev_last_index = next_indexes_[selfid_];
    uint64_t last_index = getLastLogIndex();
    if (prev_last_index == last_index + 1ull) {
        return vec_msg; // already up to date
    }

    assert(prev_last_index < last_index + 1ull);
    for (auto peer_id : peer_ids_) {
        assert(next_indexes_.end() != next_indexes_.find(peer_id));
        if (prev_last_index == next_indexes_[peer_id]) {
            auto msg_app = buildMsgApp(
                    peer_id, next_indexes_[peer_id], max_batch_size);
            if (nullptr != msg_app) {
                vec_msg.emplace_back(move(msg_app));
            }
            assert(nullptr == msg_app);
        }
    }

    next_indexes_[selfid_] = min(
            prev_last_index + max_batch_size, last_index + 1ull);
    return vec_msg;
}

std::vector<std::unique_ptr<Message>>
RaftImpl::batchBuildMsgApp(size_t max_batch_size)
{
    assert_role(*this, RaftRole::LEADER);
    assert(size_t{0} < max_batch_size);

    vector<std::unique_ptr<Message>> vec_msg;
    uint64_t last_index = getLastLogIndex();
    for (auto peer_id : peer_ids_) {
        assert(next_indexes_.end() != next_indexes_.find(peer_id));
        assert(next_indexes_[peer_id] <= last_index + 1ull);
        if (next_indexes_[peer_id] != last_index + 1ull) {
            assert(next_indexes_[peer_id] < last_index + 1ull);

            auto msg_app = buildMsgApp(
                    peer_id, next_indexes_[peer_id], max_batch_size);
            if (nullptr != msg_app) {
                vec_msg.emplace_back(move(msg_app));
            }
            assert(nullptr == msg_app);
        }
    }

    return vec_msg;
}

std::unique_ptr<Message>
RaftImpl::buildMsgHeartbeat(
        uint64_t peer_id, uint64_t next_index) const
{
    assert(0ull < next_index);
    auto hb_msg = make_unique<Message>();
    assert(nullptr != hb_msg);

    hb_msg->set_type(MessageType::MsgHeartbeat);
    hb_msg->set_logid(logid_);
    hb_msg->set_term(term_);
    hb_msg->set_from(selfid_);
    hb_msg->set_to(peer_id);

    // heartbeat
    uint64_t base_index = getBaseLogIndex();
    uint64_t last_index = getLastLogIndex();
    if (next_index < base_index || next_index > last_index) {
        // empty heartbeat: carry nothing
    }
    else {
        hb_msg->set_index(next_index - 1ull);
        hb_msg->set_log_term(getLogTerm(next_index - 1ull));
        hb_msg->set_commit(commited_index_);
    }

    return hb_msg;
}

std::vector<std::unique_ptr<Message>>
RaftImpl::batchBuildMsgHeartbeat()
{
    assert_role(*this, RaftRole::LEADER);

    vector<std::unique_ptr<Message>> vec_msg;
    vec_msg.reserve(peer_ids_.size());
    for (auto peer_id : peer_ids_) {
        assert(next_indexes_.end() != next_indexes_.find(peer_id));

        auto next_index = next_indexes_[peer_id];
        assert(0ull < next_index);
        auto hb_msg = buildMsgHeartbeat(peer_id, next_index);

        assert(nullptr != hb_msg);
        vec_msg.emplace_back(move(hb_msg));
    }
    assert(vec_msg.size() == peer_ids_.size());
    return vec_msg;
}


bool RaftImpl::isUpToDate(
        uint64_t peer_log_term, uint64_t peer_max_index)
{
    // raft paper 
    //  5.4.1 Election restriction
    //  raft determines which of two logs is more up-to-date by
    //  comparing the index and term of the last entries in the logs.
    //  - If the logs have last entries with different terms, then the log
    //    with the later term is more up-to-date;
    //  - If the logs end with the same term, then whichever log is longer
    //    is more up-to-date.
    uint64_t log_term = getLastLogTerm();
    if (peer_log_term > log_term) {
        return true;
    }
    
    assert(peer_log_term <= log_term);
    if (peer_log_term == log_term) {
        return peer_max_index >= getLastLogIndex();
    }

    // else
    return false;
}

void RaftImpl::appendLogs(gsl::array_view<const Entry> entries)
{
    if (size_t(0) == entries.length()) {
        return ; // do nothing;
    }

    uint64_t base_index = entries[0].index();
    truncateLogs(logs_, base_index);

    uint64_t last_index = getLastLogIndex();
    assert(last_index + 1 == base_index);
    for (size_t idx = 0; idx < entries.length(); ++idx) {
        logs_.emplace_back(make_unique<Entry>(entries[idx]));
        assert(nullptr != logs_.back());
    }

    return ;
}

void RaftImpl::appendEntries(
        uint64_t prev_log_index, 
        uint64_t prev_log_term, 
        uint64_t leader_commited_index, 
        gsl::array_view<const Entry> entries)
{
    assert_role(*this, RaftRole::FOLLOWER);
    assert(leader_commited_index >= commited_index_);

    if (!isMatch(prev_log_index, prev_log_term)) {
        return ;
    }

    // match
    // raft paper:
    // - If an existing entry conflicts with a new one(same index but
    //   different terms), delete the existing entry and all that follow it
    // - Append any new entries not already in the log
    uint64_t conflict_index = findConflict(entries);
    assert(0ull == conflict_index || commited_index_ < conflict_index);

    auto new_entries = shrinkEntries(conflict_index, entries);
    appendLogs(new_entries);

    updateFollowerCommitedIndex(leader_commited_index);
    return ;
}

int RaftImpl::checkAndAppendEntries(
        uint64_t prev_log_index, 
        gsl::array_view<const Entry> entries)
{
    assert_role(*this, RaftRole::LEADER);
    
    uint64_t last_index = getLastLogIndex();
    if (prev_log_index != last_index) {
        return -1;
    }

    // max length control ?
    for (size_t idx = 0; idx < entries.length(); ++idx) {
        logs_.emplace_back(
                make_unique<Entry>(entries[idx]));
        assert(nullptr != logs_.back());
        logs_.back()->set_term(term_);
        logs_.back()->set_index(last_index + 1 + idx);
    }

    // store !

    return 0;
}


void RaftImpl::setRole(RaftRole new_role)
{
    logdebug("change role_ %d new_role %d", 
            static_cast<int>(role_), static_cast<int>(new_role));
    role_ = new_role;
    switch (role_) {
        case RaftRole::FOLLOWER:
            timeout_handler_ = ::raft::follower::onTimeout;
            step_handler_ = ::raft::follower::onStepMessage;
            break;
        case RaftRole::CANDIDATE:
            timeout_handler_ = ::raft::candidate::onTimeout;
            step_handler_ = ::raft::candidate::onStepMessage;
            break;
        case RaftRole::LEADER:
            timeout_handler_ = ::raft::leader::onTimeout;
            step_handler_ = ::raft::leader::onStepMessage;
            break;
    }

    return ;
}

void RaftImpl::setTerm(uint64_t new_term)
{
    logdebug("change current term %" PRIu64 " new_term %" PRIu64, 
            term_, new_term);
    term_ = new_term;
    return ;
}

void RaftImpl::setVoteFor(bool reset, uint64_t candidate)
{
    logdebug("selfid_ %" PRIu64 " reset %d vote_for_ %" PRIu64 
            " to candidate %" PRIu64, 
            selfid_, static_cast<int>(reset), vote_for_, candidate);
    if (true == reset || 0ull == vote_for_) {
        vote_for_ = candidate;
    }
}

uint64_t RaftImpl::assignStoreSeq(uint64_t index)
{
    auto seq = ++store_seq_;
    pending_store_[index] = seq;
    logdebug("index %" PRIu64 " assigned store_seq %" PRIu64, 
            index, seq);
    return seq;
}

uint64_t RaftImpl::pendingStoreSeq(uint64_t index) const
{
    if (pending_store_.end() != pending_store_.find(index)) {
        logdebug("index %" PRIu64 " pending store seq %" PRIu64, 
                index, pending_store_.at(index));
        return pending_store_.at(index);
    }

    return 0ull;
}


void RaftImpl::updateActiveTime(
        std::chrono::time_point<std::chrono::system_clock> time_now)
{
    active_time_ = time_now;
}

uint64_t RaftImpl::getLastLogIndex() const 
{
    uint64_t base_index = getBaseLogIndex();
    if (0ull == base_index) {
        assert(true == logs_.empty());
        return base_index;
    }

    assert(nullptr != logs_.back());
    assert(base_index + logs_.size() - 1ull == logs_.back()->index());
    return logs_.back()->index();
}

uint64_t RaftImpl::getLastLogTerm() const 
{
    if (true == logs_.empty()) {
        assert(0ull == commited_index_);
        return 0ull;
    }

    assert(nullptr != logs_.back());
    assert(term_ >= logs_.back()->term());
    return logs_.back()->term();
}

uint64_t RaftImpl::getBaseLogTerm() const 
{
    if (true == logs_.empty()) {
        assert(0ull == commited_index_);
        return 0ull;
    }

    assert(nullptr != logs_.front());
    assert(0ull != logs_.front()->term());
    assert(term_ >= logs_.front()->term());
    return logs_.front()->term();
}

uint64_t RaftImpl::getBaseLogIndex() const 
{
    if (true == logs_.empty()) {
        assert(0ull == commited_index_);
        return 0ull;
    }

    assert(nullptr != logs_.front());
    assert(0ull < logs_.front()->index());
    assert(0ull == commited_index_ ||
            commited_index_ >= logs_.front()->index());
    return logs_.front()->index();
}

uint64_t RaftImpl::getLogTerm(uint64_t log_index) const
{
    if (0ull == log_index) {
        return 0ull; // 0ull <-> 0ull;
    }

    assert(0ull < log_index);
    assert(false == logs_.empty());
    const uint64_t base_index = getBaseLogIndex();
    const uint64_t last_index = getLastLogIndex();
    assert(log_index >= base_index);
    assert(log_index <= last_index);

    size_t idx = log_index - base_index;
    assert(0 <= idx && idx < logs_.size());
    assert(nullptr != logs_[idx]);
    assert(log_index == logs_[idx]->index());
    assert(0 < logs_[idx]->term());
    return logs_[idx]->term();
}

bool RaftImpl::isIndexInMem(uint64_t log_index) const 
{
    return getBaseLogIndex() <= log_index;
}


void RaftImpl::beginVote()
{
    assert_role(*this, RaftRole::CANDIDATE);
    // start election:
    // - increment currentTerm
    // - vote for self
    // - reset election timer
    // - send requestVote RPCs to all other servers
    setTerm(term_ + 1); // aka inc term
    setVoteFor(true, selfid_);
    assignStoreSeq(META_INDEX);

    // reset
    vote_resps_.clear();
    vote_resps_[selfid_] = true;
    return ;
}

void RaftImpl::updateVote(uint64_t peer_id, bool current_rsp)
{
    // current_rsp: 
    // - true: vote yes;
    // - false: vote no;
    assert_role(*this, RaftRole::CANDIDATE);
    if (vote_resps_.end() == vote_resps_.find(peer_id)) {
        vote_resps_[peer_id] = current_rsp;
        return ;
    }

    bool prev_rsp = vote_resps_[peer_id];
    logerr("peer_id %" PRIu64 " prev_rsp %d current_rsp %d", 
            peer_id, static_cast<int>(prev_rsp), static_cast<int>(current_rsp));
    // assert ?
    return ;
}

bool RaftImpl::isMajorVoteYes() const
{
    assert_role(*this, RaftRole::CANDIDATE);
    int true_cnt = 0;
    int false_cnt = 0;
    tie(true_cnt, false_cnt) = countVotes(vote_resps_);
    logdebug("selfid %" PRIu64 " peer_ids.size %zu"
            " true_cnt %d false_cnt %d", 
            selfid_, peer_ids_.size(), true_cnt, false_cnt);
    return true_cnt >= static_cast<int>(peer_ids_.size() / 2 + 1);
}

bool RaftImpl::isMatch(uint64_t log_index, uint64_t log_term) const
{
    assert_role(*this, RaftRole::FOLLOWER);
    
    if (0ull == log_index) {
        assert(0ull == log_term);
        return true; // always
    }

    assert(0ull < log_index);
    assert(0ull < log_term);

    const uint64_t local_log_term = getLogTerm(log_index);
    if (local_log_term == log_term) {
        return true;
    }

    if (log_index <= commited_index_) {
        assert(0ull == local_log_term);
        return true;
    }

    assert(0ull == local_log_term || log_index <= getLastLogIndex());
    return false;
}

void RaftImpl::updateLeaderCommitedIndex(uint64_t new_commited_index)
{
    assert_role(*this, RaftRole::LEADER);
    assert(commited_index_ < new_commited_index);
    logdebug("selfid(leader) %" PRIu64 " commited_index_ %" PRIu64
            " new_commited_index %" PRIu64, 
            selfid_, commited_index_, new_commited_index);
    commited_index_ = new_commited_index;
}

void RaftImpl::updateFollowerCommitedIndex(uint64_t leader_commited_index)
{
    assert_role(*this, RaftRole::FOLLOWER);
    assert(commited_index_ <= leader_commited_index);
    const uint64_t last_index = getLastLogIndex();
    logdebug("selfid %" PRIu64 " commited_index_ %" PRIu64 
            " last_index %" PRIu64 " leader_commited_index %" PRIu64, 
            selfid_, commited_index_, last_index, leader_commited_index);

    // if leaderCommit > commitIndex, 
    // set commitIndex = min(leaderCommit, index of last new entry)
    commited_index_ = min(leader_commited_index, last_index);
    return ;
}

uint64_t RaftImpl::findConflict(gsl::array_view<const Entry> entries) const
{
    if (size_t{0} == entries.length() || true == logs_.empty()) {
        return 0ull;
    }

    assert(size_t{0} < entries.length());
    assert(false == logs_.empty());
    for (size_t idx = 0; idx < entries.length(); ++idx) {
        if (!isMatch(entries[idx].index(), entries[idx].term())) {   
            return entries[idx].index();
        }
    }

    return entries[entries.length() -1].index() + 1ull;
}

bool RaftImpl::updateReplicateState(
        uint64_t peer_id, 
        bool reject, uint64_t /* reject_hint */, uint64_t peer_next_index)
{
    assert_role(*this, RaftRole::LEADER);
    assert(peer_ids_.end() != peer_ids_.find(peer_id));

    assert(0ull < next_indexes_[peer_id]);
    assert(next_indexes_[peer_id] > match_indexes_[peer_id]);
    if (true == reject) {
        assert(next_indexes_[peer_id] > match_indexes_[peer_id] + 1);

        // decrease next_indexes_
        // TODO: use reject_hint ?
        uint64_t next_peer_index = 
            (next_indexes_[peer_id] - match_indexes_[peer_id]) / 2 + 
            match_indexes_[peer_id];
        assert(next_peer_index > match_indexes_[peer_id]);
        assert(next_peer_index < next_indexes_[peer_id]);
        next_indexes_[peer_id] = next_peer_index;
        return true;
    }

    assert(false == reject);
    if (match_indexes_[peer_id] > peer_next_index || 
            1ull >= peer_next_index) {
        return false; // update nothing
    }

    assert(0ull < peer_next_index);

    const auto new_match_index = peer_next_index - 1ull;
    assert(match_indexes_[peer_id] < new_match_index);
    match_indexes_[peer_id] = new_match_index;

    if (getTerm() == getLogTerm(new_match_index) && 
            new_match_index > getCommitedIndex()) {
        // update commited_index_
        if (hasReplicateOnMajority(new_match_index, match_indexes_)) {
            updateLeaderCommitedIndex(new_match_index);
        }
    }

    next_indexes_[peer_id] = max(next_indexes_[peer_id], peer_next_index);
    return true;
}


void RaftImpl::becomeFollower()
{
    setRole(RaftRole::FOLLOWER);
    setLeader(true, 0ull);
    // TODO ??

    return ;
}

void RaftImpl::becomeCandidate()
{
    setRole(RaftRole::CANDIDATE);
    setLeader(true, 0ull);

    // raft paper
    // Candidates 5.2
    // On conversion to candidate or election timeout elapse, start election:
    beginVote();
    return ;
}

void RaftImpl::becomeLeader()
{
    setRole(RaftRole::LEADER);

    // raft paper
    // State
    // nextIndex[] 
    //   for each server, index of the next log entry to send to that
    //   server(initialized to leader last log index + 1)
    // matchIndex[]
    //   for each server, index of highest log entry known to be 
    //   replicated on server(initailzed to 0, increases monotonically)
    
    setLeader(false, selfid_);
    assert(selfid_ == leader_id_);
    next_indexes_.clear();
    match_indexes_.clear();

    uint64_t last_index = getLastLogIndex();
    for (auto peer_id : peer_ids_) {
        next_indexes_[peer_id] = last_index + 1ull;
        match_indexes_[peer_id] = 0ull;
    }

    // for most-up-to-date MsgApp
    next_indexes_[selfid_] = last_index + 1ull;
    return ;
}

void RaftImpl::setLeader(bool reset, uint64_t leader_id)
{
    logdebug("selfid_ %" PRIu64 " reset %d leader_id_ %" PRIu64
            " to leader_id %" PRIu64, 
            selfid_, static_cast<int>(reset), leader_id_, leader_id);
    if (true == reset || 0ull == leader_id_) {
        leader_id_ = leader_id; 
    }
}


} // namespace raft


