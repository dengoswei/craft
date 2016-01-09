#include <algorithm>
#include <sstream>
#include "raft_impl.h"
#include "replicate_tracker.h"

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

template <typename T>
bool isMajority(
        T expected, 
        const std::map<uint64_t, T>& votes, 
        size_t peer_ids_size, 
        const std::set<uint64_t>& old_peer_ids, 
        const std::set<uint64_t>& new_peer_ids)
{
    bool major = countMajor(expected, votes, peer_ids_size);
    if (true == old_peer_ids.empty() || false == major) {
        return major;
    }

    assert(false == old_peer_ids.empty());
    assert(false == new_peer_ids.empty());
    // joint consensus
    // raft paper: 
    // Agreement(for elections and entry commitment) requires
    // seperate majorities from both the old and new configrations.
    if (false == countMajor(expected, votes, old_peer_ids)) {
        return false;
    }

    return countMajor(expected, votes, new_peer_ids);
}

std::vector<const Entry*> make_vec_entries(const raft::Message& msg)
{
    vector<const Entry*> vec_entries(msg.entries_size(), nullptr);

    for (int idx = 0; idx < msg.entries_size(); ++idx) {
        vec_entries[idx] = &msg.entries(idx);
        assert(nullptr != vec_entries[idx]);
    }

    assert(vec_entries.size() == static_cast<size_t>(msg.entries_size()));
    return vec_entries;
}

gsl::array_view<const Entry*> 
make_entries(std::vector<const Entry*>& vec_entries)
{
    return gsl::array_view<const Entry*>{
        true == vec_entries.empty() ? nullptr : &vec_entries[0], 
        vec_entries.size()
    };
}

gsl::array_view<const Entry*> 
shrinkEntries(uint64_t conflict_index, gsl::array_view<const Entry*> entries)
{
    if (size_t(0) == entries.length() || 0ull == conflict_index) {
        return entries;
    }

    assert(size_t(0) < entries.length());
    assert(nullptr != entries[0]);
    uint64_t base_index = entries[0]->index();
    assert(conflict_index >= base_index);
    size_t idx = conflict_index - base_index;
    if (idx >= entries.length()) {
        return gsl::array_view<const Entry*>{nullptr};
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

void appendBroadcastMsg(
        std::vector<std::unique_ptr<Message>>& vec_msg, 
        const std::set<uint64_t>& group_ids, 
        const Message& msg_template)
{

    // TODO
    for (auto peer_id : group_ids) {
        if (msg_template.from() == peer_id) {
            continue;
        }

        vec_msg.emplace_back(make_unique<Message>(msg_template));
        auto& msg = vec_msg.back();
        assert(nullptr != msg);
        msg->set_to(peer_id);
    }
}


inline bool IsAGroupMember(
        const RaftImpl& raft_impl, uint64_t peer_id, bool check_current)
{ 
    if (true == check_current) {
        return raft_impl.GetCurrentConfig().IsAGroupMember(peer_id);
    }

    return raft_impl.GetCommitedConfig().IsAGroupMember(peer_id);
}

inline bool IsAReplicateMember(
        const RaftImpl& raft_impl, uint64_t peer_id)
{
    return raft_impl.GetCurrentConfig().IsAReplicateMember(peer_id);
}

int ApplyConfChange(
        const Entry& conf_entry, bool check_pending, 
        ConfChange& conf_change, RaftConfig& raft_config)
{
    if (EntryType::EntryConfChange == conf_entry.type()) {
        return -1;
    }

    // 1.
    {
        std::stringstream ss;
        ss.str(conf_entry.data());
        if (false == conf_change.ParseFromIstream(&ss)) {
            return -2;
        }
    }

    raft_config.ApplyConfChange(conf_change, check_pending);
    return 0;
}

std::vector<std::unique_ptr<raft::Message>>
batchBuildMsgApp(raft::RaftImpl& raft_impl)
{
    vector<unique_ptr<Message>> vec_msg;   
    const auto& replicate_group = 
        raft_impl.GetCurrentConfig().GetReplicateGroup();
    uint64_t last_log_index = raft_impl.getLastLogIndex();
    for (auto peer_id : replicate_group) {
        auto msg = 
            raft_impl.GetReplicateTracker().BuildMsgApp(
                last_log_index, peer_id, 
                [&](uint64_t to, uint64_t next_index, size_t batch_size) {
                    return raft_impl.buildMsgApp(
                            to, next_index, batch_size);
                });
        if (nullptr != msg) {
            vec_msg.emplace_back(move(msg));
        }
        assert(nullptr == msg);
    }

    return vec_msg;
}

std::vector<std::unique_ptr<raft::Message>>
batchBuildMsgHeartbeat(raft::RaftImpl& raft_impl)
{
    vector<unique_ptr<Message>> vec_msg;
    const auto& replicate_group = 
        raft_impl.GetCurrentConfig().GetReplicateGroup();
    for (auto peer_id : replicate_group) {
        auto msg = 
            raft_impl.GetReplicateTracker().BuildMsgHeartbeat(
                    peer_id, 
                    [&](uint64_t to, uint64_t next_index, size_t /*  */) {
                        return raft_impl.buildMsgHeartbeat(to, next_index);
                    });
        if (nullptr != msg) {
            vec_msg.emplace_back(move(msg));
        }
        assert(nullptr == msg);
    }

    return vec_msg;
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
    logdebug("selfid(follower) %" PRIu64 " term %" PRIu64, 
            raft_impl.getSelfId(), raft_impl.getTerm());
    // only timeout if selfid IsAGroupMember
    if (false == IsAGroupMember(raft_impl, raft_impl.getSelfId(), true)) {
        // don't have MsgVote right;
        raft_impl.updateActiveTime(time_now);
        return MessageType::MsgNull;
    }

    // raft paper: 
    // Followers 5.2
    // if election timeout elapses without receiving AppendEntries
    // RPC from current leader or granting vote to candidate:
    // convert to candidate
    raft_impl.becomeCandidate();
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
            // valid check
            // 1. msg.from => is a valid group member in current config;
            // 2. selfid   => is a valid group member in current config;
            if (false == IsAGroupMember(raft_impl, msg.from(), true) || 
                    false == IsAGroupMember(
                        raft_impl, raft_impl.getSelfId(), true)) {
                logerr("NotAGroupMember: msg.to %" PRIu64 " msg.from %" PRIu64, 
                        msg.to(), msg.from());
                break;
            }

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

            auto vec_entries = make_vec_entries(msg);
            auto entries = make_entries(vec_entries);
            assert(static_cast<size_t>(
                        msg.entries_size()) == entries.length());

            auto append_count = 
                raft_impl.appendEntries(
                    msg.index(), msg.log_term(), msg.commit(), entries);

            auto store_seq = 0 < append_count ? 
                raft_impl.assignStoreSeq(msg.index() + 1ull) : 
                0ull;
            logdebug("selfid(follower) %" PRIu64 " index %" PRIu64 
                    " term %" PRIu64 " log_term %" PRIu64 
                    " entries_size %zu append_count %d"
                    " store_seq %" PRIu64, 
                    raft_impl.getSelfId(), msg.index(), msg.term(), 
                    msg.log_term(), 
                    vec_entries.size(), append_count, store_seq);

            raft_impl.updateActiveTime(time_now);

            rsp_msg_type = MessageType::MsgAppResp;
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
    logdebug("selfid(candidate) %" PRIu64 " term %" PRIu64, 
            raft_impl.getSelfId(), raft_impl.getTerm());

    if (false == IsAGroupMember(raft_impl, raft_impl.getSelfId(), true)) {
        raft_impl.becomeFollower(raft_impl.getTerm()); // step back as follower;
        return MessageType::MsgNull;
    }

    // raft paper:
    // Candidates 5.2
    // On conversion to candidate or election timeout elapse, 
    // start election:
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
            // valid check
            if (false == IsAGroupMember(raft_impl, msg.from(), true) ||
                    false == IsAGroupMember(
                        raft_impl, raft_impl.getSelfId(), true)) {
                logerr("NotAGroupMember: msg.to %" PRIu64 " msg.from %" PRIu64, 
                        msg.to(), msg.from());
                break;
            }

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
            logdebug("IGNORE: recv msg type %d", 
                    static_cast<int>(msg.type()));
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
    // check msg.from

    switch (msg.type()) {
        case MessageType::MsgProp:
        {
            // client prop
            auto vec_entries = make_vec_entries(msg);
            auto entries = make_entries(vec_entries);
            assert(static_cast<size_t>(
                        msg.entries_size()) == entries.length());

            auto ret = 
                raft_impl.checkAndAppendEntries(msg.index(), entries);
            if (0 != ret) {
                logerr("checkAndAppendEntries ret %d", ret);
                break;
            }
            
            assert(0ull < msg.index() + 1ull);
            auto store_seq = raft_impl.assignStoreSeq(msg.index() + 1ull);
            logdebug("selfid(leader) %" PRIu64 " MsgProp index %" PRIu64 
                    " store_seq %" PRIu64 " entries_size %zu "
                    "last_index %" PRIu64, 
                    raft_impl.getSelfId(), msg.index(), store_seq, 
                    vec_entries.size(), raft_impl.getLastLogIndex());
            rsp_msg_type = MessageType::MsgApp;
        }
            break;

        case MessageType::MsgAppResp:
        {
            if (false == IsAReplicateMember(raft_impl, msg.from())) {
                // not a replicate member now
                // => ignore this request
                logerr("selfid %" PRIu64 
                        " recv MsgAppResp from NotAReplicateMember %" PRIu64, 
                        raft_impl.getSelfId(), msg.from());
                break;
            }

            // collect appendEntry resp
            // TODO: update commited!
            bool update = raft_impl.updateReplicateState(
                    msg.from(), msg.reject(), 
                    msg.reject_hint(), msg.index());
            if (update) {
                rsp_msg_type = MessageType::MsgApp;
            }

            logdebug("selfid(leader) %" PRIu64 
                    " MsgAppResp msg.from %" PRIu64
                    " msg.index %" PRIu64 " reject %d rsp_msg_type %d", 
                    raft_impl.getSelfId(), msg.from(), msg.index(), 
                    static_cast<int>(msg.reject()), 
                    static_cast<int>(rsp_msg_type));
        }
            break;

        case MessageType::MsgHeartbeatResp:
        {
            if (false == IsAReplicateMember(raft_impl, msg.from())) {
                // not a replicate member now
                // => ignore this request
                logerr("selfid %" PRIu64 
                        " recv MsgHeartbeatResp from NotAReplicateMember %" PRIu64, 
                        raft_impl.getSelfId(), msg.from());
                break;
            }

 
            // collect heartbeat resp
            bool update = raft_impl.updateReplicateState(
                    msg.from(), msg.reject(), 
                    msg.reject_hint(), msg.index());
            if (true == update) {
                rsp_msg_type = MessageType::MsgHeartbeat;
            }
            // TODO: logdebug
        }
            break;

        case MessageType::MsgNull:
        {
            // check followers timeout ?
            assert(0ull == msg.from());
            auto time_now = chrono::system_clock::now();
            if (raft_impl.isHeartbeatTimeout(time_now)) {
                raft_impl.updateHeartbeatTime(time_now);
                rsp_msg_type = MessageType::MsgHeartbeat;
            }
        }
            break;

        default:
            logdebug("IGNORE: recv msg type %d", 
                    static_cast<int>(msg.type()));
            // TODO: ?
            break;
    }

    return rsp_msg_type;
}
    
} // namespace leader

RaftImpl::RaftImpl(
        uint64_t logid, 
        uint64_t selfid, 
        const std::set<uint64_t>& peer_ids, 
        int min_election_timeout, 
        int max_election_timeout)
    : logid_(logid)
    , selfid_(selfid)
    , current_config_(selfid)
    , commited_config_(selfid)
//    , peer_ids_(peer_ids)
//    , app_peer_ids_(peer_ids)
    , rtimeout_(min_election_timeout, max_election_timeout)
    , election_timeout_(rtimeout_())
    , active_time_(chrono::system_clock::now())
    , hb_timeout_(min_election_timeout / 2)
{
    assert(0 < min_election_timeout);
    assert(min_election_timeout <= max_election_timeout);

    // fake ?
    for (auto id : peer_ids) {
        ConfChange conf_change;
        conf_change.set_type(ConfChangeType::ConfChangeAddNode);
        conf_change.set_node_id(id);

        current_config_.ApplyConfChange(conf_change, false);
        commited_config_.ApplyConfChange(conf_change, false);
    }
    assert(false == current_config_.IsPending());
    assert(false == commited_config_.IsPending());
    
    assert(0 < election_timeout_.count());
    assert(0 < hb_timeout_.count());
    setRole(RaftRole::FOLLOWER);
}

RaftImpl::~RaftImpl() = default;

MessageType RaftImpl::CheckTerm(uint64_t msg_term)
{
    // raft paper: rules for servers: 5.1
    // => If RPC request or response contains term T > currentTerm: 
    //    set currentTerm = T, convert to follower;
    if (msg_term != term_) {
        if (msg_term > term_) {
            becomeFollower(msg_term);
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
    // ! IMPORTANT !
    // if setTerm => false == CheckTimeout
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

        vec_msg = current_config_.BroadcastGroupMsg(msg_template);
        logdebug("MsgVote term %" PRIu64 " candidate %" PRIu64 
                " lastLogIndex %" PRIu64 " lastLogTerm %" PRIu64
                " vec_msg.size %zu", 
                term_, selfid_, msg_template.index(), 
                msg_template.log_term(), vec_msg.size());
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
        assert(nullptr != replicate_states_);
        // req_msg.type() == MessageType::MsgProp
        if (0ull == req_msg.from()) {
            vec_msg = batchBuildMsgApp(*this);
            logdebug("MsgApp BatchBuildMsgAppUpToDate "
                    "vec_msg.size %zu", vec_msg.size());
        }
        else {
            assert(0ull != req_msg.from());
            // catch-up mode maybe
            assert(MessageType::MsgAppResp == req_msg.type());
            auto rsp_msg = 
                replicate_states_->BuildMsgApp(
                        getLastLogIndex(), req_msg.from(), 
                        [&](uint64_t to, 
                            uint64_t next_index, size_t batch_size) {
                            return buildMsgApp(to, next_index, batch_size);
                        });
            if (nullptr != rsp_msg) {
                logdebug("MsgApp from %" PRIu64 " to %" PRIu64 
                        " index %" PRIu64 " log_term %" PRIu64, 
                        rsp_msg->from(), rsp_msg->to(), 
                        rsp_msg->index(), rsp_msg->log_term());
                if (0 != rsp_msg->entries_size() || 
                        !isPeerUpToDate(req_msg.commit())) {
                    vec_msg.emplace_back(move(rsp_msg));
                    assert(size_t{1} == vec_msg.size());
                }
                else {
                    logdebug("INGORE: peer_id %" PRIu64 
                            " rsp_msg->entries_size %d"
                            " req_msg.commit %" PRIu64 
                            " commited_index_ %" PRIu64, 
                            req_msg.from(), 
                            rsp_msg->entries_size(), 
                            req_msg.commit(), 
                            getCommitedIndex());
                }
                // else => ignore
            }
            logdebug("MsgApp selfid %" PRIu64 " req_msg.from %" PRIu64 
                    " req_msg.index %" PRIu64 " vec_msg.size %zu", 
                    selfid_, req_msg.from(), req_msg.index(), 
                    vec_msg.size());
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
        rsp_msg->set_commit(getCommitedIndex());

        logdebug("MsgAppResp term %" PRIu64 " req_msg.from(leader) %" 
                PRIu64 " prev_index %" PRIu64 " prev_log_term %" PRIu64 
                " entries_size %d reject %d next_index %" PRIu64, 
                term_, req_msg.from(), req_msg.index(), 
                req_msg.log_term(), req_msg.entries_size(),
                static_cast<int>(rsp_msg->reject()), 
                rsp_msg->index());
    }
        break;

    case MessageType::MsgHeartbeat:
    {
        assert(nullptr != replicate_states_);
        // TODO: 
        // better way to probe the next_indexes_ & match_indexes_ 
        //
        // MsgHeartbeat => empty AppendEntries RPCs
        if (MessageType::MsgHeartbeatResp == req_msg.type()) {
            // 1 : 1
            assert(MessageType::MsgHeartbeatResp == req_msg.type());
            assert(true == req_msg.reject());

            auto hb_msg = 
                replicate_states_->BuildMsgHeartbeat(
                        req_msg.from(), 
                        [&](uint64_t to, 
                            uint64_t next_index, size_t /* */) {
                            return buildMsgHeartbeat(to, next_index);
                        });
            if (nullptr != hb_msg) {
                vec_msg.emplace_back(move(hb_msg));
            }
            assert(nullptr == hb_msg);
        }
        else {
            // broad cast
            vec_msg = batchBuildMsgHeartbeat(*this);
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
                term_, req_msg.from(), req_msg.index(), 
                req_msg.log_term(), 
                static_cast<int>(rsp_msg->reject()), rsp_msg->index());
    }
        break;

    case MessageType::MsgNull:
        // DO NOTHING ?
        break;

    case MessageType::MsgInvalidTerm:
        
        logdebug("MsgInvalidTerm selfid %" PRIu64 
                " role %d term_ %" PRIu64 " msg.from %" PRIu64 
                " msg.term %" PRIu64, 
                getSelfId(), static_cast<int>(getRole()), 
                getTerm(), req_msg.from(), req_msg.term());
        // TODO: rsp with ?
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
    assert(size_t{0} <= max_batch_size);
    assert(0ull < index);
    auto app_msg = make_unique<Message>();
    assert(nullptr != app_msg);

    app_msg->set_type(MessageType::MsgApp);
    app_msg->set_logid(logid_);
    app_msg->set_term(term_);
    app_msg->set_from(selfid_);
    app_msg->set_to(peer_id);
    app_msg->set_commit(commited_index_);

    uint64_t base_index = 0ull;
    uint64_t last_index = 0ull;
    tie(base_index, last_index) = getInMemIndex();
    logdebug("selfid %" PRIu64 " leader_id %" PRIu64 
            " index %" PRIu64 " base_index %" PRIu64 
            " last_index %" PRIu64 " logs_.size %zu max_batch_size %zu", 
            selfid_, leader_id_, 
            index, base_index, last_index, logs_.size(), max_batch_size);
    if (index < base_index || index > last_index + 1ull) {
        if (index < base_index) {
            // report: not in mem
            // => or submit a catch-up job ? 
            //    => catch-up to commited_index point ?
            // => which will read log from db directly ??
            ids_not_in_mem_.insert(peer_id);
        }

        return nullptr;
//        assert(index == last_index + 1ull);
//        app_msg->set_index(last_index); // only case
//        app_msg->set_log_term(getLogTerm(last_index)); // 0ull
//        return app_msg;
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
    logdebug("selfid %" PRIu64 " peer_id %" PRIu64 " index %" PRIu64 
            " max_batch_size %zu", 
            getSelfId(), peer_id, index, max_batch_size);
    return app_msg;
}

//std::vector<std::unique_ptr<Message>>
//RaftImpl::batchBuildMsgAppUpToDate(size_t max_batch_size) 
//{
//    assert_role(*this, RaftRole::LEADER);
//    assert(size_t{0} < max_batch_size);
//
//    vector<std::unique_ptr<Message>> vec_msg;
//    const uint64_t prev_last_index = next_indexes_[selfid_];
//    uint64_t last_index = getLastLogIndex();
//    logdebug("prev_last_index %" PRIu64 " last_index %" PRIu64, 
//            prev_last_index, last_index);
//    if (prev_last_index == last_index + 1ull) {
//        return vec_msg; // already up to date
//    }
//
//    assert(prev_last_index < last_index + 1ull);
//    for (auto peer_id : peer_ids_) {
//        if (peer_id == getSelfId()) {
//            continue;
//        }
//
//        assert(next_indexes_.end() != next_indexes_.find(peer_id));
//        logdebug("peer_id %" PRIu64 " prev_last_index %" PRIu64
//                " next_indexes_ %" PRIu64 " vec_msg.size %zu", 
//                peer_id, prev_last_index, next_indexes_[peer_id], 
//                vec_msg.size());
//        if (prev_last_index == next_indexes_[peer_id]) {
//            auto msg_app = buildMsgApp(
//                    peer_id, next_indexes_[peer_id], max_batch_size);
//            if (nullptr != msg_app) {
//                logdebug("peer_id %" PRIu64 " prev_last_index %" PRIu64
//                        " next_index %" PRIu64 " max_batch_size %zu"
//                        " entries_size %d", 
//                        peer_id, prev_last_index, next_indexes_[peer_id], 
//                        max_batch_size, msg_app->entries_size());
//                vec_msg.emplace_back(move(msg_app));
//            }
//            assert(nullptr == msg_app);
//        }
//    }
//
//    next_indexes_[selfid_] = min(
//            prev_last_index + max_batch_size, last_index + 1ull);
//    match_indexes_[selfid_] = next_indexes_[selfid_];
//    return vec_msg;
//}
//
//std::vector<std::unique_ptr<Message>>
//RaftImpl::batchBuildMsgApp(size_t max_batch_size)
//{
//    assert_role(*this, RaftRole::LEADER);
//    assert(size_t{0} < max_batch_size);
//
//    vector<std::unique_ptr<Message>> vec_msg;
//    uint64_t last_index = getLastLogIndex();
//    for (auto peer_id : peer_ids_) {
//        if (peer_id == getSelfId()) {
//            continue;
//        }
//
//        assert(next_indexes_.end() != next_indexes_.find(peer_id));
//        assert(next_indexes_[peer_id] <= last_index + 1ull);
//        if (next_indexes_[peer_id] != last_index + 1ull) {
//            assert(next_indexes_[peer_id] < last_index + 1ull);
//
//            auto msg_app = buildMsgApp(
//                    peer_id, next_indexes_[peer_id], max_batch_size);
//            if (nullptr != msg_app) {
//                vec_msg.emplace_back(move(msg_app));
//            }
//            assert(nullptr == msg_app);
//        }
//    }
//
//    return vec_msg;
//}
//

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
    uint64_t base_index = 0ull;
    uint64_t last_index = 0ull;
    tie(base_index, last_index) = getInMemIndex();
    next_index = max(base_index + 1ull, next_index);
    next_index = min(last_index + 1ull, next_index);

    hb_msg->set_index(next_index - 1ull);
    hb_msg->set_log_term(getLogTerm(next_index - 1ull));
    hb_msg->set_commit(commited_index_);
    return hb_msg;
}

//std::vector<std::unique_ptr<Message>>
//RaftImpl::batchBuildMsgHeartbeat()
//{
//    assert_role(*this, RaftRole::LEADER);
//
//    vector<std::unique_ptr<Message>> vec_msg;
//    vec_msg.reserve(peer_ids_.size() - 1);
//    for (auto peer_id : peer_ids_) {
//        if (peer_id == getSelfId()) {
//            continue;
//        }
//
//        assert(next_indexes_.end() != next_indexes_.find(peer_id));
//
//        auto next_index = next_indexes_[peer_id];
//        assert(0ull < next_index);
//        auto hb_msg = buildMsgHeartbeat(peer_id, next_index);
//
//        assert(nullptr != hb_msg);
//        vec_msg.emplace_back(move(hb_msg));
//    }
//    assert(vec_msg.size() == peer_ids_.size());
//    return vec_msg;
//}
//

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

int RaftImpl::appendLogs(gsl::array_view<const Entry*> entries)
{
    if (size_t(0) == entries.length()) {
        return 0; // do nothing;
    }

    assert(nullptr != entries[0]);
    uint64_t base_index = entries[0]->index();
    auto truncate_size = truncateLogs(logs_, base_index);
    if (size_t{0} < truncate_size) {
        logerr("INFO selfid %" PRIu64 " truncate_size %zu", 
                getSelfId(), truncate_size);

        assert(0 == reconstructCurrentConfig()); // reset currentConfig;
    }

    uint64_t last_index = getLastLogIndex();
    assert(last_index + 1 == base_index);
    for (size_t idx = 0; idx < entries.length(); ++idx) {
        assert(nullptr != entries[idx]);
        if (EntryType::EntryConfChange == entries[idx]->type()) {
            applyCommitedConfEntry(*entries[idx]);
        }

        logs_.emplace_back(make_unique<Entry>(*entries[idx]));
        assert(nullptr != logs_.back());
    }

    return entries.length();
}

int RaftImpl::appendEntries(
        uint64_t prev_log_index, 
        uint64_t prev_log_term, 
        uint64_t leader_commited_index, 
        gsl::array_view<const Entry*> entries)
{
    assert_role(*this, RaftRole::FOLLOWER);
    assert(leader_commited_index >= commited_index_);

    if (!isMatch(prev_log_index, prev_log_term)) {
        return -1;
    }

    // match
    // raft paper:
    // - If an existing entry conflicts with a new one(same index but
    //   different terms), delete the existing entry and all that follow it
    // - Append any new entries not already in the log
    uint64_t conflict_index = findConflict(entries);
    assert(0ull == conflict_index || commited_index_ < conflict_index);

    auto new_entries = shrinkEntries(conflict_index, entries);
    int append_count = appendLogs(new_entries);

    updateFollowerCommitedIndex(leader_commited_index);
    return append_count;
}

int RaftImpl::checkAndAppendEntries(
        uint64_t prev_log_index, 
        gsl::array_view<const Entry*> entries)
{
    assert_role(*this, RaftRole::LEADER);
    assert(nullptr != replicate_states_);
    
    uint64_t last_index = getLastLogIndex();
    if (prev_log_index != last_index) {
        return -1;
    }

    // max length control ?
    for (size_t idx = 0; idx < entries.length(); ++idx) {
        assert(nullptr != entries[idx]);
        // apply conf change before push entries into logs_
        if (EntryType::EntryConfChange == entries[idx]->type()) {
            assert(size_t{1} == entries.length());
            assert(size_t{0} == idx);
            // applyConfChange:
            auto ret = applyUnCommitedConfEntry(*entries[idx]);
            if (0 != ret) {
                // drop conf change request
                logerr("applyConfChange ret %d", ret);
                return -2;
            }
        }

        logs_.emplace_back(
                make_unique<Entry>(*entries[idx]));
        assert(nullptr != logs_.back());
        logs_.back()->set_term(term_);
        logs_.back()->set_index(last_index + 1ull + idx);
    }

    replicate_states_->UpdateSelfState(getLastLogIndex());
    logdebug("selfid %" PRIu64 " last_index %" PRIu64 " logs_size %zu", 
            selfid_, getLastLogIndex(), logs_.size());
    // TODO: find a way to store logs_ to disk
    return 0;
}


void RaftImpl::setRole(RaftRole new_role)
{
    logdebug("selfid %" PRIu64 " change role_ %d new_role %d", 
            selfid_, static_cast<int>(role_), static_cast<int>(new_role));
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
    logdebug("selfid %" PRIu64 " role %d change current term %" PRIu64 
            " new_term %" PRIu64, 
            selfid_, static_cast<int>(role_), term_, new_term);
    assert(term_ < new_term);
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
    if (0ull == index) {
        pending_meta_seq_ = seq;
    }
    else {
        pending_log_idx_ = 
            0ull == pending_log_idx_ ? 
            index : min(pending_log_idx_, index);
        pending_log_seq_ = seq;
    }

//    logdebug("selfid %" PRIu64 " index %" PRIu64 
//            " pending_meta_seq_ %" PRIu64 
//            " pending_log_idx_ %" PRIu64 " pending_log_seq_ %" PRIu64, 
//            getSelfId(), index, pending_meta_seq_, 
//            pending_log_idx_, pending_log_seq_);
    return seq;
}

std::tuple<uint64_t, uint64_t, uint64_t>
RaftImpl::getStoreSeq() const
{
    logdebug("selfid %" PRIu64 
            " pending_meta_seq_ %" PRIu64 
            " pending_log_idx_ %" PRIu64 " pending_log_seq_ %" PRIu64, 
            getSelfId(), pending_meta_seq_, 
            pending_log_idx_, pending_log_seq_);

    return make_tuple(
            pending_meta_seq_, pending_log_idx_, pending_log_seq_);
}

void RaftImpl::commitedStoreSeq(
        uint64_t meta_seq, uint64_t log_idx, uint64_t log_seq) 
{
    logdebug("selfid %" PRIu64 
            " meta_seq %" PRIu64 " log_idx %" PRIu64 
            " log_seq %" PRIu64 
            " pending_meta_seq_ %" PRIu64 " pending_log_idx_ %"
            PRIu64 " pending_log_seq_ %" PRIu64, 
            getSelfId(), meta_seq, log_idx, log_seq, 
            pending_meta_seq_, pending_log_idx_, 
            pending_log_seq_);
    if (meta_seq == pending_meta_seq_) {
        pending_meta_seq_ = 0ull; // reset
    }

    if (log_idx == pending_log_idx_ && 
            log_seq == pending_log_seq_) {
        pending_log_idx_ = 0ull;
        pending_log_seq_ = 0ull;
    }
}

void RaftImpl::updateActiveTime(
        std::chrono::time_point<std::chrono::system_clock> time_now)
{
    {
        auto at_str = format_time(active_time_);
        auto time_str = format_time(time_now);
//        logdebug("selfid %" PRIu64 " update active_time_ %s "
//                "to time_now %s", 
//                getSelfId(), at_str.c_str(), time_str.c_str());
    }
    active_time_ = time_now;
}

uint64_t RaftImpl::getLastLogIndex() const 
{
    auto t = getInMemIndex();
    return get<1>(t);
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

std::tuple<uint64_t, uint64_t> RaftImpl::getInMemIndex() const 
{
    if (true == logs_.empty()) {
        assert(0ull == commited_index_);
        return make_tuple(0ull, 0ull);
    }

    assert(nullptr != logs_.front());
    assert(0ull < logs_.front()->index());
    assert(0ull == commited_index_ ||
            commited_index_ >= logs_.front()->index());
    assert(nullptr != logs_.back());
    assert(logs_.back()->index() == 
            logs_.front()->index() + logs_.size() - 1ull);
    
    return make_tuple(logs_.front()->index(), logs_.back()->index());
}

uint64_t RaftImpl::getBaseLogIndex() const 
{
    auto t = getInMemIndex();
    return get<0>(t);
}


const Entry* RaftImpl::getLogEntry(uint64_t log_index) const 
{
    if (0ull == log_index) {
        return nullptr;
    }

    assert(0ull < log_index);
    assert(false == logs_.empty());
    auto base_index = 0ull;
    auto last_index = 0ull;
    tie(base_index, last_index) = getInMemIndex();
    if (log_index < base_index || log_index > last_index) {
        return nullptr;
    }

    size_t idx = log_index - base_index;
    assert(0 <= idx && idx < logs_.size());
    assert(nullptr != logs_[idx]);
    assert(log_index == logs_[idx]->index());
    assert(0 < logs_[idx]->term());
    return logs_[idx].get();
}

std::vector<std::unique_ptr<raft::Entry>>
RaftImpl::getLogEntriesAfter(uint64_t log_index) const
{
    vector<unique_ptr<Entry>> vec_entries;
    uint64_t base_index = 0ull;
    uint64_t last_index = 0ull;
    tie(base_index, last_index) = getInMemIndex();
    assert(base_index <= log_index + 1ull);
    if (log_index >= last_index) {
        return vec_entries;
    }

    assert(0ull < last_index - log_index);
    vec_entries.reserve(last_index - log_index);
    for (; log_index < last_index; ++log_index) {
        auto index = log_index + 1;
        auto entry = getLogEntry(index);
        assert(nullptr != entry);
        assert(index == entry->index());
        vec_entries.emplace_back(
                make_unique<Entry>(*entry));
        assert(nullptr != vec_entries.back());
        vec_entries.back()->set_seq(pending_log_seq_);
    }

    return vec_entries;
}

std::unique_ptr<raft::HardState>
RaftImpl::getCurrentHardState() const 
{
    auto hs = make_unique<HardState>();
    assert(nullptr != hs);
    hs->set_term(term_);
    hs->set_vote(vote_for_);
    hs->set_commit(commited_index_);
    hs->set_seq(pending_meta_seq_);
    return hs;
}

uint64_t RaftImpl::getLogTerm(uint64_t log_index) const
{
    const auto entry = getLogEntry(log_index);
    if (nullptr == entry) {
        return 0ull;
    }

    return entry->term();
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

    return current_config_.IsMajorVoteYes(vote_resps_);
//    return isMajority(true, vote_resps_, 
//            peer_ids_.size(), old_peer_ids_, new_peer_ids_);
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

uint64_t 
RaftImpl::findConflict(gsl::array_view<const Entry*> entries) const
{
    if (size_t{0} == entries.length() || true == logs_.empty()) {
        return 0ull;
    }

    assert(size_t{0} < entries.length());
    assert(false == logs_.empty());
    for (size_t idx = 0; idx < entries.length(); ++idx) {
        assert(nullptr != entries[idx]);
        if (!isMatch(entries[idx]->index(), entries[idx]->term())) {   
            return entries[idx]->index();
        }
    }

    return entries[entries.length() -1]->index() + 1ull;
}

bool RaftImpl::updateReplicateState(
        uint64_t peer_id, 
        bool reject, uint64_t reject_hint, uint64_t peer_next_index)
{
    assert_role(*this, RaftRole::LEADER);
    assert(nullptr != replicate_states_);
    bool update = replicate_states_->UpdateReplicateState(
            peer_id, reject, reject_hint, peer_next_index);
    if (true == update) {
        assert(0ull < peer_next_index);
        auto new_commited_index = peer_next_index - 1ull;
        if (getTerm() == getLogTerm(new_commited_index) && 
                new_commited_index > getCommitedIndex()) {

            if (current_config_.IsMajorCommited(
                        new_commited_index, 
                        replicate_states_->peekMatchIndexes())) {
                updateLeaderCommitedIndex(new_commited_index);
            }
        }
    }

    return update;
//    return replicate_states_->UpdateReplicateState(
//            *this, 
//            peer_id, reject, reject_hint, peer_next_index);
}
// OLD updateReplicateState impl
//    assert(peer_ids_.end() != peer_ids_.find(peer_id));
//
//    assert(0ull < next_indexes_[peer_id]);
//    assert(next_indexes_[peer_id] > match_indexes_[peer_id]);
//    if (true == reject) {
//        assert(next_indexes_[peer_id] > match_indexes_[peer_id] + 1);
//
//        // decrease next_indexes_
//        // TODO: use reject_hint ?
//        uint64_t next_peer_index = 
//            (next_indexes_[peer_id] - match_indexes_[peer_id]) / 2 + 
//            match_indexes_[peer_id];
//        assert(next_peer_index > match_indexes_[peer_id]);
//        assert(next_peer_index < next_indexes_[peer_id]);
//        next_indexes_[peer_id] = next_peer_index;
//        return true;
//    }
//
//    assert(false == reject);
//    if (match_indexes_[peer_id] > peer_next_index || 
//            1ull >= peer_next_index) {
//        return false; // update nothing
//    }
//
//    assert(0ull < peer_next_index);
//
//    const auto new_match_index = peer_next_index - 1ull;
//    assert(match_indexes_[peer_id] < new_match_index);
//    match_indexes_[peer_id] = new_match_index;
//
//    if (getTerm() == getLogTerm(new_match_index) && 
//            new_match_index > getCommitedIndex()) {
//        // update commited_index_
//        // raft paper: joint consensus
//        // Agreement(for elections and entry commitment) requires
//        // seperate majorities from both the old and nwe configrations.
//        // TODO
//        if (isMajority(new_match_index, match_indexes_, 
//                    peer_ids_.size(), old_peer_ids_, new_peer_ids_)) {
//            updateLeaderCommitedIndex(new_match_index);
//        }
//    }
//
//    next_indexes_[peer_id] = max(next_indexes_[peer_id], peer_next_index);
//    return true;
//}


void RaftImpl::becomeFollower(uint64_t term)
{
    if (nullptr != replicate_states_) {
        replicate_states_.reset();
    }

    assert(nullptr == replicate_states_);
    setRole(RaftRole::FOLLOWER);
    
    term = 0ull == term ? term_ + 1ull : term;
    assert(term_ < term);
    setTerm(term);

    setLeader(true, 0ull);
    setVoteFor(true, 0ull); // reset vote_for_
    assignStoreSeq(META_INDEX);
    // new follower => will not timeout immidiate;
    resetElectionTimeout();
    updateActiveTime(chrono::system_clock::now());
    // TODO ??

    return ;
}

void RaftImpl::becomeCandidate()
{
    assert(nullptr == replicate_states_);

    setRole(RaftRole::CANDIDATE);
    setLeader(true, 0ull);

    // raft paper
    // Candidates 5.2
    // On conversion to candidate or election timeout elapse, 
    // start election:
    // - inc term
    // - set vote for
    // - assign store seq
    beginVote();
    resetElectionTimeout();
    updateActiveTime(chrono::system_clock::now());
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

    assert(nullptr == replicate_states_);
    replicate_states_ = 
        current_config_.CreateReplicateTracker(getLastLogIndex(), MAX_BATCH_SIZE);
    assert(nullptr != replicate_states_);

    makeHeartbeatTimeout(chrono::system_clock::now());
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

bool RaftImpl::isHeartbeatTimeout(
        std::chrono::time_point<
            std::chrono::system_clock> time_now)
{
    return hb_time_ + hb_timeout_ < time_now;
}

void RaftImpl::updateHeartbeatTime(
        std::chrono::time_point<
            std::chrono::system_clock> next_hb_time)
{
    {
        auto hb_str = format_time(hb_time_);
        auto next_hb_str = format_time(next_hb_time);
        logdebug("selfid %" PRIu64 
                " update hb_time_ %s to next_hb_time %s", 
                getSelfId(), 
                hb_str.c_str(), next_hb_str.c_str());
    }
    hb_time_ = next_hb_time;
}

void RaftImpl::makeElectionTimeout(
        std::chrono::time_point<std::chrono::system_clock> tp)
{
    tp -= chrono::milliseconds{getElectionTimeout() + 1};
    updateActiveTime(tp);
}

void RaftImpl::makeHeartbeatTimeout(
        std::chrono::time_point<std::chrono::system_clock> tp)
{
    tp -= chrono::milliseconds{getHeartbeatTimeout() + 1};
    updateHeartbeatTime(tp);
}

std::unique_ptr<raft::HardState>
RaftImpl::getPendingHardState() const 
{
    if (0ull == pending_meta_seq_) {
        return nullptr;
    }

    return getCurrentHardState();
}

std::vector<std::unique_ptr<raft::Entry>>
RaftImpl::getPendingLogEntries() const
{
    if (0ull == pending_log_idx_) {
        assert(0ull == pending_log_seq_);
        return vector<unique_ptr<Entry>>{};
    }

    assert(0ull < pending_log_idx_);
    assert(0ull < pending_log_seq_);

    return getLogEntriesAfter(pending_log_idx_ - 1ull);
}

void RaftImpl::resetElectionTimeout()
{
    int next_election_timeout = rtimeout_();
    assert(0 < next_election_timeout);
    logdebug("election_timeout_ %d next_election_timeout %d", 
            election_timeout_.count(), next_election_timeout);
    election_timeout_ = chrono::milliseconds{next_election_timeout};
}

//bool RaftImpl::confirmMajority(
//        uint64_t major_match_index, 
//        const std::map<uint64_t, uint64_t>& match_indexes) const 
//{
//    return isMajority(
//            major_match_index, match_indexes, peer_ids_.size(), 
//            old_peer_ids_, new_peer_ids_);
//}

bool RaftImpl::isPeerUpToDate(uint64_t peer_commited_index) const
{
    assert(peer_commited_index <= commited_index_);
    return peer_commited_index == commited_index_;
}

void RaftImpl::assertNoPending() const
{
    assert(RaftRole::LEADER == getRole());
    assert(nullptr != replicate_states_);

    for (const auto& id_pending : replicate_states_->peekPendingState()) {
        if (getSelfId() == id_pending.first) {
            continue;
        }

        assert(false == id_pending.second);
    }
}

int RaftImpl::applyUnCommitedConfEntry(const Entry& conf_entry)
{
    ConfChange conf_change;
    auto ret = ApplyConfChange(conf_entry, true, conf_change, current_config_);
    if (0 != ret) {
        return ret;
    }

    // 3.
    if (nullptr == replicate_states_) {
        return 0;
    }

    assert(RaftRole::LEADER == getRole());
    replicate_states_->ApplyConfChange(conf_change, getLastLogIndex());
    return 0;
}

int RaftImpl::applyCommitedConfEntry(const Entry& conf_entry)
{
    ConfChange conf_change;
    return ApplyConfChange(conf_entry, false, conf_change, commited_config_);
}

//bool RaftImpl::isAValidPeer(uint64_t peer_id, MessageType msg_type)
//{
//    assert(RaftRole::LEADER == getRole());
//    switch (msg_type) {
//        case MessageType::MsgProp:         
//            return 0ull == peer_id;
//
//        case MessageType::MsgAppResp:
//            return current_config_.IsAReplicateMember(peer_id);
//
//        case MessageType::MsgHeartbeatResp:
//            return current_config_.IsAReplicateMember(peer_id);
//    }
//    // invalid msg type!!!
//    return true;
//}

int RaftImpl::reconstructCurrentConfig()
{
    assert(RaftRole::FOLLOWER == getRole());

    current_config_ = commited_config_;
    auto last_index = getLastLogIndex();
    for (auto index = 
            getCommitedIndex() + 1ull; index <= last_index; ++index) {
        auto entry = getLogEntry(index);
        assert(nullptr != entry);

        ConfChange conf_change;
        auto ret = ApplyConfChange(
                *entry, true, conf_change, current_config_);
        if (0 != ret) {
            return ret;
        }
        assert(0 == ret);
    }

    return 0;
}

} // namespace raft


