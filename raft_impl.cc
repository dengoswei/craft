#include "raft_impl.h"


using namespace std;

namespace {

using namespace raft;

inline void assert_role(RaftImpl& raft_impl, RaftRole expected_role)
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


}


namespace raft {

const uint64_t META_INDEX = 0ull;

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
    raft_impl.setRole(RaftRole::CANDIDATE);
    
    // candidate::onTimeout will start a new election
    return ::raft::candidate::onTimeout(raft_impl, time_now);
}

MessageType onStepMessage(RaftImpl& raft_impl, const Message& msg)
{
    assert_role(raft_impl, RaftRole::FOLLOWER);
    assert_term(raft_impl, msg.term());

    // TODO
    auto time_now = std::chrono::system_clock::now();
    auto rsp_msg_type = MessageType::MsgNull;
    switch (msg.type()) {
        case MessageType::MsgHeartbeat:
            // leader heart beat msg
            rsp_msg_type = MessageType::MsgHeartbeatResp;
            raft_impl.updateActiveTime(time_now);
            break;
        case MessageType::MsgApp:
            // leader appendEntry msg
            // TODO

            rsp_msg_type = MessageType::MsgAppResp;
            raft_impl.updateActiveTime(time_now);
            break;

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
                raft_impl.setVoteFor(false, msg.from()); 
            }

            // check getVoteFor() != msg.from() => reject!
            rsp_msg_type = MessageType::MsgVoteResp;
            raft_impl.updateActiveTime(time_now);
            logdebug("selfid %" PRIu64 " handle MsgVote");
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

    switch (msg.type()) {
        case MessageType::MsgVoteResp:
            // collect vote resp
            raft_impl.updateVote(msg.from(), !msg.reject());
            if (raft_impl.isMajorVoteYes()) {
                // step as leader: TODO ? 
                raft_impl.setRole(RaftRole::LEADER);
            }

            logdebug("selfid %" PRIu64 " handle MsgVoteResp");
            break;
        default:
            logdebug("IGNORE: recv msg type %d", static_cast<int>(msg.type()));
            // TODO: ?
            break;
    }

    return MessageType::MsgNull;
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
            // client prop
            break;

        case MessageType::MsgAppResp:
            // collect appendEntry resp
            break;

        case MessageType::MsgHeartbeatResp:
            // collect heartbeat resp
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
        uint64_t logid, uint64_t selfid, uint64_t group_size, int election_timeout)
    : logid_(logid)
    , selfid_(selfid)
    , group_size_(group_size)
    , election_timeout_(election_timeout)
    , active_time_(chrono::system_clock::now())
{
    assert(3ull <= group_size_);
    assert(0 < election_timeout_.count());
    setRole(RaftRole::FOLLOWER);
}

RaftImpl::~RaftImpl() = default;

MessageType RaftImpl::CheckTerm(uint64_t msg_term)
{
    auto rsp_msg_type = MessageType::MsgNull;
    // raft paper: rules for servers: 5.1
    // => If RPC request or response contains term T > currentTerm: 
    //    set currentTerm = T, convert to follower;
    if (msg_term != term_) {
        if (msg_term > term_) {
            setRole(RaftRole::FOLLOWER);
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

std::unique_ptr<Message> RaftImpl::produceRsp(
        const Message& req_msg, MessageType rsp_msg_type)
{
    assert(req_msg.logid() == logid_);
    assert(req_msg.to() == selfid_);


    if (MessageType::MsgNull == rsp_msg_type) {
        return nullptr;
    }

    auto rsp_msg = make_unique<Message>();
    assert(nullptr != rsp_msg);
    rsp_msg->set_type(rsp_msg_type);
    rsp_msg->set_logid(logid_);
    rsp_msg->set_from(selfid_);
    rsp_msg->set_term(term_);

    switch (rsp_msg_type) {

    case MessageType::MsgVote:
        // raft paper:
        // RequestVote RPC Arguments:
        // - term
        // - candidicateId
        // - lastLogIndex
        // - lastLogTerm
        rsp_msg->set_index(getLastLogIndex());
        rsp_msg->set_log_term(getLastLogTerm());
        rsp_msg->set_to(0ull); // broad-cast
        logdebug("MsgVote term %" PRIu64 " candidate %" PRIu64 
                " lastLogIndex %" PRIu64 " lastLogTerm %" PRIu64, 
                term_, selfid_, rsp_msg->index(), rsp_msg->log_term());
        break;

    case MessageType::MsgVoteResp:
        // raft paper:
        // RequestVote RPC Results:
        // - term 
        // - voteGranted
        rsp_msg->set_reject(req_msg.from() != getVoteFor());
        rsp_msg->set_to(req_msg.from());
        logdebug("MsgVoteResp term %" PRIu64 " req_msg.from %" PRIu64 
                " getVoteFor %" PRIu64 " reject %d", 
                term_, req_msg.from(), getVoteFor(), 
                static_cast<int>(rsp_msg->reject()));
        break;

    case MessageType::MsgNull:
        // DO NOTHING ?
        break;

    default:
        hassert(false, "invalid rsp_msg_type %d", 
                static_cast<int>(rsp_msg_type));
        break;
    }

    return rsp_msg;
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
            selfid_, reset, vote_for_, candidate);
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

uint64_t RaftImpl::getLastLogIndex() const 
{
    if (true == logs_.empty()) {
        assert(0 == commited_index_);
        assert(0 == max_index_);
        return 0ull;
    }

    assert(nullptr != logs_.back());
    assert(nullptr != logs_.front());
    assert(logs_.front()->index() + logs_.size() == logs_.back()->index());
    return logs_.back()->index();
}

uint64_t RaftImpl::getLastLogTerm() const 
{
    if (true == logs_.empty()) {
        assert(0 == commited_index_);
        assert(0 == max_index_);
        return 0ull;
    }

    assert(nullptr != logs_.back());
    assert(term_ >= logs_.back()->term());
    return logs_.back()->term();
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

bool RaftImpl::isMajorVoteYes()
{
    assert_role(*this, RaftRole::CANDIDATE);
    int true_cnt = 0;
    int false_cnt = 0;
    tie(true_cnt, false_cnt) = countVotes(vote_resps_);
    logdebug("selfid %" PRIu64 " group_size %" PRIu64 
            " true_cnt %d false_cnt %d", 
            selfid_, group_size_, true_cnt, false_cnt);
    return true_cnt >= (group_size_ / 2 + 1);
}

} // namespace raft


