#pragma once

#include <chrono>
#include <set>
#include <map>
#include <deque>
#include <memory>
#include <functional>
#include <stdint.h>
#include "utils.h"
#include "raft.pb.h"
#include "gsl.h"


namespace raft {

enum class RaftRole : uint8_t {
    LEADER = 1, 
    CANDIDATE = 2, 
    FOLLOWER = 3, 
};



// not thread safe
class RaftImpl {

private:

    using TimeoutHandler = 
        std::function<MessageType(
                RaftImpl&, std::chrono::time_point<std::chrono::system_clock>)>;

    using StepMessageHandler = 
        std::function<MessageType(RaftImpl&, const Message&)>;

public:
    RaftImpl(uint64_t logid, uint64_t selfid, 
            std::set<uint64_t> peer_ids, int election_timeout);

    ~RaftImpl();

    MessageType CheckTerm(uint64_t msg_term);

    MessageType CheckTimout(
            std::chrono::time_point<std::chrono::system_clock> time_now);

    MessageType step(const Message& msg);

    std::vector<std::unique_ptr<Message>> 
        produceRsp(const Message& req_msg, MessageType rsp_msg_type);

    std::unique_ptr<Message> buildMsgApp(
            uint64_t peer_id, uint64_t next_index, size_t max_batch_size);

    std::vector<std::unique_ptr<Message>> 
        batchBuildMsgAppUpToDate(size_t max_batch_size);

    std::vector<std::unique_ptr<Message>> 
        batchBuildMsgApp(size_t max_batch_size);

    std::unique_ptr<Message> 
        buildMsgHeartbeat(uint64_t peer_id, uint64_t next_index) const;

    std::vector<std::unique_ptr<Message>> batchBuildMsgHeartbeat();

    bool isUpToDate(
            uint64_t peer_log_term, uint64_t peer_max_index);

    void appendLogs(gsl::array_view<const Entry> entries);
    void appendEntries(
            uint64_t prev_log_index, 
            uint64_t prev_log_term, 
            uint64_t leader_commited_index, 
            gsl::array_view<const Entry> entries);

    int checkAndAppendEntries(
            uint64_t prev_log_index, 
            gsl::array_view<const Entry> entries);

public:
    uint64_t getSelfId() const {
        return selfid_;
    }

    RaftRole getRole() const {
        return role_;
    }

    void setRole(RaftRole new_role);

    uint64_t getTerm() const {
        return term_;
    }

    void setTerm(uint64_t new_term);

    uint64_t getVoteFor() const {
        return vote_for_;
    }

    void setVoteFor(bool reset, uint64_t candidate);

    uint64_t getLeader() const {
        return leader_id_;
    }

    void setLeader(bool reset, uint64_t leader_id);

    uint64_t pendingStoreSeq(uint64_t index) const;
    uint64_t assignStoreSeq(uint64_t index);

    int getElectionTimout() const {
        return election_timeout_.count();
    }

    void updateActiveTime(
            std::chrono::time_point<std::chrono::system_clock> time_now);

    uint64_t getLastLogTerm() const;
    uint64_t getLastLogIndex() const;

    uint64_t getBaseLogTerm() const;
    uint64_t getBaseLogIndex() const;

    uint64_t getLogTerm(uint64_t log_index) const;
    bool isIndexInMem(uint64_t log_index) const;

    void beginVote();
    void updateVote(uint64_t peer_id, bool current_rsp);
    bool isMajorVoteYes() const;

    void updateCommitedIndex(uint64_t leader_commited_index);
    bool isMatch(uint64_t log_index, uint64_t log_term) const;

    uint64_t findConflict(gsl::array_view<const Entry> entries) const;
    
    bool updatePeerReplicateState(
            uint64_t peer_id, 
            bool reject, uint64_t reject_hint, uint64_t peer_next_index);

    void becomeFollower();
    void becomeCandidate();
    void becomeLeader();

private:
    RaftRole role_ = RaftRole::FOLLOWER;
    TimeoutHandler timeout_handler_;
    StepMessageHandler step_handler_;

    uint64_t logid_ = 0;
    uint64_t selfid_ = 0;
    std::set<uint64_t> peer_ids_;

    uint64_t term_ = 0;
    uint64_t vote_for_ = 0;
    uint64_t commited_index_ = 0;

    uint64_t leader_id_ = 0;

    std::deque<std::unique_ptr<Entry>> logs_;

    uint64_t store_seq_ = 0;
    std::map<uint64_t, uint64_t> pending_store_;

    std::chrono::milliseconds election_timeout_;
    std::chrono::time_point<std::chrono::system_clock> active_time_;

    // status of peers
    // # candidcate #
    std::map<uint64_t, bool> vote_resps_;

    // # leader #
    std::map<uint64_t, uint64_t> next_indexes_;
    std::map<uint64_t, uint64_t> match_indexes_;
    // TODO:
    // trace peer_ids need log entries not in mem
    std::set<uint64_t> ids_not_in_mem_;
    // std::map<uint64_t, uint64_t> probe_indexes_; // last reject index
};


} // namespace raft;


