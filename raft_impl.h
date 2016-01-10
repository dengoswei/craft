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
#include "raft_config.h"


namespace raft {

class ReplicateTracker;

enum class RaftRole : uint8_t {
    LEADER = 1, 
    CANDIDATE = 2, 
    FOLLOWER = 3, 
};

extern const size_t MAX_BATCH_SIZE;

// not thread safe
class RaftImpl {

private:

    using TimeoutHandler = 
        std::function<MessageType(
                RaftImpl&, 
                std::chrono::time_point<std::chrono::system_clock>)>;

    using StepMessageHandler = 
        std::function<MessageType(RaftImpl&, const Message&)>;

public:
    RaftImpl(uint64_t logid, 
            uint64_t selfid, 
            const std::set<uint64_t>& group_ids, 
            int min_election_timeout, 
            int max_election_timeout);

    ~RaftImpl();

    MessageType CheckTerm(uint64_t msg_term);

    MessageType CheckTimout(
            std::chrono::time_point<std::chrono::system_clock> time_now);

    MessageType step(const Message& msg);

    std::vector<std::unique_ptr<Message>> 
        produceRsp(const Message& req_msg, MessageType rsp_msg_type);

    std::unique_ptr<Message> buildMsgApp(
            uint64_t peer_id, uint64_t next_index, size_t max_batch_size);
//
//    std::vector<std::unique_ptr<Message>> 
//        batchBuildMsgAppUpToDate(size_t max_batch_size);
//
//    std::vector<std::unique_ptr<Message>> 
//        batchBuildMsgApp(size_t max_batch_size);
//

    std::unique_ptr<Message> 
        buildMsgHeartbeat(uint64_t peer_id, uint64_t next_index) const;
//
//    std::vector<std::unique_ptr<Message>> batchBuildMsgHeartbeat();

    bool isUpToDate(
            uint64_t peer_log_term, uint64_t peer_max_index);

    int appendLogs(gsl::array_view<const Entry*> entries);
    int appendEntries(
            uint64_t prev_log_index, 
            uint64_t prev_log_term, 
            uint64_t leader_commited_index, 
            gsl::array_view<const Entry*> entries);

    int checkAndAppendEntries(
            uint64_t prev_log_index, 
            gsl::array_view<const Entry*> entries);

    std::vector<std::unique_ptr<raft::Entry>>
        getLogEntriesAfter(uint64_t log_index) const;

    std::unique_ptr<raft::HardState>
        getCurrentHardState() const;

    std::unique_ptr<raft::HardState>
        getPendingHardState() const;

    std::vector<std::unique_ptr<raft::Entry>>
        getPendingLogEntries() const;

//    bool confirmMajority(
//            uint64_t major_match_index, 
//            const std::map<uint64_t, uint64_t>& match_index) const;

    // test helper function
    void makeElectionTimeout(
            std::chrono::time_point<std::chrono::system_clock> tp);

    void makeHeartbeatTimeout(
            std::chrono::time_point<std::chrono::system_clock> tp);

    void assertNoPending() const;

    int applyUnCommitedConfEntry(const Entry& conf_entry);

    int applyCommitedConfEntry(const Entry& conf_entry);

    int reconstructCurrentConfig();

    const RaftConfig& GetCurrentConfig() const {
        return current_config_;
    }

    const RaftConfig& GetCommitedConfig() const {
        return commited_config_;
    }

    ReplicateTracker& GetReplicateTracker() {
        assert(nullptr != replicate_states_);
        return *replicate_states_;
    }

public:


    uint64_t getLogId() const {
        return logid_;
    }

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

    uint64_t getCommitedIndex() const {
        return commited_index_;
    }

    uint64_t getVoteFor() const {
        return vote_for_;
    }

    void setVoteFor(bool reset, uint64_t candidate);

    uint64_t getLeader() const {
        return leader_id_;
    }

    void setLeader(bool reset, uint64_t leader_id);

    uint64_t assignStoreSeq(uint64_t index);

    // meta_seq, log_idx, log_seq
    std::tuple<uint64_t, uint64_t, uint64_t> getStoreSeq() const;

    // commited
    void commitedStoreSeq(
            uint64_t meta_seq, uint64_t log_idx, uint64_t log_seq);

    int getElectionTimeout() const {
        return election_timeout_.count();
    }

    int getHeartbeatTimeout() const {
        return hb_timeout_.count();
    }

    void updateActiveTime(
            std::chrono::time_point<std::chrono::system_clock> time_now);

    uint64_t getLastLogTerm() const;
    uint64_t getLastLogIndex() const;

    uint64_t getBaseLogTerm() const;
    uint64_t getBaseLogIndex() const;

    // base_index, last_index
    std::tuple<uint64_t, uint64_t> getInMemIndex() const;

    const Entry* getLogEntry(uint64_t log_index) const;
    uint64_t getLogTerm(uint64_t log_index) const;
    bool isIndexInMem(uint64_t log_index) const;

    void beginVote();
    void updateVote(uint64_t peer_id, bool current_rsp);
    bool isMajorVoteYes() const;

    bool isPeerUpToDate(uint64_t peer_commited_index) const;

    void updateLeaderCommitedIndex(uint64_t new_commited_index);
    void updateFollowerCommitedIndex(uint64_t leader_commited_index);
    bool isMatch(uint64_t log_index, uint64_t log_term) const;

    uint64_t findConflict(gsl::array_view<const Entry*> entries) const;
    
    bool updateReplicateState(
            uint64_t peer_id, 
            bool reject, uint64_t reject_hint, uint64_t peer_next_index);

    void becomeFollower(uint64_t term);
    void becomeCandidate();
    void becomeLeader();


    void resetElectionTimeout();
    bool isHeartbeatTimeout(
            std::chrono::time_point<
                std::chrono::system_clock> time_now);
    void updateHeartbeatTime(
            std::chrono::time_point<
                std::chrono::system_clock> next_hb_time);

private:
    RaftRole role_ = RaftRole::FOLLOWER;
    TimeoutHandler timeout_handler_;
    StepMessageHandler step_handler_;

    uint64_t logid_ = 0;
    uint64_t selfid_ = 0;

    RaftConfig current_config_;
    RaftConfig commited_config_;

    uint64_t term_ = 0;
    uint64_t vote_for_ = 0;
    uint64_t commited_index_ = 0;

    uint64_t leader_id_ = 0;

    std::deque<std::unique_ptr<Entry>> logs_;

    uint64_t store_seq_ = 0;
    uint64_t pending_meta_seq_ = 0;
    uint64_t pending_log_idx_ = 0;
    uint64_t pending_log_seq_ = 0;

    RandomTimeout rtimeout_;
    std::chrono::milliseconds election_timeout_;
    std::chrono::time_point<std::chrono::system_clock> active_time_;

    // status of peers
    // # candidcate #
    std::map<uint64_t, bool> vote_resps_;

    // # leader #
    std::unique_ptr<ReplicateTracker> replicate_states_;

    std::chrono::milliseconds hb_timeout_;
    std::chrono::time_point<std::chrono::system_clock> hb_time_;
    // TODO:
    // trace peer_ids need log entries not in mem
    std::set<uint64_t> ids_not_in_mem_;
};


} // namespace raft;
