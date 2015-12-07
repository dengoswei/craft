#pragma once

#include <chrono>
#include <map>
#include <queue>
#include <memory>
#include <functional>
#include <stdint.h>
#include "utils.h"
#include "raft.pb.h"


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
    RaftImpl(uint64_t logid, 
            uint64_t selfid, uint64_t group_size, int election_timeout);

    ~RaftImpl();

    // error, meta store_seq
    MessageType CheckTerm(uint64_t msg_term);

    // error, meta store_seq, rsp_msg_type
    MessageType CheckTimout(
            std::chrono::time_point<std::chrono::system_clock> time_now);


    MessageType step(const Message& msg);

    std::unique_ptr<Message> produceRsp(
            const Message& req_msg, MessageType rsp_msg_type);

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

    uint64_t pendingStoreSeq(uint64_t index) const;
    uint64_t assignStoreSeq(uint64_t index);

    int getElectionTimout() const {
        return election_timeout_.count();
    }

    void updateActiveTime(
            std::chrono::time_point<std::chrono::system_clock> time_now);

    bool isUpToDate(
            uint64_t peer_log_term, uint64_t peer_max_index);

    uint64_t getLastLogTerm() const;
    uint64_t getLastLogIndex() const;

    void beginVote();
    void updateVote(uint64_t peer_id, bool current_rsp);
    bool isMajorVoteYes();

private:
    RaftRole role_ = RaftRole::FOLLOWER;
    TimeoutHandler timeout_handler_;
    StepMessageHandler step_handler_;

    uint64_t logid_ = 0;
    uint64_t selfid_ = 0;
    uint64_t group_size_ = 0;

    uint64_t term_ = 0;
    uint64_t vote_for_ = 0;
    uint64_t commited_index_ = 0;
    uint64_t max_index_ = 0;

    std::queue<std::unique_ptr<Entry>> logs_;

    uint64_t store_seq_ = 0;
    std::map<uint64_t, uint64_t> pending_store_;

    std::chrono::milliseconds election_timeout_;
    std::chrono::time_point<std::chrono::system_clock> active_time_;

    std::map<uint64_t, bool> vote_resps_;
};


} // namespace raft;


