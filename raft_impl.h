#pragma once

#include <chrono>


namespace raft {


enum class RaftRole : uint8_t {
    LEADER = 1;
    CANDIDATE = 2;
    FOLLOWER = 3;
};


enum class ActionCode : uint8_t {
    LOG_META = 1;
    LOG_ENTRY = 2;
};

class RaftImpl {


public:
    RaftImpl(uint64_t logid, uint64_t selfid, uint64_t group_size);

    ~RaftImpl();

    ActionCode evaluate(const Message& msg);

    // error, store_seq, rsp_msg_type
    std::tuple<
        ErrorCode, uint64_t, MessageType> evaluateRole(const Message& msg);

private:
    void setRole(RaftRole new_role);

    void setTerm(uint64_t new_term);

private:
    RaftRole role_ = RaftRole::FOLLOWER;

    uint64_t logid_ = 0;
    uint64_t selfid_ = 0;
    uint64_t group_size_ = 0;

    uint64_t term_ = 0;
    uint64_t vote_for_ = 0;
    uint64_t commited_index_ = 0;
    uint64_t max_index_ = 0;

    std::chrono::time_point<std::chrono::system_clock> active_time_;
};


} // namespace raft;


