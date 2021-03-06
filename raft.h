#pragma once

#include <set>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <stdint.h>
#include <functional>
#include "raft.pb.h"


namespace raft {

enum class RaftRole : uint8_t;

class RaftImpl;


struct RaftCallBack {
    std::function<std::tuple<
        int, std::unique_ptr<raft::Entry>>(uint64_t, uint64_t)> read;

    std::function<int(
            std::unique_ptr<raft::RaftState>, 
            std::vector<std::unique_ptr<raft::Entry>>)> write;

    std::function<
        int(std::vector<std::unique_ptr<raft::Message>>)> send;
};


class Raft {

public:
    Raft(
        uint64_t logid, uint64_t selfid, 
        const std::set<uint64_t>& group_ids, 
        int min_election_timeout, 
        int max_election_timeout, 
        RaftCallBack callback);

    Raft(
        uint64_t selfid, 
        int min_election_timeout, 
        int max_election_timeout, 
        const SnapshotMetadata& meta, 
        const RaftState* raft_state, 
        RaftCallBack callback);

    ~Raft();

    raft::ErrorCode Step(const raft::Message& msg);

    std::tuple<raft::ErrorCode, uint64_t>
        Propose(uint64_t index, 
                const std::vector<std::string>& vec_view);

    // err_code, commited_index, entry
    std::tuple<raft::ErrorCode, uint64_t, std::unique_ptr<Entry>> 
        Get(uint64_t index);

    // err_code, log_index
    std::tuple<raft::ErrorCode, uint64_t>
        TrySet(uint64_t index, 
                const std::vector<std::string>& entries);

    void Wait(uint64_t index);
    bool WaitFor(uint64_t index, const std::chrono::milliseconds timeout);

    uint64_t GetSelfId() const {
        return selfid_;
    }

    uint64_t GetLogId() const {
        return logid_;
    }

    bool IsFollower();
    bool IsLeader();
    bool IsCandidate();

    bool IsPending();

    raft::ErrorCode TryToBecomeLeader();
    raft::ErrorCode MakeTimeoutHeartbeat();

    uint64_t GetTerm();

    uint64_t GetCommitedIndex();

    uint64_t GetMaxIndex();

    uint64_t GetVoteFor();

    std::unique_ptr<raft::SnapshotMetadata> CreateSnapshotMetadata();

    // only for test
    void ReflashTimer(
            std::chrono::time_point<std::chrono::system_clock> time_now);

private:
    bool checkRole(raft::RaftRole role);

private:
    const uint64_t logid_ = 0ull;
    const uint64_t selfid_ = 0ull;

    std::mutex raft_mutex_;
    std::condition_variable raft_cv_;
    std::unique_ptr<RaftImpl> raft_impl_;

    std::mutex raft_prop_mutex_;

    RaftCallBack callback_;
}; 

} // namespace raft


