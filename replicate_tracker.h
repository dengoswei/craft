#pragma once

#include <map>
#include <set>
#include <vector>
#include <memory>
#include <stdint.h>



namespace raft {

class Message;
class ConfChange;
class RaftImpl;
class RaftConfig;


class ReplicateTracker {

public:
    ReplicateTracker(
            const RaftConfig& current_config, 
            uint64_t last_log_index, 
            size_t max_batch_size);

    void AddNode(uint64_t peer_id, uint64_t last_log_index);

    void RemoveNode(uint64_t peer_id);

    void UpdateSelfState(uint64_t last_log_index);

    std::vector<std::unique_ptr<Message>>
        BatchBuildMsgApp(RaftImpl& raft_impl);

    std::unique_ptr<Message>
        BuildMsgApp(RaftImpl& raft_impl, uint64_t peer_id);

    std::vector<std::unique_ptr<Message>>
        BatchBuildMsgHeartbeat(RaftImpl& raft_impl);

    std::unique_ptr<Message>
        BuildMsgHeartbeat(RaftImpl& raft_impl, uint64_t peer_id);

    bool UpdateReplicateState(
            RaftImpl& raft_impl, 
            uint64_t peer_id, 
            bool reject, 
            uint64_t /* reject_hint */, 
            uint64_t peer_next_index);

    int ApplyConfChange(
            const ConfChange& conf_change, uint64_t last_log_index);

    // add for test
    const std::map<uint64_t, bool>& peekPendingState() const {
        return pending_;
    }


private:

    size_t nextBatchSize(uint64_t peer_id, uint64_t last_index);

    void logdebugPeerState(uint64_t peer_id);

private:
    const RaftConfig& current_config_;
//    uint64_t selfid_;
    const size_t max_batch_size_;
    // including selfid
//    std::set<uint64_t> peer_ids_;

    std::map<uint64_t, uint64_t> next_indexes_;
    std::map<uint64_t, uint64_t> match_indexes_;
    std::map<uint64_t, size_t> next_batch_sizes_;
    std::map<uint64_t, bool> pending_;
}; 

} // namespace raft;
