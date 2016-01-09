#pragma once

#include <map>
#include <set>
#include <vector>
#include <memory>
#include <functional>
#include <stdint.h>



namespace raft {

class Message;
class ConfChange;

using BuildMsgCB = std::function<
    std::unique_ptr<Message>(uint64_t, uint64_t, size_t)>;

class ReplicateTracker {

public:
    ReplicateTracker(
            uint64_t selfid, 
            const std::set<uint64_t>& replicate_group, 
            uint64_t last_log_index, 
            size_t max_batch_size);

    void UpdateSelfState(uint64_t last_log_index);

    std::unique_ptr<Message>
        BuildMsgApp(
                uint64_t last_log_index, 
                uint64_t peer_id, BuildMsgCB build_msg_cb);

    std::unique_ptr<Message>
        BuildMsgHeartbeat(
                uint64_t peer_id, BuildMsgCB build_msg_cb);

    bool UpdateReplicateState(
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

    // add for test
    const std::map<uint64_t, uint64_t> peekNextIndexes() const {
        return next_indexes_;
    }

    const std::map<uint64_t, uint64_t> peekMatchIndexes() const {
        return match_indexes_;
    }

    void AddNode(uint64_t peer_id, uint64_t last_log_index);

    void RemoveNode(uint64_t peer_id);

private:

    size_t nextBatchSize(uint64_t peer_id, uint64_t last_index);

    void logdebugPeerState(uint64_t peer_id);

private:
    uint64_t selfid_;
    const size_t max_batch_size_;

    std::map<uint64_t, uint64_t> next_indexes_;
    std::map<uint64_t, uint64_t> match_indexes_;
    std::map<uint64_t, size_t> next_batch_sizes_;
    std::map<uint64_t, bool> pending_;
}; 

} // namespace raft;
