#include <algorithm>
#include "replicate_tracker.h"
#include "raft.pb.h"
#include "utils.h"
#include "log_utils.h"

using namespace std;

namespace raft {

ReplicateTracker::ReplicateTracker(
        uint64_t selfid, 
        const std::set<uint64_t>& replicate_group, 
        uint64_t last_log_index, 
        size_t max_batch_size)
    : selfid_(selfid)
    , max_batch_size_(max_batch_size)
{
    assert(0ull < selfid_);
    assert(0 < max_batch_size);

    for (auto id : replicate_group) {
        AddNode(id, last_log_index);
    }

    UpdateSelfState(last_log_index);
}

void ReplicateTracker::AddNode(
        uint64_t peer_id, uint64_t last_log_index)
{
    if (next_indexes_.end() == next_indexes_.find(peer_id)) {
        logdebug("selfid %" PRIu64 " peer_id %" PRIu64 
                " last_log_index %" PRIu64 " peer_ids_.size %zu", 
                selfid_, peer_id, last_log_index, next_indexes_.size());

        next_indexes_[peer_id] = last_log_index + 1ull;
        match_indexes_[peer_id] = 0ull;
        next_batch_sizes_[peer_id] = size_t{0};
        pending_[peer_id] = false;
    }
    assert(next_indexes_.end() != next_indexes_.find(peer_id));
}

void ReplicateTracker::RemoveNode(uint64_t peer_id)
{
    if (next_indexes_.end() != next_indexes_.find(peer_id)) {
        logdebugPeerState(peer_id);
            
        next_indexes_.erase(peer_id);
        match_indexes_.erase(peer_id);
        next_batch_sizes_.erase(peer_id);
        pending_.erase(peer_id);
    }

    assert(next_indexes_.end() == next_indexes_.find(peer_id));
}

void ReplicateTracker::UpdateSelfState(uint64_t last_log_index)
{
    assert(last_seen_index_ <= last_log_index);
    last_seen_index_ = last_log_index;
    if (next_indexes_.end() == next_indexes_.find(selfid_)) {
        return ;
    }

    next_indexes_[selfid_] = last_log_index + 1ull;
    match_indexes_[selfid_] = last_log_index;
    pending_[selfid_] = true; // always pending for selfid
}

std::unique_ptr<Message>
ReplicateTracker::BuildMsgApp(
        uint64_t last_log_index, 
        uint64_t peer_id, BuildMsgCB build_msg_cb)
{
    if (next_indexes_.end() == next_indexes_.find(peer_id)) {
        logdebug("leader: missing peer_id %" PRIu64, peer_id);
        return nullptr;
    }

    assert(next_indexes_.end() != next_indexes_.find(peer_id));
    logdebugPeerState(peer_id);
    if (true == pending_[peer_id]) {
        return nullptr;
    }

    auto batch_size = nextBatchSize(peer_id, last_log_index);
    assert(0 <= batch_size);
    auto msg_app = build_msg_cb(
            peer_id, next_indexes_[peer_id], batch_size);
    if (nullptr != msg_app) {
        logdebug("TEST-INFO set pending_ true peer_id %" PRIu64, peer_id);
        pending_[peer_id] = true;
    }

    return msg_app;
}

std::unique_ptr<Message>
ReplicateTracker::BuildMsgHeartbeat(
        uint64_t peer_id, BuildMsgCB build_msg_cb)
{
    if (next_indexes_.end() == next_indexes_.find(peer_id)) {
        logdebug("leader: missing peer_id %" PRIu64 , peer_id);
        return nullptr;
    }

    assert(next_indexes_.end() != next_indexes_.find(peer_id));
    logdebugPeerState(peer_id);

    // reset peer_id pending state
    pending_[peer_id] = false;
    return build_msg_cb(peer_id, next_indexes_[peer_id], size_t{0});
}

bool ReplicateTracker::UpdateReplicateState(
        uint64_t peer_id, 
        bool reject, uint64_t /* reject_hint */, 
        uint64_t peer_next_index)
{
    logdebug("peer_id %" PRIu64 " reject %d peer_next_index %" PRIu64 
            " next_indexes_ %" PRIu64, 
            peer_id, int{reject}, peer_next_index, 
            next_indexes_[peer_id]);
    if (next_indexes_.end() == next_indexes_.find(peer_id)) {
        logdebug("leader: missing peer_id %" PRIu64, peer_id);
        return false;
    }

    assert(next_indexes_.end() != next_indexes_.find(peer_id));
    if (true == reject) {
        pending_[peer_id] = false;
        assert(next_indexes_[peer_id] > 
                match_indexes_[peer_id] + 1ull);

        // decrease next_indexes_ & next_batch_size
        // TODO: use reject_hint ?
        uint64_t next_peer_index = 
            (next_indexes_[peer_id] - match_indexes_[peer_id]) / 2 + 
            match_indexes_[peer_id];
        assert(next_peer_index > match_indexes_[peer_id]);
        assert(next_peer_index <= next_indexes_[peer_id]);

        next_batch_sizes_[peer_id] = next_batch_sizes_[peer_id] / 2;
        if (next_peer_index == next_indexes_[peer_id]) {
            logerr("selfid %" PRIu64 " possible fake leader", selfid_);
            return false;
        }

        next_indexes_[peer_id] = next_peer_index;
        return true;
    }

    assert(false == reject);
    logdebug("peer_next_index %" PRIu64 
            " next_indexes_[%" PRIu64 "] %" PRIu64, 
            peer_next_index, peer_id, next_indexes_[peer_id]);
    if (peer_next_index < next_indexes_[peer_id]) {
        // out-date msg
        return false;
    }

    pending_[peer_id] = false;
    assert(peer_next_index >= next_indexes_[peer_id]);
    assert(match_indexes_[peer_id] < next_indexes_[peer_id]);

    // update next_batch_sizes to at least 1 anyway
    next_batch_sizes_[peer_id] = max(
            size_t{1}, next_batch_sizes_[peer_id]);
    const auto new_match_index = peer_next_index - 1ull;
    assert(match_indexes_[peer_id] <= new_match_index);
    if (new_match_index == match_indexes_[peer_id]) {
        assert(peer_next_index == next_indexes_[peer_id]);
        return false;
    }

    match_indexes_[peer_id] = new_match_index;
    next_indexes_[peer_id] = peer_next_index;
    next_batch_sizes_[peer_id] = min(
            max_batch_size_, next_batch_sizes_[peer_id] * 2);

    return true;
}

size_t 
ReplicateTracker::nextBatchSize(uint64_t peer_id, uint64_t last_index)
{
    return min<size_t>(
            next_batch_sizes_[peer_id], 
            last_index + 1ull - next_indexes_[peer_id]);
}

void ReplicateTracker::logdebugPeerState(uint64_t peer_id)
{
    logdebug("selfid %" PRIu64 " peer_id %" PRIu64 
            " next_indexes_ %" PRIu64 " match_indexes_ %" PRIu64
            " next_batch_sizes_ %zu pending_ %d next_indexes_.size %zu", 
            selfid_, 
            peer_id, next_indexes_[peer_id], 
            match_indexes_[peer_id], next_batch_sizes_[peer_id], 
            int{pending_[peer_id]}, 
            next_indexes_.size());
}

int ReplicateTracker::ApplyConfChange(
        const ConfChange& conf_change, uint64_t last_log_index)
{
    auto node_id = conf_change.node_id();
    switch (conf_change.type()) 
    {
        case ConfChangeType::ConfChangeAddNode:
        case ConfChangeType::ConfChangeAddCatchUpNode:
            AddNode(node_id, last_log_index);
            break;
        case ConfChangeType::ConfChangeRemoveNode:
        case ConfChangeType::ConfChangeRemoveCatchUpNode:
            RemoveNode(node_id);
            break;
    }

    return 0;
}

} // namespace raft;



