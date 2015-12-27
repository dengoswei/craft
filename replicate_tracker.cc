#include <algorithm>
#include "replicate_tracker.h"
#include "raft_impl.h"
#include "utils.h"

using namespace std;

namespace raft {

ReplicateTracker::ReplicateTracker(
        uint64_t selfid, 
        const std::set<uint64_t>& peer_ids, 
        uint64_t last_log_index, 
        size_t max_batch_size)
    : selfid_(selfid)
    , max_batch_size_(max_batch_size)
{
    assert(0ull < selfid);
    assert(0 < max_batch_size);

    for (auto id : peer_ids) {
        AddNode(id, last_log_index);
    }

    UpdateSelfState(last_log_index);
    assert(peer_ids_.end() != peer_ids_.find(selfid_));
}

void ReplicateTracker::AddNode(
        uint64_t peer_id, uint64_t last_log_index)
{
    if (peer_ids_.end() == peer_ids_.find(peer_id)) {
        logdebug("selfid %" PRIu64 " peer_id %" PRIu64 
                " last_log_index %" PRIu64 " peer_ids_.size %zu", 
                selfid_, peer_id, last_log_index, peer_ids_.size());
        peer_ids_.insert(peer_id);
        assert(next_indexes_.end() == next_indexes_.find(peer_id));
        next_indexes_[peer_id] = last_log_index + 1ull;
        match_indexes_[peer_id] = 0ull;
        next_batch_sizes_[peer_id] = size_t{0};
        pending_[peer_id] = false;
    }
    assert(peer_ids_.end() != peer_ids_.find(peer_id));
}

void ReplicateTracker::RemoveNode(uint64_t peer_id)
{
    if (peer_ids_.end() != peer_ids_.find(peer_id)) {
        logdebugPeerState(peer_id);
            
        peer_ids_.erase(peer_id);
        assert(next_indexes_.end() != next_indexes_.find(peer_id));
        next_indexes_.erase(peer_id);
        match_indexes_.erase(peer_id);
        next_batch_sizes_.erase(peer_id);
        pending_.erase(peer_id);
    }
}

void ReplicateTracker::UpdateSelfState(uint64_t last_log_index)
{
    next_indexes_[selfid_] = last_log_index + 1ull;
    match_indexes_[selfid_] = last_log_index;
    pending_[selfid_] = true; // always pending for selfid
}

std::vector<std::unique_ptr<Message>>
ReplicateTracker::BatchBuildMsgApp(RaftImpl& raft_impl)
{
    vector<unique_ptr<Message>> vec_msg;
    for (auto peer_id : peer_ids_) {
        auto msg_app = BuildMsgApp(raft_impl, peer_id);
        if (nullptr != msg_app) {
            vec_msg.emplace_back(move(msg_app));
        }
    }

    return vec_msg;
}

std::unique_ptr<Message>
ReplicateTracker::BuildMsgApp(RaftImpl& raft_impl, uint64_t peer_id)
{
    if (peer_ids_.end() == peer_ids_.find(peer_id)) {
        logdebug("leader: missing peer_id %" PRIu64, peer_id);
        return nullptr;
    }

    assert(peer_ids_.end() != peer_ids_.find(peer_id));
    logdebugPeerState(peer_id);
    if (true == pending_[peer_id]) {
        return nullptr;
    }

    auto batch_size = nextBatchSize(peer_id, raft_impl.getLastLogIndex());
    assert(0 <= batch_size);
    auto msg_app = raft_impl.buildMsgApp(
            peer_id, next_indexes_[peer_id], batch_size);
    if (nullptr != msg_app) {
        logdebug("TEST-INFO set pending_ true peer_id %" PRIu64, peer_id);
        pending_[peer_id] = true;
    }

    return msg_app;
}

std::vector<std::unique_ptr<Message>>
ReplicateTracker::BatchBuildMsgHeartbeat(RaftImpl& raft_impl)
{
    vector<unique_ptr<Message>> vec_msg;
    for (auto peer_id : peer_ids_) {
        auto msg_hb = BuildMsgHeartbeat(raft_impl, peer_id);
        assert(nullptr != msg_hb);
        vec_msg.emplace_back(move(msg_hb));
        assert(nullptr == msg_hb);
    }
    return vec_msg;
}

std::unique_ptr<Message>
ReplicateTracker::BuildMsgHeartbeat(
        RaftImpl& raft_impl, uint64_t peer_id)
{
    if (peer_ids_.end() == peer_ids_.find(peer_id)) {
        logdebug("leader: missing peer_id %" PRIu64, peer_id);
        return nullptr;
    }

    assert(peer_ids_.end() != peer_ids_.find(peer_id));
    logdebugPeerState(peer_id);

    // reset peer_id for every period of msg heart-beat
    pending_[peer_id] = false;
    logdebug("reset pending_ false peer_id %" PRIu64 , peer_id);

    // <peer_id, next_index>
    return raft_impl.buildMsgHeartbeat(peer_id, next_indexes_[peer_id]);
}

bool ReplicateTracker::UpdateReplicateState(
        RaftImpl& raft_impl,
        uint64_t peer_id, 
        bool reject, uint64_t /* reject_hint */, 
        uint64_t peer_next_index)
{
    logdebug("peer_id %" PRIu64 " reject %d peer_next_index %" PRIu64 
            " next_indexes_ %" PRIu64, 
            peer_id, int{reject}, peer_next_index, 
            next_indexes_[peer_id]);
    if (peer_ids_.end() == peer_ids_.find(peer_id)) {
        logdebug("leader: missing peer_id %" PRIu64, peer_id);
        return false;
    }

    assert(peer_ids_.end() != peer_ids_.find(peer_id));
    if (peer_next_index < next_indexes_[peer_id]) {
        // out-date msg
        return false;
    }

    assert(peer_next_index >= next_indexes_[peer_id]);
    // reset pending_ mark;
    pending_[peer_id] = false;
    logdebug("reset pending_ false peer_id %" PRIu64, peer_id);
    if (true == reject) {
        assert(next_indexes_[peer_id] > match_indexes_[peer_id] + 1ull);

        // decrease next_indexes_ & next_batch_size
        // TODO: use reject_hint ?
        uint64_t next_peer_index = 
            (next_indexes_[peer_id] - match_indexes_[peer_id]) / 2 + 
            match_indexes_[peer_id];
        assert(next_peer_index > match_indexes_[peer_id]);
        assert(next_peer_index < next_indexes_[peer_id]);

        next_indexes_[peer_id] = next_peer_index;
        next_batch_sizes_[peer_id] = next_batch_sizes_[peer_id] / 2;
        return true;
    }

    assert(false == reject);
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
   
    if (raft_impl.getTerm() == raft_impl.getLogTerm(new_match_index) && 
            new_match_index > raft_impl.getCommitedIndex()) {
        // update commited_index_
        // raft paper: joint consensus
        // Agreement(for elections and entry commitment) requires
        // seperate majorities from both the old and new configrations.
        // TODO
        if (raft_impl.confirmMajority(
                    new_match_index, match_indexes_)) {
            raft_impl.updateLeaderCommitedIndex(new_match_index);
        }
    }

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
            " next_batch_sizes_ %zu pending_ %d peer_ids_.size %zu", 
            selfid_, peer_id, next_indexes_[peer_id], 
            match_indexes_[peer_id], next_batch_sizes_[peer_id], 
            int{pending_[peer_id]}, peer_ids_.size());
}


} // namespace raft;


