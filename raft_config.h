#pragma once

#include <map>
#include <set>
#include <vector>
#include <memory>
#include <stdint.h>

namespace raft {

class ConfChange;
class Message;
class ReplicateTracker;

class RaftConfig {

public:
    RaftConfig(uint64_t selfid);

    int ApplyConfChange(const ConfChange& conf_change, bool check_pending);

    void CommitConfChange(const ConfChange& conf_change);

    bool IsAGroupMember(uint64_t peer_id) const {
        return group_ids_.end() != group_ids_.find(peer_id);
    }

    bool IsAReplicateMember(uint64_t peer_id) const {
        return replicate_group_ids_.end() != 
            replicate_group_ids_.find(peer_id);
    }

    std::vector<std::unique_ptr<Message>>
        BuildBroadcastMsg(const Message& msg_template);

    bool IsMajorVoteYes(const std::map<uint64_t, bool>& votes) const;

    bool IsMajorCommited(
            uint64_t next_commited_index, 
            const std::map<uint64_t, uint64_t>& match_indexes) const;

    bool IsPending() const {
        return pending_;
    }

    std::unique_ptr<ReplicateTracker> 
        CreateReplicateTracker(
                uint64_t last_log_index, size_t max_batch_size) const;

    uint64_t GetSelfId() const {
        return selfid_;
    }

    const std::set<uint64_t>& GetReplicateGroup() const {
        return replicate_group_ids_;
    }

private:

    int addNode(const ConfChange& conf_change, bool check_pending);
    int removeNode(const ConfChange& conf_change, bool check_pending);
    int updateNode(const ConfChange& conf_change, bool check_pending);
    int addCatchUpNode(const ConfChange& conf_change);
    int removeCatchUpNode(const ConfChange& conf_change);

private:
    uint64_t selfid_ = 0ull;
    bool pending_ = false;

    std::set<uint64_t> group_ids_;
    std::set<uint64_t> replicate_group_ids_;
}; 



} // namespace raft


