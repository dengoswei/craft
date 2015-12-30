#include "raft_config.h"
#include "raft.pb.h"
#include "utils.h"
#include "replicate_tracker.h"


using namespace std;


namespace raft {

RaftConfig::RaftConfig(uint64_t selfid)
    : selfid_(selfid)
{

}

int RaftConfig::ApplyConfChange(
        const ConfChange& conf_change, bool check_pending)
{
    auto ret = 0;
    switch (conf_change.type()) 
    {
        case ConfChangeType::ConfChangeAddNode:
            ret = addNode(conf_change, check_pending);
            break;
        case ConfChangeType::ConfChangeRemoveNode:
            ret = removeNode(conf_change, check_pending);
            break;
        case ConfChangeType::ConfChangeUpdateNode:
            ret = updateNode(conf_change, check_pending);
            break;

        case ConfChangeType::ConfChangeAddCatchUpNode:
            ret = addCatchUpNode(conf_change);
            break;
        case ConfChangeType::ConfChangeRemoveCatchUpNode:
            ret = removeCatchUpNode(conf_change);
            break;
        default:
            assert(false);
            break;
    }

    return ret;
}

void RaftConfig::CommitConfChange(const ConfChange& conf_change)
{
    auto conf_type = conf_change.type();
    if (ConfChangeType::ConfChangeAddNode != conf_type || 
            ConfChangeType::ConfChangeRemoveNode != conf_type || 
            ConfChangeType::ConfChangeUpdateNode != conf_type) {
        return ;
    }

    pending_ = false;
    logdebug("selfid %" PRIu64 " reset pending false", selfid_);
}



int RaftConfig::addNode(
        const ConfChange& conf_change, bool check_pending)
{
    assert(ConfChangeType::ConfChangeAddNode == conf_change.type());
    if (true == check_pending && true == pending_) {
        return -1;
    }
    
    assert(false == pending_);
    auto node_id = conf_change.node_id();
    group_ids_.insert(node_id);
    replicate_group_ids_.insert(node_id);
    if (true == check_pending) {
        pending_ = true;
    }

    logdebug("selfid %" PRIu64 " conf change id %" PRIu64 
            " node_id %" PRIu64 " context.size %zu", 
            selfid_, conf_change.id(), conf_change.node_id(), 
            conf_change.context().size());
    return 0;
}

int RaftConfig::removeNode(
        const ConfChange& conf_change, bool check_pending)
{
    assert(ConfChangeType::ConfChangeRemoveNode == conf_change.type());
    if (true == check_pending && true == pending_) {
        // drop request
        return -1;
    }

    assert(false == pending_);
    auto node_id = conf_change.node_id();
    group_ids_.erase(node_id);
    replicate_group_ids_.insert(node_id);
    if (true == check_pending) {
        pending_ = true;
    }

    logdebug("selfid %" PRIu64 " conf change id %" PRIu64 
            " node_id %" PRIu64 " context.size %zu", 
            selfid_, conf_change.id(), conf_change.node_id(), 
            conf_change.context().size());
    return 0; 
}

int RaftConfig::updateNode(
        const ConfChange& conf_change, bool check_pending)
{
    assert(ConfChangeType::ConfChangeUpdateNode == conf_change.type());
    if (true == check_pending && true == pending_) {
        return -1;
    }

    assert(false == pending_);
    auto node_id = conf_change.node_id();
    if (group_ids_.end() == group_ids_.find(node_id)) {
        // nothing to update
        return -2;
    }
    if (true == check_pending) {
        pending_ = true;
    }

    // update what ?
    logdebug("selfid %" PRIu64 " conf change id %" PRIu64 
            " node_id %" PRIu64 " context.size %zu", 
            selfid_, conf_change.id(), conf_change.node_id(), 
            conf_change.context().size());
    return 0; 
}


int RaftConfig::addCatchUpNode(const ConfChange& conf_change)
{
    assert(ConfChangeType::ConfChangeAddCatchUpNode == conf_change.type());

    auto node_id = conf_change.node_id();
    replicate_group_ids_.insert(node_id);
    logdebug("selfid %" PRIu64 " conf change id %" PRIu64 
            " node_id %" PRIu64 " context.size %zu", 
            selfid_, conf_change.id(), conf_change.node_id(), 
            conf_change.context().size());
    return 0;
}

int RaftConfig::removeCatchUpNode(const ConfChange& conf_change)
{
    assert(ConfChangeType::ConfChangeRemoveCatchUpNode == 
            conf_change.type());
    
    auto node_id = conf_change.node_id();
    replicate_group_ids_.erase(node_id);
    logdebug("selfid %" PRIu64 " conf change id %" PRIu64 
            " node_id %" PRIu64 " context.size %zu", 
            selfid_, conf_change.id(), conf_change.node_id(), 
            conf_change.context().size());
    return 0;
}

std::vector<std::unique_ptr<Message>>
RaftConfig::BuildBroadcastMsg(const Message& msg_template)
{
    vector<unique_ptr<Message>> vec_msg;
    for (auto peer_id : group_ids_) {
        if (selfid_ == peer_id) {
            continue;
        }

        vec_msg.emplace_back(
                make_unique<Message>(msg_template));
        auto& msg = vec_msg.back();
        assert(nullptr != msg);
        msg->set_to(peer_id);
    }
    
    return vec_msg;
}

bool 
RaftConfig::IsMajorVoteYes(const std::map<uint64_t, bool>& votes) const
{
    return countMajor(true, votes, group_ids_);
}

bool RaftConfig::IsMajorCommited(
        uint64_t next_commited_index, 
        const std::map<uint64_t, uint64_t>& match_indexes) const
{
    return countMajor(next_commited_index, match_indexes, group_ids_);
}


std::unique_ptr<ReplicateTracker>
RaftConfig::CreateReplicateTracker(
        uint64_t last_log_index, size_t max_batch_size) const
{
    return make_unique<ReplicateTracker>(
            *this, last_log_index, max_batch_size);
}

} // namespace raft;


