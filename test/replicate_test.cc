#include "gtest/gtest.h"
#include "raft_config.h"
#include "replicate_tracker.h"
#include "raft.pb.h"
#include "test_helper.h"


using namespace std;
using namespace raft;
using namespace test;

void checkConsistence(RaftConfig& config, ReplicateTracker& replicate)
{
    const auto& rep_set = config.GetReplicateGroup();

    const auto& next_indexes = replicate.peekNextIndexes();
    const auto& match_indexes = replicate.peekMatchIndexes();
    const auto& pending_state = replicate.peekPendingState();
    for (auto id : rep_set) {
        assert(next_indexes.end() != next_indexes.find(id));
        assert(match_indexes.end() != match_indexes.find(id));
        assert(pending_state.end() != pending_state.find(id));
    }
}



TEST(TestReplicateTracker, Create)
{
    auto config = buildTestConfig();
    const uint64_t last_log_index = 0ull;
    auto replicate_tracker = 
        config.CreateReplicateTracker(last_log_index, size_t{10});
    assert(nullptr != replicate_tracker);

    checkConsistence(config, *replicate_tracker);

    const auto selfid = config.GetSelfId();
    const auto& rep_set = config.GetReplicateGroup();
    // 1. 
    {
        const auto& next_indexes = 
            replicate_tracker->peekNextIndexes();
        const auto& match_indexes = 
            replicate_tracker->peekMatchIndexes();
        const auto& pending_state = 
            replicate_tracker->peekPendingState();

        assert(next_indexes.size() == rep_set.size());
        assert(match_indexes.size() == rep_set.size());
        assert(pending_state.size() == rep_set.size());
        for (auto id : rep_set) {
            assert(next_indexes.end() != next_indexes.find(id));
            assert(last_log_index + 1ull == next_indexes.at(id));
            assert(0ull == match_indexes.at(id));
            assert((selfid == id) == pending_state.at(id));
        }
    }
}

TEST(TestReplicateTracker, AddRemoveNode)
{
    auto config = buildTestConfig();
    auto replicate = config.CreateReplicateTracker(0ull, size_t{10});
    assert(nullptr != replicate);
    
    checkConsistence(config, *replicate);
    // 1. add node
    {
        assert(0 == addCatchUpNode(config, 4ull));
        replicate->AddNode(4ull, 0ull);
        checkConsistence(config, *replicate);
    }

    // 2. remove node
    {
        assert(0 == removeCatchUpNode(config, 4ull));
        replicate->RemoveNode(4ull);
        checkConsistence(config, *replicate);
    }
}

TEST(TestReplicateTracker, UpdateReplicateState)
{
    auto config = buildTestConfig();
    auto replicate = config.CreateReplicateTracker(0ull, size_t{10});
    assert(nullptr != replicate);

    // 1.
    {
        auto peer_id = 2ull;
        auto peer_next_index = 2ull;
        auto update = 
            replicate->UpdateReplicateState(
                    peer_id, false, 0ull, peer_next_index);
        assert(true == update);
        assert(peer_next_index == 
                replicate->peekNextIndexes().at(peer_id)); 
        assert(peer_next_index - 1ull == 
                replicate->peekMatchIndexes().at(peer_id));
        printf ( "%zu\n", replicate->peekNextBatchSizes().at(peer_id) );
        assert(size_t{2} == replicate->peekNextBatchSizes().at(peer_id));
    }

    // 2.
    {
        auto peer_id = 2ull;
        auto peer_next_index = 2ull;
        auto update = 
            replicate->UpdateReplicateState(
                    peer_id, false, 0ull, peer_next_index);
        assert(false == update);
        assert(size_t{2} == replicate->peekNextBatchSizes().at(peer_id));
    }

    // 3.
    {
        auto peer_id = 2ull;
        auto peer_next_index = 1ull;
        auto update = 
            replicate->UpdateReplicateState(
                    peer_id, false, 0ull, peer_next_index);
        assert(false == update);
        assert(size_t{2} == replicate->peekNextBatchSizes().at(peer_id));
    }

    // update all
    for (auto peer_id : config.GetReplicateGroup()) {
        auto peer_next_index = 3ull;
        auto update = replicate->UpdateReplicateState(
                peer_id, false, 0ull, peer_next_index);
        assert(true == update);
        assert(size_t{2} <= replicate->peekNextBatchSizes().at(peer_id));
    }

    // reset replicate
    replicate = config.CreateReplicateTracker(10ull, size_t{10});
    assert(nullptr != replicate);
    // 1.
    for (auto peer_id : config.GetReplicateGroup()) {
        if (peer_id == config.GetSelfId()) {
            continue;
        }

        auto peer_next_index = 0ull;
        auto update = 
            replicate->UpdateReplicateState(
                peer_id, true, 0ull, peer_next_index);
        assert(true == update);
        assert(0 == replicate->peekMatchIndexes().at(peer_id));
        assert(5ull == replicate->peekNextIndexes().at(peer_id));
        assert(size_t{0} == replicate->peekNextBatchSizes().at(peer_id));
    }
}


