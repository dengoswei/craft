#include "gtest/gtest.h"
#include "raft_impl.h"
#include "utils.h"
#include "raft.pb.h"
#include "test_helper.h"


using namespace std;
using namespace raft;
using namespace test;

void ClearPendingStoreSeq(
        std::map<uint64_t, std::unique_ptr<raft::RaftImpl>>& map_raft)
{
    for (auto& id_raft : map_raft) {
        auto& raft = id_raft.second;
        assert(nullptr != raft);

        uint64_t meta_seq = 0ull;
        uint64_t log_idx = 0ull;
        uint64_t log_seq = 0ull;
        tie(meta_seq, log_idx, log_seq) = raft->getStoreSeq();
    
        raft->commitedStoreSeq(meta_seq, log_idx, log_seq);

        tie(meta_seq, log_idx, log_seq) = raft->getStoreSeq();
        assert(0ull == meta_seq);
        assert(0ull == log_idx);
        assert(0ull == log_seq);
    }
} 


TEST(TestHardStateStore, Timeout)
{
    uint64_t logid = LOGID;
    set<uint64_t> group_ids = GROUP_IDS;

    auto map_raft = build_rafts(group_ids, logid, 10, 20);
    assert(map_raft.size() == group_ids.size());

    for (auto& id_raft : map_raft) {
        assert(nullptr != id_raft.second);
        assert(id_raft.first == id_raft.second->getSelfId());
        assert(RaftRole::FOLLOWER == id_raft.second->getRole());

        assert(0ull == id_raft.second->getTerm());
    }

    usleep(30 * 1000); // make sure timeout
    for (auto& id_raft : map_raft) {
        auto msg_null = buildMsgNull(
                id_raft.first, logid, id_raft.second->getTerm());
        assert(nullptr != msg_null);

        auto& raft = id_raft.second;
        auto rsp_msg_type = raft->step(*msg_null);
        assert(MessageType::MsgVote == rsp_msg_type);

        uint64_t meta_seq = 0ull;
        uint64_t log_idx = 0ull;
        uint64_t log_seq = 0ull;
        tie(meta_seq, log_idx, log_seq) = raft->getStoreSeq();
        assert(0ull < meta_seq);
        assert(0ull == log_idx);
        assert(0ull == log_seq);

        auto hs = raft->getCurrentHardState();
        assert(nullptr != hs);
        assert(0ull < hs->term());
        assert(hs->term() == raft->getTerm());
        assert(0ull != hs->vote());
        assert(hs->vote() == raft->getSelfId());
        assert(0ull == hs->commit());
        assert(hs->commit() == raft->getCommitedIndex());
    }

    for (auto& id_raft : map_raft) {
        auto& raft = id_raft.second;

        uint64_t meta_seq = 0ull;
        uint64_t log_idx = 0ull;
        uint64_t log_seq = 0ull;
        tie(meta_seq, log_idx, log_seq) = raft->getStoreSeq();
        assert(0ull < meta_seq);
        assert(0ull == log_idx);
        assert(0ull == log_seq);

        raft->commitedStoreSeq(meta_seq, 0ull, 0ull);

        meta_seq = 0ull;
        tie(meta_seq, log_idx, log_seq) = raft->getStoreSeq();
        assert(0ull == meta_seq);
        assert(0ull == log_idx);
        assert(0ull == log_seq);
    }
}

TEST(TestHardStateStore, AppendEntriesBestCase)
{
    uint64_t logid = 0ull;
    set<uint64_t> group_ids;
    map<uint64_t, unique_ptr<RaftImpl>> map_raft;

    uint64_t leader_id = 1ull;
    tie(logid, group_ids, map_raft) = comm_init(leader_id, 100, 200);

    auto& raft = map_raft[leader_id];
    assert(nullptr != raft);
    assert(RaftRole::LEADER == raft->getRole());

    for (auto& id_raft : map_raft) {
        uint64_t meta_seq = 0ull;
        uint64_t log_idx = 0ull;
        uint64_t log_seq = 0ull;

        tie(meta_seq, log_idx, log_seq) = id_raft.second->getStoreSeq();
        assert(0ull < meta_seq);
        assert(0ull == log_idx);
        assert(0ull == log_seq);

        id_raft.second->commitedStoreSeq(meta_seq, log_idx, log_seq);

        meta_seq = 0ull;
        tie(meta_seq, log_idx, log_seq) = id_raft.second->getStoreSeq();
        assert(0ull == meta_seq);
        assert(0ull == log_idx);
        assert(0ull == log_seq);
    }

    for (int i = 0; i < 10; ++i) {
        uint64_t prev_index = raft->getLastLogIndex();

        auto vec_msg = batchBuildMsgProp(
                logid, leader_id, raft->getTerm(), 
                raft->getLastLogIndex(), 1, 2);
        assert(vec_msg.size() == size_t{1});

        apply_until(map_raft, move(vec_msg));

        for (auto& id_raft : map_raft) {
            uint64_t meta_seq = 0ull;
            uint64_t log_idx = 0ull;
            uint64_t log_seq = 0ull;

            tie(meta_seq, log_idx, log_seq) = id_raft.second->getStoreSeq();
            assert(0ull == meta_seq);
            hassert(log_idx == prev_index + 1ull, 
                    "log_idx %" PRIu64 " prev_index %" PRIu64, 
                    log_idx, prev_index);
            assert(0ull < log_seq);

            id_raft.second->commitedStoreSeq(
                    meta_seq, log_idx, log_seq);
            log_idx = 0ull;
            log_seq = 0ull;
            tie(meta_seq, log_idx, log_seq) = id_raft.second->getStoreSeq();
            assert(0ull == meta_seq);
            assert(0ull == log_idx);
            assert(0ull == log_seq);
        }
    }
}

TEST(TestHardStateStore, WhatIfFailedToCommited)
{
    uint64_t logid = 0ull;
    set<uint64_t> group_ids;
    map<uint64_t, unique_ptr<RaftImpl>> map_raft;

    uint64_t leader_id = 1ull;
    tie(logid, group_ids, map_raft) = comm_init(leader_id, 100, 200);

    auto& raft = map_raft[leader_id];
    assert(nullptr != raft);
    assert(RaftRole::LEADER == raft->getRole());

    // 1. clear
    ClearPendingStoreSeq(map_raft);

    // 2. build pending state
    uint64_t prev_index = raft->getLastLogIndex();
    for (int i = 0; i < 2; ++i) {
        auto vec_msg = batchBuildMsgProp(
                logid, leader_id, raft->getTerm(), 
                raft->getLastLogIndex(), 1, 2);
        assert(vec_msg.size() == size_t{1});

        apply_until(map_raft, move(vec_msg));
    }

    // 3. check pending state
    {
        for (auto& id_raft : map_raft) {
            uint64_t meta_seq = 0ull;
            uint64_t log_idx = 0ull;
            uint64_t log_seq = 0ull;

            tie(meta_seq, log_idx, log_seq) = id_raft.second->getStoreSeq();
            assert(0ull == meta_seq);
            assert(prev_index + 1ull == log_idx);
            assert(0ull < log_seq);

            id_raft.second->commitedStoreSeq(meta_seq, log_idx, log_seq);
            meta_seq = 0ull;
            log_idx = 0ull;
            log_seq = 0ull;

            tie(meta_seq, log_idx, log_seq) = id_raft.second->getStoreSeq();
            assert(0ull == meta_seq);
            assert(0ull == log_idx);
            assert(0ull == log_seq);
        }
    }
}

