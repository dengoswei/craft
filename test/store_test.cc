#include "gtest/gtest.h"
#include "raft_impl.h"
#include "utils.h"
#include "raft.pb.h"
#include "test_helper.h"


using namespace std;
using namespace raft;
using namespace test;


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
        tie(meta_seq, log_idx, log_seq) = raft->getStoreSeq(0ull);
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
        tie(meta_seq, log_idx, log_seq) = raft->getStoreSeq(0ull);
        assert(0ull < meta_seq);
        assert(0ull == log_idx);
        assert(0ull == log_seq);

        raft->commitedStoreSeq(meta_seq, 0ull, 0ull);

        meta_seq = 0ull;
        tie(meta_seq, log_idx, log_seq) = raft->getStoreSeq(0ull);
        assert(0ull == meta_seq);
        assert(0ull == log_idx);
        assert(0ull == log_seq);
    }
}



