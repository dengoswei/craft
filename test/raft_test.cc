#include "gtest/gtest.h"
#include "raft.h"
#include "raft.pb.h"
#include "test_helper.h"
#include "hassert.h"
#include "mem_utils.h"


using namespace raft;
using namespace std;
using namespace test;

void AssertCheckConfState(
        const raft::ConfState& conf_state, 
        const raft::ConfState& expected_conf_state)
{
    assert(conf_state.nodes_size() == expected_conf_state.nodes_size());

    set<uint64_t> group_ids;
    set<uint64_t> expected_group_ids;
    for (int i = 0; i < conf_state.nodes_size(); ++i) {
        group_ids.insert(conf_state.nodes(i));
        expected_group_ids.insert(expected_conf_state.nodes(i));
    }

    assert(group_ids == expected_group_ids);
}



TEST(RaftTest, EmptySnapshotMeta)
{
    SnapshotMetadata meta;
    meta.set_logid(LOGID);
    meta.set_commited_index(0);
    {
        auto conf_state = meta.mutable_conf_state();
        assert(nullptr != conf_state);
        for (auto id : GROUP_IDS) {
            conf_state->add_nodes(id);
        }
        assert(true == meta.has_conf_state());
    }

    auto selfid = 1ull;
    RaftCallBack callback;
    callback.read = 
        [](uint64_t logid, uint64_t log_index) 
            -> std::tuple<int, std::unique_ptr<raft::Entry>> {

            return make_tuple(1, nullptr);
        };

    callback.write = 
        [](std::unique_ptr<raft::RaftState> raft_state, 
           std::vector<std::unique_ptr<raft::Entry>> entries) 
            -> int {
            return 0;
        };
    
    callback.send = 
        [](std::vector<std::unique_ptr<raft::Message>> messages) 
            -> int {
            return 0;
        };

    {
        auto raft = cutils::make_unique<Raft>(
                selfid, 10, 20, meta, nullptr, callback);
        assert(nullptr != raft);
        assert(0 == raft->GetMaxIndex());
        assert(0 == raft->GetCommitedIndex());
        assert(0 == raft->GetTerm());
        assert(0 == raft->GetVoteFor());
        assert(selfid == raft->GetSelfId());
        assert(meta.logid() == raft->GetLogId());
    }

    {
        RaftState raft_state;
        raft_state.set_term(3);
        raft_state.set_vote(1);
        raft_state.set_seq(10);

        auto raft = cutils::make_unique<Raft>(
                selfid, 10, 20, meta, &raft_state, callback);
        assert(nullptr != raft);
        assert(0 == raft->GetMaxIndex());
        assert(0 == raft->GetCommitedIndex());
        assert(raft_state.term() == raft->GetTerm());
        assert(raft_state.vote() == raft->GetVoteFor());
    }
}

TEST(RaftTest, SnapshotMetadataConstruct)
{
    ConfState conf_state;
    for (auto id : GROUP_IDS) {
        conf_state.add_nodes(id);
    }

    RaftCallBack callback;
    callback.write = 
        [](std::unique_ptr<raft::RaftState> raft_state, 
           std::vector<std::unique_ptr<raft::Entry>> entries) 
            -> int {
            return 0;
        };
    
    callback.send = 
        [](std::vector<std::unique_ptr<raft::Message>> messages) 
            -> int {
            return 0;
        };

    auto test_index = 10ull;
    auto test_term = 5ull;
    auto selfid = 1ull;
    auto simple_read_cb = 
        [=](uint64_t logid, uint64_t log_index) 
            -> std::tuple<int, std::unique_ptr<raft::Entry>> {

            if (log_index > test_index) {
                return make_tuple(1, nullptr);
            }

            auto entry = cutils::make_unique<Entry>();
            assert(nullptr != entry);
            entry->set_type(raft::EntryType::EntryNormal);
            entry->set_term(test_term);
            entry->set_index(log_index);
            entry->set_seq(1);
            return make_tuple(0, move(entry));
        };

    // case 1
    {
        SnapshotMetadata meta;
        meta.set_logid(LOGID);
        assert(nullptr != meta.mutable_conf_state());
        *(meta.mutable_conf_state()) = conf_state;

        meta.set_commited_index(test_index);
        callback.read = simple_read_cb;

        auto new_term = test_term + 10;
        raft::RaftState raft_state;
        raft_state.set_term(test_term + 10);
        raft_state.set_vote(2);
        raft_state.set_seq(10);
        raft_state.set_commit(test_index);

        auto raft = cutils::make_unique<Raft>(
                selfid, 10, 20, meta, &raft_state, callback);
        assert(nullptr != raft);
        assert(test_index == raft->GetMaxIndex());
        assert(test_index == raft->GetCommitedIndex());
        assert(raft_state.term() == raft->GetTerm());
        assert(raft_state.vote() == raft->GetVoteFor());

        auto new_meta = raft->CreateSnapshotMetadata();
        assert(nullptr != new_meta);

        assert(true == new_meta->has_conf_state());
        AssertCheckConfState(new_meta->conf_state(), conf_state);
        assert(LOGID == new_meta->logid());
        assert(test_index == new_meta->commited_index());
    }

    // case 2
    callback.read = nullptr;
    {
        SnapshotMetadata meta;
        meta.set_logid(LOGID);
        assert(nullptr != meta.mutable_conf_state());
        *(meta.mutable_conf_state()) = conf_state;

        meta.set_commited_index(0);
        callback.read = simple_read_cb;

        RaftState raft_state;
        raft_state.set_term(test_term);
        raft_state.set_vote(0);
        raft_state.set_seq(20);
        raft_state.set_commit(test_index / 2);

        auto raft = cutils::make_unique<Raft>(
                selfid, 10, 20, meta, &raft_state, callback);
        assert(nullptr != raft);
        assert(test_index == raft->GetMaxIndex());
        assert(raft_state.commit() == raft->GetCommitedIndex());
        assert(raft_state.term() == raft->GetTerm());
        assert(test_term == raft->GetTerm());
        assert(raft_state.vote() == raft->GetVoteFor());

        auto new_meta = raft->CreateSnapshotMetadata();
        assert(nullptr != new_meta);

        assert(true == new_meta->has_conf_state());
        AssertCheckConfState(new_meta->conf_state(), conf_state);
        assert(LOGID == new_meta->logid());
        assert(raft_state.commit() == new_meta->commited_index());
    }
}







