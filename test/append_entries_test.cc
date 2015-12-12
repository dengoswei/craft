#include "gtest/gtest.h"
#include "raft_impl.h"
#include "utils.h"
#include "raft.pb.h"
#include "test_helper.h"


using namespace raft;
using namespace std;
using namespace test;

std::vector<std::string> extract_value(
        const std::vector<std::unique_ptr<raft::Message>>& vec_msg)
{
    vector<string> vec_value;
    for (const auto& msg : vec_msg) {
        assert(nullptr != msg);
        for (auto idx = 0; idx < msg->entries_size(); ++idx) {
            auto value = msg->entries(idx).data();
            vec_value.emplace_back(move(value));
        }
    }

    return vec_value;
}

//std::unique_ptr<raft::Message> buildMsgProp(
//        uint64_t logid, uint64_t leader_id, 
//        uint64_t term, uint64_t prev_index, int entries_size)
//{ 
//    assert(0ull < leader_id);
//    assert(0ull < term);
//    assert(0ull <= prev_index);
//    assert(0 < entries_size);
//    auto prop_msg = make_unique<Message>();
//    assert(nullptr != prop_msg);
//
//    prop_msg->set_logid(logid);
//    prop_msg->set_type(MessageType::MsgProp);
//    prop_msg->set_to(leader_id);
//    prop_msg->set_term(term);
//    prop_msg->set_index(prev_index);
//
//    RandomStrGen<100, 200> str_gen;
//    for (auto i = 0; i < entries_size; ++i) {
//        auto entry = prop_msg->add_entries();
//        assert(nullptr != entry);
//
//        entry->set_type(EntryType::EntryNormal);
//        entry->set_data(str_gen.Next());
//    }
//
//    return prop_msg;
//}


TEST(TestRaftAppendEntries, SimpleAppendTest)
{
    uint64_t logid = 0ull;
    set<uint64_t> group_ids;
    map<uint64_t, unique_ptr<raft::RaftImpl>> map_raft;

    uint64_t leader_id = 1ull;
    tie(logid, group_ids, map_raft) = comm_init(leader_id, 10, 20);

    auto& raft = map_raft[leader_id];
    assert(nullptr != raft);
    assert(RaftRole::LEADER == raft->getRole());

    string value;
    vector<unique_ptr<Message>> vec_msg;
    {
        auto prop_msg = buildMsgProp(
                logid, leader_id, raft->getTerm(), 
                raft->getLastLogIndex(), 1);
        assert(nullptr != prop_msg);
        assert(1 == prop_msg->entries_size()); 
        value = prop_msg->entries(0).data();
        assert(false == value.empty());
        vec_msg.emplace_back(move(prop_msg));
    }
    assert(size_t{1} == vec_msg.size());

    // 1. leader accept MsgProp => produce MsgApp to up-to-date peers
    vec_msg = apply(map_raft, vec_msg);
    assert(size_t{2} == vec_msg.size());
    for (auto& rsp_msg : vec_msg) {
        assert(nullptr != rsp_msg);
        assert(MessageType::MsgApp == rsp_msg->type());
        assert(raft->getTerm() == rsp_msg->term());
        assert(0ull == rsp_msg->index());
        assert(0ull == rsp_msg->log_term());
        assert(0ull == rsp_msg->commit());
        assert(1 == rsp_msg->entries_size());
        
        auto entry = rsp_msg->entries(0);
        assert(EntryType::EntryNormal == entry.type());
        assert(1ull == entry.index());
        assert(raft->getTerm() == entry.term());
        assert(entry.data() == value);
    }

    // 2. followers accept MsgApp => produce MsgAppResp(false == reject)
    vec_msg = apply(map_raft, vec_msg);
    assert(size_t{2} == vec_msg.size());
    for (auto& rsp_msg : vec_msg) {
        assert(nullptr != rsp_msg);
        assert(MessageType::MsgAppResp == rsp_msg->type());
        assert(raft->getTerm() == rsp_msg->term());
        assert(2ull == rsp_msg->index());
        assert(false == rsp_msg->reject());
        assert(raft->getSelfId() == rsp_msg->to());
    }

    // 3. leader collect MsgAppResp, update commited_index_
    //    => send back empty MsgApp to followers notify the commited_index_
    vec_msg = apply(map_raft, vec_msg);
    assert(size_t{2} == vec_msg.size());
    for (auto& rsp_msg : vec_msg) {
        assert(nullptr != rsp_msg);
        assert(MessageType::MsgApp == rsp_msg->type());
        assert(raft->getTerm() == rsp_msg->term());
        assert(0ull == rsp_msg->index());
        assert(0ull == rsp_msg->log_term());
        assert(1ull == rsp_msg->commit());
        assert(0 == rsp_msg->entries_size());
    }

    // 4. followers recv empty MsgApp, update commited_index_
    //    => & rsp nothing ?
    vec_msg = apply(map_raft, vec_msg);
    for (const auto& id_raft : map_raft) {
        assert(nullptr != id_raft.second);
        assert(1ull == id_raft.second->getCommitedIndex());
    }

    assert(true == vec_msg.empty());
}

TEST(TestRaftAppendEntries, RepeatAppendTest)
{
    uint64_t logid = 0ull;
    set<uint64_t> group_ids;
    map<uint64_t, unique_ptr<raft::RaftImpl>> map_raft;

    uint64_t leader_id = 1ull;
    tie(logid, group_ids, map_raft) = comm_init(leader_id, 10, 20);

    auto& raft = map_raft[leader_id];
    assert(nullptr != raft);
    assert(RaftRole::LEADER == raft->getRole());

    // repeat test count 100
    RandomStrGen<100, 200> str_gen;
    const auto fix_term = raft->getTerm();
    for (int i = 0; i < 100; ++i) {
        vector<unique_ptr<Message>> vec_msg;
        string value;
        {
            auto prop_msg = buildMsgProp(
                    logid, leader_id, raft->getTerm(), 
                    raft->getLastLogIndex(), 1);
            assert(nullptr != prop_msg);
            value = prop_msg->entries(0).data();
            vec_msg.emplace_back(move(prop_msg));
        }
        assert(size_t{1} == vec_msg.size());

        apply_until(map_raft, move(vec_msg));
        assert(fix_term == raft->getTerm());
        const auto index = 1ull + i;
        for (const auto& id_raft : map_raft) {
            hassert(index == id_raft.second->getCommitedIndex(), 
                    "id_raft.first(id) %" PRIu64 
                    " index %" PRIu64 " commited_index %" PRIu64, 
                    id_raft.first, index, 
                    id_raft.second->getCommitedIndex());
            const auto entry = id_raft.second->getLogEntry(index);
            assert(nullptr != entry);
            assert(EntryType::EntryNormal == entry->type());
            hassert(raft->getTerm() == entry->term(), 
                    "raft->getTerm() %" PRIu64 
                    " entry->term() %" PRIu64 " entry->index() %" PRIu64, 
                    raft->getTerm(), entry->term(), entry->index());
            assert(index == entry->index());
            assert(value == entry->data());
        }
    }
}


TEST(TestRaftAppendEntries, RepeatBatchAppendTest)
{
    uint64_t logid = 0ull;
    set<uint64_t> group_ids;
    map<uint64_t, unique_ptr<raft::RaftImpl>> map_raft;

    uint64_t leader_id = 1ull;
    tie(logid, group_ids, map_raft) = comm_init(leader_id, 10, 20);

    auto& raft = map_raft[leader_id];
    assert(nullptr != raft);
    assert(RaftRole::LEADER == raft->getRole());


    RandomStrGen<100, 200> str_gen;
    const auto fix_term = raft->getTerm();
    for (int i = 0; i < 10; ++i) {
        auto vec_msg = batchBuildMsgProp(
                logid, leader_id, raft->getTerm(), 
                raft->getLastLogIndex(), 1 + i, 1 + i); 
        assert(vec_msg.size() == static_cast<size_t>(1 + i));

        auto vec_value = extract_value(vec_msg);
        assert(vec_value.size() == static_cast<size_t>((1 + i) * (1 + i)));
        uint64_t prev_index = raft->getLastLogIndex();
        apply_until(map_raft, move(vec_msg));
        assert(fix_term == raft->getTerm());
        assert(raft->getLastLogIndex() == prev_index + vec_value.size());
        assert(raft->getCommitedIndex() == raft->getLastLogIndex());
    }
}



