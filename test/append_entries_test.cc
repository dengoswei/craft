#include "gtest/gtest.h"
#include "raft_impl.h"
#include "utils.h"
#include "raft.pb.h"
#include "test_helper.h"


using namespace raft;
using namespace std;
using namespace test;


TEST(TestRaftAppendEntries, SimpleAppendTest)
{
    const auto logid = test::LOGID;
    const auto& group_ids = test::GROUP_IDS;
    auto map_raft = build_rafts(group_ids, logid, 10, 20);
    assert(map_raft.size() == group_ids.size());

    const auto leader_id = 1ull;
    assert(map_raft.end() != map_raft.find(leader_id));

    init_leader(logid, leader_id, map_raft);

    auto& raft = map_raft[leader_id];
    assert(nullptr != raft);
    assert(RaftRole::LEADER == raft->getRole());

    string value;
    vector<unique_ptr<Message>> vec_msg;
    {
        auto prop_msg = make_unique<Message>();
        assert(nullptr != prop_msg);
        prop_msg->set_logid(logid);
        prop_msg->set_type(MessageType::MsgProp);
        prop_msg->set_to(leader_id);
        prop_msg->set_term(raft->getTerm());
        // index: 0ull

        auto entry = prop_msg->add_entries();
        assert(nullptr != entry);

        RandomStrGen<100, 200> str_gen;
        value = str_gen.Next();
        entry->set_type(EntryType::EntryNormal);
        entry->set_data(value);

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
    // TODO
}




