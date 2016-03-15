#include "gtest/gtest.h"
#include "raft_impl.h"
#include "raft.h"
#include "utils.h"
#include "raft.pb.h"
#include "test_helper.h"
#include "log.h"
#include "hassert.h"


using namespace raft;
using namespace std;
using namespace test;
using namespace cutils;

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

void updateAllActiveTime(
        std::map<uint64_t, std::unique_ptr<raft::RaftImpl>>& map_raft)
{
    auto tp = chrono::system_clock::now();
    for (auto& id_raft : map_raft) {
        auto& raft = id_raft.second;
        assert(nullptr != raft);
        raft->updateActiveTime(tp);
    }
}

void updateAllActiveTime(
        std::map<uint64_t, std::unique_ptr<raft::Raft>>& map_raft)
{
    auto tp = chrono::system_clock::now();
    for (auto& id_raft : map_raft) {
        auto& raft = id_raft.second;
        assert(nullptr != raft);
        assert(id_raft.first == raft->GetSelfId());
        raft->ReflashTimer(tp);
    }
}

std::vector<std::string> generatePropValue(int entries_size)
{
    assert(0 < entries_size);
    RandomStrGen<100, 200> str_gen;

    vector<string> vec_value;
    vec_value.reserve(entries_size);
    for (auto i = 0; i < entries_size; ++i) {
        vec_value.emplace_back(str_gen.Next());
    }

    return vec_value;
}


void checkValue(
        uint64_t base_index, 
        const std::vector<std::string>& vec_value, 
        StorageHelper& store)
{
    auto log_index = base_index;
    for (auto& value : vec_value) {
        auto entry = store.read(log_index);
        hassert(nullptr != entry, "log_index %" PRIu64, log_index);

//        logdebug("index %" PRIu64 " term %" PRIu64 
//                " seq %" PRIu64 " data %s", 
//                entry->index(), entry->term(), entry->seq(), 
//                entry->data().c_str());
        assert(entry->type() == EntryType::EntryNormal);
        assert(entry->index() == log_index);
        assert(0ull < entry->term());
        assert(0ull < entry->seq());
        assert(entry->data() == value);

        ++log_index;
    }
}

void checkValue(
        uint64_t base_index, 
        const std::vector<std::string>& vec_value, 
        std::map<uint64_t, std::unique_ptr<StorageHelper>>& map_store)
{
    for (auto& id_store : map_store) {
        assert(nullptr != id_store.second);
        checkValue(base_index, vec_value, *(id_store.second));
    }
}

TEST(TestRaftAppendEntriesImpl, SimpleAppendTest)
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
        assert(raft->getLastLogIndex() == rsp_msg->index());
        assert(raft->getTerm() == rsp_msg->log_term());
        assert(1ull == rsp_msg->commit());
        assert(0 == rsp_msg->entries_size());
    }

    // 4. followers recv empty MsgApp, update commited_index_
    //    => & feed back rsp
    vec_msg = apply(map_raft, vec_msg);
    for (const auto& id_raft : map_raft) {
        assert(nullptr != id_raft.second);
        assert(1ull == id_raft.second->getCommitedIndex());
    }

    // 5. leader collect empty rsp, produce nothing
    assert(size_t{2} == vec_msg.size());
    vec_msg = apply(map_raft, vec_msg);
    assert(true == vec_msg.empty());
}

TEST(TestRaftAppendEntriesImpl, RepeatAppendTest)
{
    uint64_t logid = 0ull;
    set<uint64_t> group_ids;
    map<uint64_t, unique_ptr<raft::RaftImpl>> map_raft;

    uint64_t leader_id = 1ull;
    tie(logid, group_ids, map_raft) = comm_init(leader_id, 30, 40);

    auto& raft = map_raft[leader_id];
    assert(nullptr != raft);
    assert(RaftRole::LEADER == raft->getRole());

    // repeat test count 100
    const auto fix_term = raft->getTerm();

    for (int i = 0; i < 100; ++i) {
        
        raft->assertNoPending();
        logdebug("BEGIN round i %d", i);
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

        updateAllActiveTime(map_raft);
        auto prev_tp = chrono::system_clock::now();
        apply_until(map_raft, move(vec_msg));
        auto now_tp = chrono::system_clock::now();
        auto duration = 
            chrono::duration_cast<chrono::milliseconds>(now_tp - prev_tp);
        hassert(fix_term == raft->getTerm(), 
                "i %d fix_term %" PRIu64 " term %" PRIu64 " cost time %d", 
                i, fix_term, raft->getTerm(), duration.count());

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

TEST(TestRaftAppendEntriesImpl, AppendAndAddNode)
{
    uint64_t logid = 0ull;
    set<uint64_t> group_ids;
    map<uint64_t, unique_ptr<raft::RaftImpl>> map_raft;

    uint64_t leader_id = 1ull;
    tie(logid, group_ids, map_raft) = comm_init(leader_id, 30, 40);

    auto& raft = map_raft[leader_id];
    assert(nullptr != raft);
    assert(RaftRole::LEADER == raft->getRole());

    // append test
    for (int i = 0; i < 2; ++i) {
        raft->assertNoPending();
        vector<unique_ptr<Message>> vec_msg;
        {
            auto prop_msg = buildMsgProp(
                    logid, leader_id, raft->getTerm(), 
                    raft->getLastLogIndex(), 1);
            assert(nullptr != prop_msg);
            vec_msg.emplace_back(move(prop_msg));
        }

        updateAllActiveTime(map_raft);
        apply_until(map_raft, move(vec_msg));
    }

    // add new raft:
    uint64_t new_peer_id = 4ull;
    assert(group_ids.end() == group_ids.find(new_peer_id));
    {
        {
            auto new_raft = make_unique<RaftImpl>(
                    logid, new_peer_id, group_ids, 30, 40);
            assert(nullptr != new_raft);
            assert(map_raft.end() == map_raft.find(new_peer_id));
            map_raft[new_peer_id] = move(new_raft);
            assert(nullptr == new_raft);
        }

        vector<unique_ptr<Message>> vec_msg;
        {
            ConfChange conf_change;
            conf_change.set_type(ConfChangeType::ConfChangeAddCatchUpNode);
            conf_change.set_node_id(new_peer_id);
            auto prop_msg = buildMsgPropConf(
                    logid, leader_id, raft->getTerm(), 
                    raft->getLastLogIndex(), conf_change);
            assert(nullptr != prop_msg);
            vec_msg.emplace_back(move(prop_msg));

            updateAllActiveTime(map_raft);
            apply_until(map_raft, move(vec_msg));
        }

        auto term = raft->getTerm();
        auto last_log_index = raft->getLastLogIndex();
        for (const auto& id_raft : map_raft) {
            assert(nullptr != id_raft.second);
            auto& config = raft->GetCurrentConfig();
            assert(true == config.IsAReplicateMember(new_peer_id));
            assert(false == config.IsAGroupMember(new_peer_id));
            assert(term == id_raft.second->getTerm());
            hassert(last_log_index == id_raft.second->getCommitedIndex(), 
                    "%" PRIu64 "  %" PRIu64, 
                    last_log_index, 
                    id_raft.second->getCommitedIndex());
        }

        auto& new_raft = map_raft[new_peer_id];
        assert(nullptr != new_raft);
        for (auto log_index = 1ull; 
                log_index < last_log_index + 1ull; ++log_index) {
            auto expected_entry = raft->getLogEntry(log_index);
            auto entry = new_raft->getLogEntry(log_index);
            assert(nullptr != expected_entry);
            assert(nullptr != entry);

            assert(expected_entry->type() == entry->type());
            assert(expected_entry->term() == entry->term());
            assert(expected_entry->index() == entry->index());
            assert(expected_entry->data() == entry->data());
        }
    }
}


TEST(TestRaftAppendEntries, RepeatBatchAppendTest)
{
    uint64_t logid = 0ull;
    set<uint64_t> group_ids;
    map<uint64_t, unique_ptr<raft::RaftImpl>> map_raft;

    uint64_t leader_id = 1ull;
    tie(logid, group_ids, map_raft) = comm_init(leader_id, 30, 40);

    auto& raft = map_raft[leader_id];
    assert(nullptr != raft);
    assert(RaftRole::LEADER == raft->getRole());

    const auto fix_term = raft->getTerm();
    for (int i = 0; i < 10; ++i) {
        auto vec_msg = batchBuildMsgProp(
                logid, leader_id, raft->getTerm(), 
                raft->getLastLogIndex(), 1 + i, min(1 + i, 10)); 
        assert(vec_msg.size() == static_cast<size_t>(1 + i));

        auto vec_value = extract_value(vec_msg);
        assert(vec_value.size() == static_cast<size_t>((1 + i) * (1 + i)));
        uint64_t prev_index = raft->getLastLogIndex();
        apply_until(map_raft, move(vec_msg));

        hassert(fix_term == raft->getTerm(), 
                "i %d fix_term %" PRIu64 " term %" PRIu64, 
                i, fix_term, raft->getTerm());
        assert(raft->getLastLogIndex() == prev_index + vec_value.size());
        assert(raft->getCommitedIndex() == raft->getLastLogIndex());
    }
}


TEST(TestRaftAppendEntries, SimpleAppendTest)
{
    SendHelper sender;
    uint64_t logid = 0ull;
    set<uint64_t> group_ids;
    map<uint64_t, unique_ptr<StorageHelper>> map_store;
    map<uint64_t, unique_ptr<Raft>> map_raft;
    uint64_t leader_id = 1ull;

    tie(logid, group_ids, map_store, map_raft) = 
        comm_init(leader_id, sender, 50, 100);

    auto& raft = map_raft[leader_id];
    assert(nullptr != raft);
    assert(true == raft->IsLeader());

    auto vec_value = generatePropValue(1);
    // auto vec_view = buildPropValueView(vec_value);

    // 1.
    raft::ErrorCode err_code = raft::ErrorCode::OK;
    uint64_t assigned_log_index = 0ull;
    tie(err_code, assigned_log_index) = raft->Propose(0ull, vec_value);
    assert(ErrorCode::OK == err_code);
    assert(1ull == assigned_log_index);

    assert(false == sender.empty());
    // 2. followers accept MsgApp
    auto apply_count = sender.apply(map_raft);
    assert(size_t{2} == apply_count);

    assert(false == sender.empty());
    // 3.  leader collect MsgAppResp
    //     => send back empty MsgApp to notify commited_index_
    apply_count = sender.apply(map_raft);
    assert(size_t{2} == apply_count);

    assert(false == sender.empty());
    // 4. followers recv empty MsgApp => update commited_index_
    apply_count = sender.apply(map_raft);
    assert(size_t{2} == apply_count);

    assert(false == sender.empty());

    // 5. leader reply nothing
    apply_count = sender.apply(map_raft);
    assert(size_t{2} == apply_count);
    assert(true == sender.empty());

    // check
    checkValue(1ull, vec_value, map_store);
}

TEST(TestRaftAppendEntries, RepeatAppendTest)
{
    SendHelper sender;
    uint64_t logid = 0ull;
    set<uint64_t> group_ids;
    map<uint64_t, unique_ptr<StorageHelper>> map_store;
    map<uint64_t, unique_ptr<Raft>> map_raft;
    uint64_t leader_id = 1ull;

    tie(logid, group_ids, map_store, map_raft) = 
        comm_init(leader_id, sender, 50, 100);

    auto& raft = map_raft[leader_id];
    assert(nullptr != raft);
    assert(true == raft->IsLeader());

    for (int i = 0; i < 100; ++i) {
        auto vec_value = generatePropValue(min(i + 1, 10));
        // auto vec_view = buildPropValueView(vec_value);

        updateAllActiveTime(map_raft);

        raft::ErrorCode err_code = raft::ErrorCode::OK;
        uint64_t assigned_log_index = 0ull;
        tie(err_code, assigned_log_index) = raft->Propose(0ull, vec_value);
        assert(ErrorCode::OK == err_code);
        assert(0ull < assigned_log_index);
        logdebug("i %d err_code %d vec_value.size %zu "
                "assigned_log_index %" PRIu64, 
                i, static_cast<int>(err_code), vec_value.size(), 
                assigned_log_index);

        assert(false == sender.empty());
        sender.apply_until(map_raft);
        assert(true == sender.empty());
 
        // check
        checkValue(assigned_log_index, vec_value, map_store);
    }
}

