#include <set>
#include "gtest/gtest.h"
#include "raft_impl.h"
#include "utils.h"
#include "raft.pb.h"
#include "test_helper.h"

using namespace raft;
using namespace std;
using namespace test;


TEST(TestRaftLeaderElection, SimpleElectionSucc)
{
    const auto logid = test::LOGID;
    const auto& group_ids = test::GROUP_IDS;
    auto map_raft = build_rafts(group_ids, logid, 10, 20);
    assert(map_raft.size() == group_ids.size());

    // 2.
    auto& test_raft = map_raft[1ull];
    assert(nullptr != test_raft);
    assert(RaftRole::FOLLOWER == test_raft->getRole());
    assert(0ull == test_raft->getTerm());

    // pretent test_raft is timeout
    int timeout = test_raft->getElectionTimout() + 1;
    hassert(0 < timeout, "timeout %d", timeout);
    auto fake_tp = 
        chrono::system_clock::now() - chrono::milliseconds{timeout};
    test_raft->updateActiveTime(fake_tp);

    // to trigger timeout
    // TODO: add MsgElection: to start a election
    auto fake_msg = make_unique<Message>();
    assert(nullptr != fake_msg);
    fake_msg->set_logid(logid);
    fake_msg->set_type(MessageType::MsgNull);
    fake_msg->set_to(test_raft->getSelfId());
    fake_msg->set_term(test_raft->getTerm());

    // 1. timeout: rsp MsgVote
    auto vec_rsp_msg = vector<unique_ptr<Message>>{};
    vec_rsp_msg.push_back(move(fake_msg));
    assert(nullptr == fake_msg);
    assert(size_t{1} == vec_rsp_msg.size());

    vec_rsp_msg = apply(map_raft, vec_rsp_msg);
    assert(size_t{2} == vec_rsp_msg.size());
    assert(RaftRole::CANDIDATE == test_raft->getRole());
    assert(1ull == test_raft->getTerm());
    for (auto& rsp_msg : vec_rsp_msg) {
        assert(nullptr != rsp_msg);
        assert(MessageType::MsgVote == rsp_msg->type());
    }

    // 2. MsgVoteResp
    vec_rsp_msg = apply(map_raft, vec_rsp_msg);
    assert(size_t{2} == vec_rsp_msg.size());
    assert(RaftRole::CANDIDATE == test_raft->getRole());
    for (auto& rsp_msg : vec_rsp_msg) {
        assert(nullptr != rsp_msg);
        assert(1ull == rsp_msg->to());
        assert(MessageType::MsgVoteResp == rsp_msg->type());
        assert(false == rsp_msg->reject());
    }
        
    // 3. become leader & send MsgHeartbeat
    vec_rsp_msg = apply(map_raft, vec_rsp_msg);
    assert(size_t{2} == vec_rsp_msg.size());
    assert(RaftRole::LEADER == test_raft->getRole());
    for (auto& rsp_msg : vec_rsp_msg) {
        assert(nullptr != rsp_msg);
        assert(MessageType::MsgHeartbeat == rsp_msg->type());
    }

    // 4. MsgHeartbeatResp
    vec_rsp_msg = apply(map_raft, vec_rsp_msg);
    assert(size_t{2} == vec_rsp_msg.size());
    for (auto& rsp_msg : vec_rsp_msg) {
        assert(nullptr != rsp_msg);
        assert(MessageType::MsgHeartbeatResp == rsp_msg->type());
    }

    // 5. nothing
    vec_rsp_msg = apply(map_raft, vec_rsp_msg);
    assert(true == vec_rsp_msg.empty());
    assert(RaftRole::LEADER == test_raft->getRole());
    for (auto id : group_ids) {
        if (id == test_raft->getSelfId()) {
            continue;
        }

        auto& raft = map_raft[id];
        assert(nullptr != raft);
        assert(RaftRole::FOLLOWER == raft->getRole());
    }
}


