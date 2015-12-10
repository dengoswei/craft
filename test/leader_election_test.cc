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
    const uint64_t logid = 1;
    const set<uint64_t> ids{1ull, 2ull, 3ull};
    map<uint64_t, unique_ptr<RaftImpl>> map_raft;

    for (auto id : ids) {
        auto election_timeout = random_int(10, 20);
        assert(0 < election_timeout);
        auto raft = make_unique<RaftImpl>(
                logid, id, ids, election_timeout);
        assert(nullptr != raft);
        assert(RaftRole::FOLLOWER == raft->getRole());
        assert(map_raft.end() == map_raft.find(id));
        map_raft[id] = move(raft);
        assert(nullptr == raft);
    }

    // 2.
    auto& test_raft = map_raft[1ull];
    assert(nullptr != test_raft);
    assert(RaftRole::FOLLOWER == test_raft->getRole());
    assert(0ull == test_raft->getTerm());

    {
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
        for (auto id : ids) {
            if (id == test_raft->getSelfId()) {
                continue;
            }

            auto& raft = map_raft[id];
            assert(nullptr != raft);
            assert(RaftRole::FOLLOWER == raft->getRole());
        }
    }

//
//    assert(RaftRole::CANDIDATE == test_raft->getRole());
//    assert(1ull == test_raft->getTerm());
//    for (auto i = size_t{1}; i < vecRaft.size(); ++i) {
//        auto& peer_raft = vecRaft[i];
//        assert(nullptr != peer_raft);
//        assert(RaftRole::FOLLOWER == peer_raft->getRole());
//
//        auto& vote_msg = map_send_msg[peer_raft->getSelfId()];
//        assert(nullptr != vote_msg);
//        assert(peer_raft->getSelfId() == vote_msg->to());
//
//        auto rsp_msg_type = peer_raft->step(*vote_msg);
//        assert(MessageType::MsgVoteResp == rsp_msg_type);
//
//        auto vec_rsp_msg = peer_raft->produceRsp(*vote_msg, rsp_msg_type);
//        assert(size_t{1} == vec_rsp_msg.size());
//
//        auto& rsp_msg = vec_rsp_msg[0];
//        assert(nullptr != rsp_msg);
//        assert(rsp_msg_type == rsp_msg->type());
//        assert(test_raft->getSelfId() == rsp_msg->to());
//        assert(false == rsp_msg->reject());
//
//        rsp_msg_type = test_raft->step(*rsp_msg);
//        assert(RaftRole::LEADER == test_raft->getRole());
//        assert(MessageType::MsgHeartbeat == rsp_msg_type || 
//                MessageType::MsgNull == rsp_msg_type);
//    }
}


