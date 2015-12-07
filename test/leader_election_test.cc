#include "gtest/gtest.h"
#include "raft_impl.h"
#include "utils.h"
#include "raft.pb.h"


using namespace raft;
using namespace std;


TEST(TestRaftLeaderElection, SimpleElectionSucc)
{
    const uint64_t logid = 1;
    std::vector<std::unique_ptr<RaftImpl>> vecRaft;
    // 1.
    for (auto id = 1ull; id <= 3ull; ++id) {
        // 10 ~ 20 ms
        auto election_timeout = random_int(10, 20);
        vecRaft.emplace_back(
                make_unique<RaftImpl>(logid, id, 3ull, election_timeout));
        assert(nullptr != vecRaft.back());

        assert(RaftRole::FOLLOWER == vecRaft.back()->getRole());
    }

    // 2.
    auto& test_raft = vecRaft[0];
    assert(nullptr != test_raft);
    assert(RaftRole::FOLLOWER == test_raft->getRole());
    assert(0ull == test_raft->getTerm());

    unique_ptr<Message> vote_msg = nullptr;
    {
        // pretent test_raft is timeout
        int timeout = test_raft->getElectionTimout() + 1;
        assert(0 < timeout);
        auto fake_tp = chrono::system_clock::now() - chrono::milliseconds{timeout};
        test_raft->updateActiveTime(fake_tp);

        Message fake_msg;
        fake_msg.set_logid(logid);
        fake_msg.set_type(MessageType::MsgNull);
        fake_msg.set_to(test_raft->getSelfId());

        auto rsp_msg_type = test_raft->step(fake_msg);
        assert(MessageType::MsgVote == rsp_msg_type);

        vote_msg = test_raft->produceRsp(fake_msg, rsp_msg_type);
        assert(nullptr != vote_msg);
        assert(rsp_msg_type == vote_msg->type());
    }

    assert(RaftRole::CANDIDATE == test_raft->getRole());
    assert(1ull == test_raft->getTerm());
    assert(nullptr != vote_msg);
    assert(0ull == vote_msg->to());

    for (auto i = 1; i < vecRaft.size(); ++i) {
        auto& peer_raft = vecRaft[i];
        assert(nullptr != peer_raft);
        assert(RaftRole::FOLLOWER == peer_raft->getRole());

        vote_msg->set_to(peer_raft->getSelfId());
        auto rsp_msg_type = peer_raft->step(*vote_msg);
        assert(MessageType::MsgVoteResp == rsp_msg_type);

        auto rsp_msg = peer_raft->produceRsp(*vote_msg, rsp_msg_type);
        assert(nullptr != rsp_msg);
        assert(rsp_msg_type == rsp_msg->type());
        assert(test_raft->getSelfId() == rsp_msg->to());
        assert(false == rsp_msg->reject());

        assert(MessageType::MsgNull == test_raft->step(*rsp_msg)); 
        assert(RaftRole::LEADER == test_raft->getRole());
    }
}


