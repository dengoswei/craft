#include <unistd.h>
#include <set>
#include "gtest/gtest.h"
#include "raft_impl.h"
#include "utils.h"
#include "raft.pb.h"
#include "test_helper.h"

using namespace raft;
using namespace std;
using namespace test;


// leaderid, term
std::tuple<uint64_t, uint64_t>
CheckLeader(
        const std::map<uint64_t, std::unique_ptr<raft::RaftImpl>>& map_raft)
{
    assert(false == map_raft.empty());
    
    uint64_t leader_id = 0ull;
    uint64_t term = 0ull;
    for (const auto& id_raft : map_raft) {
        const auto& raft = id_raft.second;
        assert(nullptr != raft);
        term = 0ull == term ? raft->getTerm() : term;
        assert(raft->getTerm() == term);
        if (RaftRole::LEADER == raft->getRole()) {
            assert(0ull == leader_id);
            leader_id = raft->getSelfId();
        }
    }

    assert(0ull < term);
    assert(0ull < leader_id);
    return make_tuple(leader_id, term);
}


TEST(TestRaftLeaderElection, SimpleElectionSucc)
{
    uint64_t logid = test::LOGID;
    const auto& group_ids = test::GROUP_IDS;
    
    auto map_raft = build_rafts(group_ids, logid, 10, 20);
    assert(map_raft.size() == group_ids.size());

    uint64_t leader_id = 1ull;
    assert(map_raft.end() != map_raft.find(leader_id));
    
     // 2.
    auto& test_raft = map_raft[leader_id];
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

TEST(TestRaftLeaderElection, HeartbeatKeepAlive)
{
    uint64_t logid = 0ull;
    set<uint64_t> group_ids;
    map<uint64_t, unique_ptr<RaftImpl>> map_raft;

    uint64_t leader_id = 1ull;
    tie(logid, group_ids, map_raft) = comm_init(leader_id, 15, 20);

    auto& raft = map_raft[leader_id];
    assert(nullptr != raft);
    assert(RaftRole::LEADER == raft->getRole());

    const int max_hb_timeout = 11;
    uint64_t term = raft->getTerm();
    for (int i = 0; i < 20; ++i) {
        vector<unique_ptr<Message>> vec_msg;
        vec_msg.emplace_back(
                buildMsgNull(leader_id, logid, term));
        assert(false == vec_msg.empty());

        usleep(max_hb_timeout * 1000);

        logdebug("INFO begin next round %d", i);
        apply_until(map_raft, move(vec_msg));
        assert(true == vec_msg.empty());

        uint64_t curr_leader_id = 0ull;
        uint64_t curr_term = 0ull;
        tie(curr_leader_id, curr_term) = CheckLeader(map_raft);;
        assert(0ull < curr_leader_id);
        assert(0ull < curr_term);
        assert(curr_leader_id == leader_id);
        assert(term == curr_term);
    }
}

TEST(TestRaftLeaderElection, EmptyStateRepeatElectionSucc)
{
    uint64_t logid = 0ull;
    set<uint64_t> group_ids;
    map<uint64_t, unique_ptr<RaftImpl>> map_raft;

    {
        uint64_t leader_id = 1ull;
        tie(logid, group_ids, map_raft) = comm_init(leader_id, 10, 20);

        auto& raft = map_raft[leader_id];
        assert(nullptr != raft);
        assert(RaftRole::LEADER == raft->getRole());
    }

    // repeat test
    const int max_timeout = 20 + 2;
    for (int i = 0; i < 20; ++i) {
        uint64_t prev_leader_id = 0ull;
        uint64_t prev_term = 0ull;
        tie(prev_leader_id, prev_term) = CheckLeader(map_raft);
        assert(0ull < prev_leader_id);
        assert(0ull < prev_term);

        uint64_t peer_id = prev_leader_id;
        while (peer_id == prev_leader_id) {
            peer_id = static_cast<uint64_t>(random_int(1, 3));
            assert(0ull < peer_id);
        }

        vector<unique_ptr<Message>> vec_msg;
        vec_msg.emplace_back(buildMsgNull(peer_id, logid, prev_term));
        assert(false == vec_msg.empty());

        usleep(max_timeout * 1000); // sleep ms to timeout

        logdebug("INFO begin next round %d", i);
        apply_until(map_raft, move(vec_msg));
        assert(true == vec_msg.empty());

        // check leader
        uint64_t curr_leader_id = 0ull;
        uint64_t curr_term = 0ull;
        tie(curr_leader_id, curr_term) = CheckLeader(map_raft);
        assert(0ull < curr_leader_id);
        assert(0ull < curr_term);

        assert(prev_term < curr_term);
    }
}



