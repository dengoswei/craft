#include "test_helper.h"
#include "raft_impl.h"
#include "raft.pb.h"


using namespace std;
using namespace raft;

namespace test {

uint64_t LOGID = 1ull;
std::set<uint64_t> GROUP_IDS{1ull, 2ull, 3ull};


std::vector<std::unique_ptr<raft::Message>>
    apply(std::map<uint64_t, std::unique_ptr<raft::RaftImpl>>& map_raft, 
          const std::vector<std::unique_ptr<raft::Message>>& vec_input_msg)
{
    vector<unique_ptr<Message>> vec_msg;
    for (const auto& msg : vec_input_msg) {
        assert(nullptr != msg);
        assert(map_raft.end() != map_raft.find(msg->to()));
        auto& raft = map_raft[msg->to()];
        assert(nullptr != raft);
        
        auto rsp_msg_type = raft->step(*msg);
        auto vec_rsp_msg = raft->produceRsp(*msg, rsp_msg_type);
        for (auto& rsp_msg : vec_rsp_msg) {
            assert(nullptr != rsp_msg);
            assert(msg->to() == rsp_msg->from());
            vec_msg.emplace_back(move(rsp_msg));
            assert(nullptr == rsp_msg);
        }
    }

    return vec_msg;
}

void
apply_until(
        std::map<uint64_t, std::unique_ptr<raft::RaftImpl>>& map_raft, 
        std::vector<std::unique_ptr<raft::Message>>&& vec_msg)
{
    while (false == vec_msg.empty()) {
        vec_msg = apply(map_raft, vec_msg);
    }
}

std::map<uint64_t, std::unique_ptr<raft::RaftImpl>>
    build_rafts(const std::set<uint64_t> group_ids, 
            uint64_t logid, int min_timeout, int max_timeout)
{
    assert(0 < min_timeout);
    assert(min_timeout <= max_timeout);
    map<uint64_t, unique_ptr<RaftImpl>> map_raft;
    for (auto id : group_ids) {
        auto election_timeout = random_int(min_timeout, max_timeout);
        assert(0 < election_timeout);

        auto raft = make_unique<RaftImpl>(
                logid, id, group_ids, election_timeout);
        assert(nullptr != raft);
        assert(RaftRole::FOLLOWER == raft->getRole());
        assert(map_raft.end() == map_raft.find(id));
        map_raft[id] = move(raft);
        assert(nullptr == raft);
    }

    return map_raft;
}

void init_leader(
        const uint64_t logid, 
        const uint64_t leader_id, 
        std::map<uint64_t, std::unique_ptr<raft::RaftImpl>>& map_raft)
{
    assert(map_raft.end() != map_raft.find(leader_id));
    // 1. timeout
    for (const auto& id_raft : map_raft) {
        assert(nullptr != id_raft.second);
        assert(logid == id_raft.second->getLogId());
        assert(RaftRole::FOLLOWER == id_raft.second->getRole());
        assert(0ull == id_raft.second->getTerm());
        
        auto tp = chrono::system_clock::now();
        if (leader_id == id_raft.second->getSelfId()) {
            // timeout leader_id
            int timeout = id_raft.second->getElectionTimout() + 1;
            tp = tp - chrono::milliseconds{timeout};
        }

        id_raft.second->updateActiveTime(tp);
    }

    vector<unique_ptr<Message>> vec_msg;
    {
        auto fake_msg = make_unique<Message>();
        fake_msg->set_logid(logid);
        fake_msg->set_type(MessageType::MsgNull);
        fake_msg->set_to(leader_id);
        vec_msg.emplace_back(move(fake_msg));
    }

    assert(size_t{1} == vec_msg.size());
    apply_until(map_raft, move(vec_msg));
    assert(true == vec_msg.empty());

    for (const auto& id_raft : map_raft) {
        assert(nullptr != id_raft.second);
        if (leader_id == id_raft.first) {
            assert(RaftRole::LEADER == id_raft.second->getRole());
        }
        else {
            assert(RaftRole::FOLLOWER == id_raft.second->getRole());
        }

        assert(1 == id_raft.second->getTerm());
    }
}



} // namespace test


