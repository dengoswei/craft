#include "test_helper.h"
#include "raft_impl.h"
#include "raft.pb.h"


using namespace std;
using namespace raft;

namespace test {


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

} // namespace test


