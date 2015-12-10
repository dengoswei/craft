#pragma once

#include <map>
#include <vector>
#include <memory>


namespace raft {

class Message;
class RaftImpl;

} // namespace raft

namespace test {

std::vector<std::unique_ptr<raft::Message>>
apply(
        std::map<uint64_t, std::unique_ptr<raft::RaftImpl>>& map_raft, 
        const std::vector<std::unique_ptr<raft::Message>>& vec_input_msg);


void apply_until(
        std::map<uint64_t, std::unique_ptr<raft::RaftImpl>>& map_raft, 
        std::vector<std::unique_ptr<raft::Message>>&& vec_msg);

} // namespace test
