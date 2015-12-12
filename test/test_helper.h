#pragma once

#include <map>
#include <set>
#include <vector>
#include <memory>
#include <stdint.h>


namespace raft {

class Message;
class RaftImpl;

} // namespace raft

namespace test {

extern uint64_t LOGID;
extern std::set<uint64_t> GROUP_IDS;


std::vector<std::unique_ptr<raft::Message>>
apply(
        std::map<uint64_t, std::unique_ptr<raft::RaftImpl>>& map_raft, 
        const std::vector<std::unique_ptr<raft::Message>>& vec_input_msg);


void apply_until(
        std::map<uint64_t, std::unique_ptr<raft::RaftImpl>>& map_raft, 
        std::vector<std::unique_ptr<raft::Message>>&& vec_msg);


std::map<uint64_t, std::unique_ptr<raft::RaftImpl>>
    build_rafts(const std::set<uint64_t> group_ids, 
            uint64_t logid, int min_timeout, int max_timeout);

void init_leader(
        uint64_t logid, 
        uint64_t leader_id, 
        std::map<uint64_t, std::unique_ptr<raft::RaftImpl>>& map_raft);


std::tuple<
    uint64_t,
    std::set<uint64_t>, 
    std::map<uint64_t, std::unique_ptr<raft::RaftImpl>>>
comm_init(uint64_t leader_id, 
        int min_election_timeout, int max_election_timeout);


std::unique_ptr<raft::Message> buildMsgProp(
        uint64_t logid, uint64_t leader_id, 
        uint64_t term, uint64_t prev_index, int entries_size);


std::vector<std::unique_ptr<raft::Message>>
batchBuildMsgProp(
        uint64_t logid, uint64_t leader_id, 
        uint64_t term, uint64_t prev_index, 
        int batch_size, int entries_size);


} // namespace test
