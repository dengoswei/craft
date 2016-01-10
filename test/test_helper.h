#pragma once

#include <map>
#include <set>
#include <deque>
#include <vector>
#include <memory>
#include <mutex>
#include <string>
#include <stdint.h>
#include "raft.pb.h"


namespace raft {

class RaftImpl;
class Raft;
class RaftConfig;

struct RaftCallBack;

} // namespace raft

namespace test {

extern uint64_t LOGID;
extern std::set<uint64_t> GROUP_IDS;

class StorageHelper;
class SendHelper;

// for raft_impl
std::vector<std::unique_ptr<raft::Message>>
apply(
        std::map<uint64_t, std::unique_ptr<raft::RaftImpl>>& map_raft, 
        const std::vector<std::unique_ptr<raft::Message>>& vec_input_msg);


void apply_until(
        std::map<uint64_t, std::unique_ptr<raft::RaftImpl>>& map_raft, 
        std::vector<std::unique_ptr<raft::Message>>&& vec_msg);


std::map<uint64_t, std::unique_ptr<raft::RaftImpl>>
    build_rafts(
            uint64_t logid, 
            const std::set<uint64_t>& group_ids, 
            int min_timeout, int max_timeout);

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
// end of raft_impl


// raft
std::unique_ptr<raft::Raft>
build_raft(
        uint64_t logid,
        uint64_t selfid, 
        const std::set<uint64_t>& group_ids, 
        int min_election_timeout, int max_election_timeout, 
        raft::RaftCallBack callback);

std::tuple<
    std::map<uint64_t, std::unique_ptr<StorageHelper>>, 
    std::map<uint64_t, std::unique_ptr<raft::Raft>>>
build_rafts(
        uint64_t logid, 
        const std::set<uint64_t>& group_ids, 
        SendHelper& sender, 
        int min_election_timeout, int max_election_timeout);

std::tuple<
    uint64_t, 
    std::set<uint64_t>, 
    std::map<uint64_t, std::unique_ptr<StorageHelper>>, 
    std::map<uint64_t, std::unique_ptr<raft::Raft>>>
comm_init(
        uint64_t leader_id, 
        SendHelper& sender, 
        int min_election_timeout, int max_election_timeout);


// end of raft

class StorageHelper {

public:
    ~StorageHelper();

    int write(
            std::unique_ptr<raft::HardState>&& hs, 
            std::vector<std::unique_ptr<raft::Entry>>&& vec_entries);

    std::unique_ptr<raft::Entry> read(uint64_t log_index);

private:
    int write(std::unique_ptr<raft::HardState>&& hs);

    int write(std::vector<std::unique_ptr<raft::Entry>>&& vec_entries);

private:
    std::mutex mutex_;
    uint64_t max_meta_seq_;
    std::unique_ptr<raft::HardState> meta_info_;

    uint64_t max_log_seq_;
    std::map<uint64_t, std::unique_ptr<raft::Entry>> log_entries_;
};


class SendHelper {

public:
    ~SendHelper();

    void send(std::unique_ptr<raft::Message>&& msg);

    size_t apply(
            std::map<uint64_t, std::unique_ptr<raft::Raft>>& map_raft);

    void apply_until(
            std::map<uint64_t, std::unique_ptr<raft::Raft>>& map_raft);

    bool empty();

private:
    std::mutex msg_queue_mutex_;
    std::deque<std::unique_ptr<raft::Message>> msg_queue_;
};

std::unique_ptr<raft::Message> buildMsgProp(
        uint64_t logid, uint64_t leader_id, 
        uint64_t term, uint64_t prev_index, int entries_size);


std::vector<std::unique_ptr<raft::Message>>
batchBuildMsgProp(
        uint64_t logid, uint64_t leader_id, 
        uint64_t term, uint64_t prev_index, 
        int batch_size, int entries_size);

        
std::unique_ptr<raft::Message>
    buildMsgNull(uint64_t to_id, uint64_t logid, uint64_t term);

std::unique_ptr<raft::Message>
    buildMsgPropConf(
            uint64_t logid, uint64_t leader_id, 
            uint64_t term, uint64_t prev_index, 
            const raft::ConfChange& conf_change);

int applyConfChange(
        raft::RaftConfig& config, 
        raft::ConfChangeType change_type, 
        uint64_t node_id, bool check_pending);

int addNode(raft::RaftConfig& config, 
        uint64_t node_id, bool check_pending);

int addCatchUpNode(
        raft::RaftConfig& config, uint64_t node_id);

int removeNode(
        raft::RaftConfig& config, uint64_t node_id, bool check_pending);

int removeCatchUpNode(
        raft::RaftConfig& config, uint64_t node_id);

raft::RaftConfig buildTestConfig();



} // namespace test
