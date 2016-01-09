#include "gtest/gtest.h"
#include "raft_config.h"
#include "raft.pb.h"
#include "test_helper.h"


using namespace std;
using namespace raft;
using namespace test;


TEST(TestRaftConfig, SimpleConstruct)
{
    uint64_t selfid = 1ull;
    RaftConfig config(selfid);
    
    assert(selfid == config.GetSelfId());

    assert(false == config.IsAGroupMember(selfid));
    assert(false == config.IsAReplicateMember(selfid));
    assert(false == config.IsPending());

    assert(true == config.GetReplicateGroup().empty());
}

TEST(TestRaftConfig, PendingCheck)
{
    uint64_t selfid = 1ull;
    RaftConfig config(selfid);

    ConfChange conf_change;
    conf_change.set_type(ConfChangeType::ConfChangeAddNode);
    conf_change.set_node_id(selfid);

    assert(false == config.IsPending());
    assert(0 == config.ApplyConfChange(conf_change, true));
    assert(true == config.IsPending());
    assert(true == config.IsAGroupMember(conf_change.node_id()));
    assert(true == config.IsAReplicateMember(conf_change.node_id()));

    conf_change.set_node_id(2ull);
    assert(0 != config.ApplyConfChange(conf_change, true));
    assert(true == config.IsPending());
    assert(false == config.IsAGroupMember(conf_change.node_id()));
    assert(false == config.IsAReplicateMember(conf_change.node_id()));

    // try to clear pending
    conf_change.set_type(ConfChangeType::ConfChangeAddCatchUpNode);
    config.CommitConfChange(conf_change);
    assert(true == config.IsPending());

    // clear pending
    conf_change.set_type(ConfChangeType::ConfChangeAddNode);
    config.CommitConfChange(conf_change);
    assert(false == config.IsPending());

    // add next 
    assert(0 == config.ApplyConfChange(conf_change, false));
    assert(false == config.IsPending());
    assert(true == config.IsAGroupMember(conf_change.node_id()));
    assert(true == config.IsAReplicateMember(conf_change.node_id()));
}

TEST(TestRaftConfig, SimpleAddRemove)
{
    uint64_t selfid = 1ull;
    RaftConfig config(selfid);

    auto vec_ids = {1ull, 2ull, 3ull};
    for (auto id : vec_ids) {
        assert(0 == addNode(config, id, false));
        assert(true == config.IsAGroupMember(id));
        assert(true == config.IsAReplicateMember(id));
    }
    assert(false == config.IsPending());

    for (auto id : vec_ids) {
        assert(0 == removeNode(config, id, false));
        assert(false == config.IsAGroupMember(id));
        assert(false == config.IsAReplicateMember(id));
    }
    assert(false == config.IsPending());

    for (auto id : vec_ids) {
        assert(0 == addCatchUpNode(config, id));
        assert(false == config.IsAGroupMember(id));
        assert(true == config.IsAReplicateMember(id));
    }
    assert(false == config.IsPending());

    for (auto id : vec_ids) {
        assert(0 == removeCatchUpNode(config, id));
        assert(false == config.IsAGroupMember(id));
        assert(false == config.IsAReplicateMember(id));
    }
    assert(false == config.IsPending());
}

TEST(TestRaftConfig, MajorVote)
{
    auto config = buildTestConfig();
    assert(false == config.IsMajorVoteYes({}));
    assert(false == config.IsMajorVoteYes({
                    {1ull, true}, 
                    {2ull, false}, 
                }));

    assert(false == config.IsMajorVoteYes({
                    {1ull, true}, 
                    {2ull, false}, 
                    {3ull, false}
                }));

    assert(true == config.IsMajorVoteYes({
                    {1ull, true}, 
                    {2ull, true}
                }));

    assert(true == config.IsMajorVoteYes({
                    {1ull, true}, 
                    {2ull, false}, 
                    {3ull, true}
                }));

    assert(false == config.IsMajorVoteYes({
                    {1ull, true}, 
                    {4ull, true}, 
                    {5ull, true}
                }));

    assert(true == config.IsMajorVoteYes({
                    {1ull, true}, 
                    {2ull, true}, 
                    {4ull, false}, 
                    {5ull, false}, 
                    {6ull, false}
                }));
}

TEST(TestRaftConfig, MajorCommited)
{
    auto config = buildTestConfig();
    assert(false == config.IsMajorCommited(0ull, {}));

    assert(false == config.IsMajorCommited(1ull, {
                    {1ull, 1ull}, 
                }));
    assert(false == config.IsMajorCommited(1ull, {
                    {1ull, 1ull}, 
                    {2ull, 0ull},
                    {3ull, 0ull}
                }));
    assert(false == config.IsMajorCommited(1ull, {
                    {1ull, 1ull}, 
                    {4ull, 1ull}, 
                    {5ull, 1ull}
                }));
    assert(true == config.IsMajorCommited(1ull, {
                    {1ull, 1ull}, 
                    {2ull, 1ull}
                }));

    assert(true == config.IsMajorCommited(1ull, {
                    {1ull, 1ull},
                    {2ull, 0ull}, 
                    {3ull, 1ull}
                }));
}


TEST(TestRaftConfig, BuildBroadcastMsg)
{
    auto config = buildTestConfig();

    const uint64_t test_term = 2ull;
    const uint64_t test_logid = 2ull;
    const MessageType test_type = MessageType::MsgApp;
    Message msg_template;
    msg_template.set_from(config.GetSelfId());
    msg_template.set_type(test_type);
    msg_template.set_term(test_term);
    msg_template.set_logid(test_logid);

    {
        auto vec_msg = config.BroadcastGroupMsg(msg_template);

        set<uint64_t> uids;
        for (const auto& msg : vec_msg) {
            assert(nullptr != msg); 
            assert(0ull < msg->to());
            assert(test_term == msg->term());
            assert(test_type == msg->type());
            assert(test_logid == msg->logid());
            
            assert(uids.end() == uids.find(msg->to()));
            uids.insert(msg->to());
        }

        assert(uids.end() == uids.find(config.GetSelfId()));
        const auto& group = config.GetGroup();
        if (group.end() == group.find(config.GetSelfId())) {
            assert(uids == group);
            assert(uids == config.GetReplicateGroup());
        }
        else {
            uids.insert(config.GetSelfId());
            assert(uids == group);
            assert(uids == config.GetReplicateGroup());
        }
    }


    {
        assert(0 == addCatchUpNode(config, 4ull));
        assert(0 == addCatchUpNode(config, 5ull));
    }

    {
        assert(config.GetGroup().size() < 
                config.GetReplicateGroup().size());
        auto vec_msg = config.BroadcastReplicateGroupMsg(msg_template);

        set<uint64_t> uids;
        for (const auto& msg : vec_msg) {
            assert(nullptr != msg);
            assert(0ull < msg->to());
            assert(test_term == msg->term());
            assert(test_type == msg->type());
            assert(test_logid == msg->logid());

            assert(uids.end() == uids.find(msg->to()));
            uids.insert(msg->to());
        }

        assert(uids.end() == uids.find(config.GetSelfId()));
        const auto& replicate_group = config.GetReplicateGroup();
        if (replicate_group.end() == 
                replicate_group.find(config.GetSelfId())) {
            assert(uids == replicate_group);
        }
        else {
            uids.insert(config.GetSelfId());
            assert(uids == replicate_group);
        }
    }
}


