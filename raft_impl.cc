#include "raft_impl.h"



namespace raft {


RaftImpl::RaftImpl(
        uint64_t logid, uint64_t selfid, uint64_t group_size)
    : logid_(logid)
    , selfid_(selfid)
    , group_size_(group_size)
    , active_time_(chrono::system_clock::now())
{

}

std::tuple<ErrorCode, uint64_t, MessageType>
RaftImpl::evaluateRole(const Message& msg)
{
    // raft paper: rules for servers: 5.1
    // => If RPC request or response contains term T > currentTerm: 
    //    set currentTerm = T, convert to follower;
}


void RaftImpl::setRole(RaftRole new_role)
{
    logdebug("change role_ %d new_role %d", 
            static_cast<int>(role_), static_cast<int>(new_role));
    role_ = new_role;
    return ;
}

void RaftImpl::setTerm(uint64_t new_term)
{
    logdebug("change current term %" PRIu64 " new_term %" PRIu64, 
            term_, new_term);
    term_ = new_term;
    return ;
}



} // namespace raft


