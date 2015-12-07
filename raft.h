#pragma once

namespace raft {

struct RaftCallBack {

};


class Raft {

public:
    Raft(uint64_t logid, uint64_t selfid, uint64_t group_size, 
            RaftCallBack callback);

    ~Raft();

    raft::ErrorCode Step(const Message& msg);

    std::tuple<raft::ErrorCode, uint64_t, std::unique_ptr<std::string>> 
        Get(uint64_t index);

    std::tuple<raft::ErrorCode, uint64_t> 
        TrySet(uint64_t index, gsl::cstring_view<> value);


    void Wait(uint64_t index);
    bool WaitFor(uint64_t index, const std::chrono::milliseconds timeout);

private:
    std::mutex raft_mutex_;
    std::condition_variable raft_cv_;
    std::unique_ptr<RaftImpl> raft_impl_;

    RaftCallBack callback_;
}; 

} // namespace raft


