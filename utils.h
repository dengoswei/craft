#pragma once

#include <stdint.h>
#include <map>
#include <set>
#include <cassert>

namespace raft {

template <typename T>
bool countMajor(
        T expected, 
        const std::map<uint64_t, T>& votes, 
        size_t group_size)
{
    auto major_count = size_t{0};
    for (const auto& v : votes) {
        if (v.second >= expected) {
            ++ major_count;
        }
    }

    return major_count >= (group_size / 2 + 1);
}

template <typename T>
bool countMajor(
        T expected, 
        const std::map<uint64_t, T>& votes, 
        const std::set<uint64_t>& peer_ids)
{
    auto major_count = size_t{0};    
    for (auto id : peer_ids) {
        if (votes.end() == votes.find(id)) {
            continue;
        }

        assert(votes.end() != votes.find(id));
        if (votes.at(id) >= expected) {
            ++major_count;
        }
    }

    return major_count >= (peer_ids.size() / 2 + 1);
}

} // namespace raft


