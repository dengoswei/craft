#pragma once

#include <random>
#include <cstdio>
#include <cassert>
#include <inttypes.h>

#define hassert(cond, fmt, ...)                     \
{                                                   \
    bool bCond = cond;                              \
    if (!bCond)                                     \
    {                                               \
        printf ( fmt "\n", ##__VA_ARGS__ );         \
    }                                               \
    assert(bCond);                                  \
}

namespace {

void log_nothing(const char* /* format */, ...) 
    __attribute__((format(printf, 1, 2)));

void log_nothing(const char* /* format */, ...) {

}

} // namespace

#ifndef TEST_DEBUG

#define logdebug(format, ...) log_nothing(format, ##__VA_ARGS__)
#define logerr(format, ...) log_nothing(format, ##__VA_ARGS__)

#else

#define logdebug(format, ...) \
    printf("[DEBUG: %s %s %d] " format "\n", __FILE__, __func__, __LINE__, ##__VA_ARGS__)

#define logerr(format, ...) \
    printf("[ERROR: %s %s %d] " format "\n", __FILE__, __func__, __LINE__, ##__VA_ARGS__)

#endif


namespace raft {


inline int random_int(int min, int max)
{
    // mark as thread local ?
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(min, max);
    return dis(gen);
}



} // namespace raft


