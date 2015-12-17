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


inline std::string 
format_time(std::chrono::time_point<std::chrono::system_clock> tp)
{
    auto ttp = std::chrono::system_clock::to_time_t(tp);
    
    std::string str(26, '\0');
    ctime_r(&ttp, &str[0]);
    auto str_len = strlen(str.data());
    assert(0 < str_len);
    // remove the added new line
    str[str_len-1] = '\0';
    str.resize(str_len);
    return str;
}


inline int random_int(int min, int max)
{
    // mark as thread local ?
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(min, max);
    return dis(gen);
}

template <typename RNGType,
         typename INTType,
         INTType iMin=0, INTType iMax=std::numeric_limits<INTType>::max()>
class RandomIntGen
{
public:
    RandomIntGen()
        : m_tUDist(iMin, iMax)
    {
        m_tMyRNG.seed(time(NULL));
    }

    INTType Next()
    {
        return m_tUDist(m_tMyRNG);
    }

private:
    RNGType m_tMyRNG;
    std::uniform_int_distribution<INTType> m_tUDist;
};

typedef RandomIntGen<std::mt19937_64, uint64_t> Random64BitGen;
typedef RandomIntGen<std::mt19937, uint32_t> Random32BitGen;

static const char DICTIONARY[] =
    "0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz";


template <int iMin, int iMax>
class RandomStrGen
{
public:
    std::string Next()
    {
        auto iLen = m_tRLen.Next();
        std::string s;
        s.resize(iLen);
        for (auto i = 0; i < iLen; ++i)
        {
            auto j = m_tRIdx.Next();
            s[i] = DICTIONARY[j];
            assert(s[i] != '\0');
        }
        return s;
    }

private:
    RandomIntGen<std::mt19937, int, iMin, iMax> m_tRLen;
    RandomIntGen<std::mt19937, int, 0, sizeof(DICTIONARY)-2> m_tRIdx;
};



} // namespace raft


