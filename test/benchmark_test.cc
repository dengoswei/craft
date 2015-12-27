#include <benchmark/benchmark_api.h>
#include <string>
#include "test_helper.h"
#include "raft.h"
#include "raft.pb.h"

static void BM_InitLeader(benchmark::State& state) {
    while (state.KeepRunning()) {
        test::SendHelper sender;
        test::comm_init(1ull, sender, 50, 60);
    }
}

BENCHMARK(BM_InitLeader);

BENCHMARK_MAIN()

