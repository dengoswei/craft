


#include <unistd.h>
#include <chrono>
#include <iostream>
#include <vector>
#include <cstdio>
#include <cstdlib>
#include "utils.h"
#include "random_utils.h"
#include "time_utils.h"

using namespace std;
using namespace raft;


class TestImpl {

private:
    using Handler = std::function<int(TestImpl&)>;

    Handler handle_ = nullptr;
};


    int
main ( int argc, char *argv[] )
{
    TestImpl t;
    for (int i = 0; i < 10; ++i) {
        cout << cutils::random_int(100, 200) << endl;
        usleep(10 * 1000);
    }

    vector<int> vec(10, 0);

    auto time_now = chrono::system_clock::now();
    auto time_str = cutils::format_time(time_now);
    printf ( "time_now %s\n", time_str.c_str() );

    return EXIT_SUCCESS;
}				/* ----------  end of function main  ---------- */
