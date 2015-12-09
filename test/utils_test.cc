


#include <unistd.h>
#include <iostream>
#include <vector>
#include <cstdio>
#include <cstdlib>
#include "utils.h"
#include "gsl.h"


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
        cout << random_int(100, 200) << endl;
        usleep(10 * 1000);
    }

    vector<int> vec(10, 0);
    gsl::array_view<int> vec_view{&vec[0], vec.size()};

    return EXIT_SUCCESS;
}				/* ----------  end of function main  ---------- */
