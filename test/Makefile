
CPPFLAGS += -isystem $(GTEST_DIR)/include -std=c++14 -stdlib=libc++
CXXFLAGS += -g -Wall -Wextra -D TEST_DEBUG



TESTS = utils_test leader_election_test \
		append_entries_test store_test benchmark_test \
		config_test replicate_test raft_test

INCLS += -I../ -I../craftpb 
INCLS += -I../../cutils
#INCLS += -I$(HOME)/open-src/github.com/microsoft/GSL/include
INCLS += -I$(HOME)/project/include
LINKS += -L$(HOME)/project/lib
LINKS += -lpthread -lprotobuf

GTEST_HEADERS = $(GTEST_DIR)/include/gtest/*.h \
                $(GTEST_DIR)/include/gtest/internal/*.h

CPPCOMPILE = $(CXX) $(CPPFLAGS) $(CXXFLAGS) $< $(INCLS) -c -o $@
BUILDEXE = $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $^ $(LINKS)

PROTOS_PATH = ../craftpb
PROTOC = $(HOME)/project/bin/protoc
GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`

all: $(TESTS)

clean :
	rm -f $(TESTS) gtest.a gtest_main.a *.o ../*.o ../craftpb/*.o 

# Builds gtest.a and gtest_main.a.

# Usually you shouldn't tweak such internal variables, indicated by a
# trailing _.
GTEST_SRCS_ = $(GTEST_DIR)/src/*.cc $(GTEST_DIR)/src/*.h $(GTEST_HEADERS)

# For simplicity and to avoid depending on Google Test's
# implementation details, the dependencies specified below are
# conservative and not optimized.  This is fine as Google Test
# compiles fast and for ordinary users its source rarely changes.
gtest-all.o : $(GTEST_SRCS_)
	$(CXX) $(CPPFLAGS) -I$(GTEST_DIR) $(CXXFLAGS) -c \
            $(GTEST_DIR)/src/gtest-all.cc

gtest_main.o : $(GTEST_SRCS_)
	$(CXX) $(CPPFLAGS) -I$(GTEST_DIR) $(CXXFLAGS) -c \
            $(GTEST_DIR)/src/gtest_main.cc

gtest.a : gtest-all.o
	$(AR) $(ARFLAGS) $@ $^

gtest_main.a : gtest-all.o gtest_main.o
	$(AR) $(ARFLAGS) $@ $^

utils_test: utils_test.o
	$(BUILDEXE)

leader_election_test: leader_election_test.o test_helper.o\
	../raft.o ../raft_impl.o ../replicate_tracker.o ../raft_config.o \
	../craftpb/raft.pb.o gtest_main.a
	$(BUILDEXE)

append_entries_test: append_entries_test.o test_helper.o\
	../raft.o ../raft_impl.o ../replicate_tracker.o ../raft_config.o \
	../craftpb/raft.pb.o gtest_main.a
	$(BUILDEXE)

config_test: config_test.o ../raft_impl.o ../replicate_tracker.o \
	test_helper.o ../raft.o ../replicate_tracker.o \
	../raft_config.o ../craftpb/raft.pb.o gtest_main.a
	$(BUILDEXE)

replicate_test: replicate_test.o ../raft_impl.o ../replicate_tracker.o \
	test_helper.o ../raft.o ../replicate_tracker.o \
	../raft_config.o ../craftpb/raft.pb.o gtest_main.a
	$(BUILDEXE)


store_test: store_test.o test_helper.o ../libcraft.a gtest_main.a
	$(BUILDEXE)

benchmark_test: LINKS += -L/usr/local/lib
benchmark_test: LINKS += -lbenchmark -lcraft
benchmark_test: INCLS += -I/usr/local/include
benchmark_test: benchmark_test.o test_helper.o
	$(BUILDEXE)

raft_test: raft_test.o test_helper.o ../libcraft.a gtest_main.a
	$(BUILDEXE)

%.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --cpp_out=../craftpb/ $<


%.o:%.cc
	$(CPPCOMPILE)

#.cc.o:
#	$(CPPCOMPILE)

