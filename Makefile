
CPPFLAGS += -isystem $(GTEST_DIR)/include -std=c++14 -stdlib=libc++
CXXFLAGS += -g -Wall -Wextra # -D TEST_DEBUG

TESTS = utils_test

INCLS += -I./craftpb/
INCLS += -I$(HOME)/open-src/github.com/microsoft/GSL/include
INCLS += -I$(HOME)/project/include
LINKS += -L$(HOME)/project/lib
LINKS += -lpthread -lprotobuf

AR = ar -rc
CPPCOMPILE = $(CXX) $(CPPFLAGS) $(CXXFLAGS) $< $(INCLS) -c -o $@
BUILDEXE = $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $^ $(LINKS)
ARSTATICLIB = $(AR) $@ $^ $(AR_FLAGS)

PROTOS_PATH = craftpb
PROTOC = $(HOME)/project/bin/protoc
GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`

all: $(TESTS)

clean :
	rm -f $(TESTS) *.o craftpb/*.o craftpb/raft.pb.* test/*.o libcraft.a

libcraft.a: raft.o raft_impl.o replicate_tracker.o craftpb/raft.pb.o 
	$(ARSTATICLIB)

%.pb.cc: craftpb/%.proto
	$(PROTOC) -I $(PROTOS_PATH) --cpp_out=craftpb/ $<

%.o:%.cc
	$(CPPCOMPILE)

#.cc.o:
#	$(CPPCOMPILE)

