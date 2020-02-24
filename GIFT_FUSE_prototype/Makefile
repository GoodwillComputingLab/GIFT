CXX=g++
CC=gcc

CLP_INCLUDE_DIR ?= $$HOME/sws/include/coin/
CLP_LIB_DIR ?= $$HOME/sws/lib64/

OSC_SRCS=log.cpp osc_ops.cpp osc.cpp util.cpp lnet.cpp lsocket.cpp dnet_osc.cpp epoll.cpp lnet_osc.cpp osc_main.cpp
OSC_TEST_SRCS=log.cpp osc_ops.cpp osc.cpp util.cpp lnet.cpp lsocket.cpp dnet_osc.cpp epoll.cpp lnet_osc.cpp osc-test.cpp
OSC_OBJS=log.o osc_ops.o osc.o util.o lnet.o osc.o lsocket.o dnet_osc.o epoll.o lnet_osc.o osc_main.o

MDS_SRCS=util.cpp lnet.cpp mds.cpp lsocket.cpp epoll.cpp mds_ops.cpp lnet_mds.cpp schemes.cpp mds_main.cpp
MDS_OBJS=util.o lnet.o mds.o lsocket.o epoll.o mds_ops.o lnet_mds.o schemes.o mds_main.o

OST_SRCS=util.cpp lnet.cpp ost.cpp lsocket.cpp dnet_ost.cpp epoll.cpp lnet_ost.cpp ost_ops.cpp ost_main.cpp
OST_OBJS=util.o lnet.o ost.o lsocket.o dnet_ost.o epoll.o lnet_ost.o ost_ops.o ost_main.o

CFLAGS=-Wall -Werror -I. -I${CLP_INCLUDE_DIR} -g3 -O0 -c -D_FILE_OFFSET_BITS=64
LDFLAGS=-lfuse -lpthread

CLP_LDFLAGS=-L${CLP_LIB_DIR} -lClp -lClpSolver -lCoinUtils -lOsi -lOsiClp -lz

default: osc ost mds

osc: $(OSC_OBJS)
	$(CXX) -o $@ $^ $(LDFLAGS)

test: osc-test ost mds

check: test
	@./osc-test && (echo "All tests passed. Cleaning up log files...";  rm -f *.log)

osc-test: $(OSC_TEST_SRCS)
	$(CXX) -I. -g3 -O0 -D_FILE_OFFSET_BITS=64 -DTEST_ENV -o $@ $^

ost: $(OST_OBJS)
	$(CXX) -o $@ $^ $(LDFLAGS)

mds: $(MDS_OBJS)
	$(CXX) -o $@ $^ $(LDFLAGS) ${CLP_LDFLAGS}

%.o: %.c
	$(CC) $(CFLAGS) $< -o $@

%.o: %.cpp
	$(CXX) $(CFLAGS) $< -o $@

cscope:
	mkprojdb $(CURDIR)

clean:
	-@rm -f *.o osc ost mds 2>/dev/null || true
	-@rm -f osc-test 2>/dev/null || true
	-@rm -f *.log 2>/dev/null || true

tidy:
	-@rm -f *.log 2>/dev/null || true
	-@rm -rf rootdir mountdir

.PHONY: default clean cscope test check tidy