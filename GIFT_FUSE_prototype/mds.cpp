#include <iostream>
#include <thread>
#include <unistd.h>

#include "mds.h"

const int DEFAULT_PORT = 7779;
const int DEFAULT_TIMER = 10;

MDS::MDS(int port)
{
  this->m = new std::mutex();
  this->waitForAllOsts = new std::condition_variable();
  this->dataIsReady = false;
  this->_mdsNet = new LnetMds(port, this->m, this->waitForAllOsts, &this->dataIsReady);
}

MDS::~MDS()
{
  this->_mdsNet->printStats();
  delete this->_mdsNet;
  delete this->m;
  delete this->waitForAllOsts;
  this->dataIsReady = false;
}

void MDS::eventLoop()
{
  if (!this->_mdsNet) return;
  std::thread lnetThread( [=] { this->_mdsNet->eventLoop(); });
  std::thread timerThread( [=] { this->startTimer(); });
  lnetThread.join();
  timerThread.join();
}

void MDS::startTimer()
{
  while (true) {
    ::sleep(DEFAULT_TIMER);
    LnetMsg msg(Timer);
    if (this->_mdsNet->bcastMsgToOsts(&msg)) {
      // The timer msg has been broadcasted to all the OSTs. Now, refrain
      // from restarting the timer and sending another set of timer msgs
      // until all the OSTs have responded in this phase and have been
      // responded to.
      std::unique_lock<std::mutex> lk(*this->m);
      this->waitForAllOsts->wait(lk, [=]{ return this->dataIsReady; });
    }
  }
}

ssize_t MDS::sendDataToOst(int idx, const void *buf, size_t len)
{
  // TODO
  return -1;
}

ssize_t MDS::recvDataFromOst(int idx, void *buf, size_t len)
{
  // TODO
  return -1;
}
