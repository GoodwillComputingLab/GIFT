#include <iostream>
#include <unistd.h>
#include <thread>

#include "ost.h"

using namespace std;

const int DEFAULT_GC_TIMER = 15;

OST::OST(const LSockAddr &addr, int port, int id, const char *name, int lnetport, int dataport)
{
  this->_info = new OstInfo();
  this->_info->id = id;
  strncpy(this->_info->name, name, sizeof(this->_info->name));
  ::gethostname(this->_info->hostname, sizeof(this->_info->hostname));
  this->_info->listenport = lnetport;
  this->_info->dataport = dataport;
  this->_dataNet = new DatanetOst(this->_info);
  this->_ostNet = new LnetOst(addr, port, this->_info, this->_dataNet);
  this->_mdsConnected = this->_ostNet->pubOstInfoToMds();
}

OST::~OST()
{
  delete this->_info;
  delete this->_ostNet;
  delete this->_dataNet;
}

void OST::eventLoop()
{
  if (!this->_mdsConnected) return;
  // Start the lnet thread
  // N.B.: We need to create a temporary Lambda function here
  // that takes "this" as argument and returns a std::thread
  // object, since the thread class constructor only supports
  // simple C-style functions.
  thread lnetThread( [=] { this->_ostNet->eventLoop(); });
  // Start the datanet thread
  thread dnetThread( [=] { this->_dataNet->eventLoop(); });
  thread dnetGcThread ( [=] {
    while (true) {
      ::sleep(DEFAULT_GC_TIMER);
      this->_dataNet->garbageCollectCompletedReqs();
     }
  });
  lnetThread.join();
  dnetThread.join();
  dnetGcThread.join();
}

ssize_t OST::sendDataToMds(const void *buf, size_t len)
{
  LnetMsg msg(Data, len);
  memcpy(msg.data, buf, len);
  return this->_ostNet->sendMsgToMds(&msg);
}

ssize_t OST::recvDataFromMds(void *buf, size_t len)
{
  LnetMsg msg(Unknown);
  ssize_t ret = this->_ostNet->recvMsgFromMds(&msg);
  if (msg.extraData && msg.data && buf) {
    size_t smaller = len > msg.len ? msg.len : len;
    memcpy(buf, msg.data, smaller);
  }
  return ret;
}
