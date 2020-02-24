#include "config.h"
#include "params.h"

#include <cassert>
#include <cstring>
#include <fuse.h>
#include <iostream>
#include <string>
#include <unistd.h>
#include <sys/types.h>

#include "log.h"
#include "lnet.h"
#include "osc.h"
#include "osc_ops.h"

#define shift argc--; argv++
#define REMOTE

using namespace std;

OSC::OSC(const LSockAddr &addr, int port, int id, const char *name)
{
  this->_info = new OscInfo();
  this->_info->id = id;
  strncpy(this->_info->name, name, sizeof(this->_info->name));
  ::gethostname(this->_info->hostname, sizeof(this->_info->hostname));
  this->_oscNet = new LnetOsc(addr, port, this->_info);
  this->_dataNet = new DatanetOsc(this->_info);
  this->_mdsConnected = this->_oscNet->pubOscInfoToMds();
}

OSC::~OSC()
{
  delete _info;
  delete _oscNet;
  delete _dataNet;
  this->_osts.clear();
  this->fdToApp.clear();
}

bool OSC::connectToOsts()
{
  if (this->_oscNet->connectToOsts(this->_osts)) {
    return this->_dataNet->connectToOsts(this->_osts, this->_info);
  }
  return false;
}

bool OSC::start()
{
  return this->_mdsConnected &&
         this->connectToOsts();
}

ssize_t OSC::sendDataToOst(int idx, const void *buf, size_t len)
{
  return this->_dataNet->sendDataToOst(idx, buf, len);
}

ssize_t OSC::recvDataFromOst(int idx, void *buf, size_t len)
{
  return this->_dataNet->recvDataFromOst(idx, buf, len);
}

bool OSC::createDirStructure(const std::string &rootdir)
{
  for (auto s: this->_osts) {
    std::string dname = rootdir + "/" + s->name;
    if (lemu_mkdir(dname.c_str(), 0600) != 0) {
      return false;
    }
  }
  return true;
}

ssize_t OSC::sendMsgToMds(const LnetMsg *msg)
{
  return this->_oscNet->sendMsgToMds(msg);
}

ssize_t OSC::recvMsgFromMds(LnetMsg *msg)
{
  return this->_oscNet->recvMsgFromMds(msg);
}

ssize_t OSC::sendMsgToOst(const LnetMsg *msg, int id)
{
  return this->_oscNet->sendMsgToOst(msg, id);
}

ssize_t OSC::recvMsgFromOst(LnetMsg *msg, int id)
{
  return this->_oscNet->recvMsgFromOst(msg, id);
}

ssize_t OSC::sendDatanetMsgToOst(const LnetMsg *msg, int id)
{
  return this->_dataNet->sendMsgToOst(id, msg);
}

ssize_t OSC::recvDatanetMsgFromOst(LnetMsg *msg, int id)
{
  return this->_dataNet->recvMsgFromOst(id, msg);
}

const OstInfo*
OSC::getOstFromPath(const char *path) const
{
  for (auto v: this->_osts) {
    if (strstr(path, v->name)) {
      return v;
    }
  }
  return NULL;
}

void OSC::insertFdToApp(int fd, AppInfo i)
{
  this->fdToApp[fd] = i;
}

bool OSC::getAppForFd(int fd, AppInfo *i)
{
  if (this->fdToApp.find(fd) != std::end(this->fdToApp)) {
    *i = this->fdToApp[fd];
    return true;
  }
  return false;
}

void OSC::cleanFdMap(int fd)
{
  auto it = this->fdToApp.find(fd);
  if (it != std::end(this->fdToApp)) {
    this->fdToApp.erase(it);
  }
}