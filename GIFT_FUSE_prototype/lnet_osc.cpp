#include <iostream>
#include <algorithm>

#include "lnet.h"
#include "osc.h"

LnetOsc::LnetOsc(const LSockAddr &addr, int port, const OscInfo *info)
 : _info(info)
{
  this->_toMds = new LnetClient(addr, port);
}

LnetOsc::~LnetOsc()
{
  this->_info = NULL;
  delete this->_toMds;
  this->_toOsts.clear();
}

bool LnetOsc::pubOscInfoToMds() const
{
  if (!this->_info) return false;
  LnetMsg msg(AddOsc, sizeof(*this->_info));
  msg.marshall(this->_info);
  if (this->_toMds->sendMsgToRemote(&msg) < (ssize_t)sizeof(msg)) return false;
  return true;
}

bool LnetOsc::pubOscInfoToOst(const LnetClient *ost) const
{
  if (!this->_info) return false;
  LnetMsg msg(AddOsc, sizeof(*this->_info));
  msg.marshall(this->_info);
  if (ost->sendMsgToRemote(&msg)) return false;
  return true;
}

bool LnetOsc::connectToOsts(std::vector<OstInfo*> &osts)
{
  LnetMsg msg(GetOstInfo);
  if (this->_toMds->sendMsgToRemote(&msg) < 0) return false;
  msg.clear();
  if (this->_toMds->recvMsgFromRemote(&msg) < 0) return false;
  if (!msg.extraData) return false;
  int count = 0;
  size_t offset = 0;
  msg.extractData(&count, offset);
  for (int i = 0; i < count; i++) {
    OstInfo *inf = new OstInfo();
    msg.extractData(inf, offset);
    LSockAddr addr(inf->hostname, inf->listenport);
    LnetClient *n = new LnetClient(addr, inf->listenport);
    this->pubOscInfoToOst(n);
    inf->sock = n->getSocket();
    osts.push_back(inf);
  }
  return true;
}

ssize_t LnetOsc::sendMsgToOst(const LnetMsg *msg, int idx)
{
  return this->_toOsts.at(idx)->sendMsgToRemote(msg);
}

ssize_t LnetOsc::recvMsgFromOst(LnetMsg *msg, int idx)
{
  return this->_toOsts.at(idx)->recvMsgFromRemote(msg);
}

ssize_t LnetOsc::sendMsgToMds(const LnetMsg *msg)
{
  return this->_toMds->sendMsgToRemote(msg);
}

ssize_t LnetOsc::recvMsgFromMds(LnetMsg *msg)
{
  return this->_toMds->recvMsgFromRemote(msg);
}