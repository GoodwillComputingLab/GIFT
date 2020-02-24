#include <algorithm>
#include <iostream>

#include "dnet_osc.h"
#include "ost_ops.h"

DatanetOsc::DatanetOsc(const OscInfo *info)
 : _info(info)
{
}

DatanetOsc::~DatanetOsc()
{
  this->_toOsts.clear();
}

bool DatanetOsc::pubOscInfoToOst(const LnetClient *s)
{
  LnetMsg connmsg(AddOsc, sizeof(*this->_info));
  connmsg.marshall(this->_info);
  return s->sendMsgToRemote(&connmsg);
}

bool DatanetOsc::connectToOsts(const std::vector<OstInfo*>& osts, const OscInfo *info)
{
  for (auto s: osts) {
    LSockAddr addr(s->hostname, s->dataport);
    LnetClient *n = new LnetClient(addr, s->dataport);
    this->_toOsts.push_back(n);
    this->pubOscInfoToOst(n);
  }
  return true;
}

ssize_t DatanetOsc::sendDataToOst(unsigned idx, const void *buf, size_t len)
{
  if (idx >= this->_toOsts.size()) return -1;
  return this->_toOsts.at(idx)->sendRawDataToRemote(buf, len);
}

ssize_t DatanetOsc::recvDataFromOst(unsigned idx, void *buf, size_t len)
{
  if (idx >= this->_toOsts.size()) return -1;
  return this->_toOsts.at(idx)->recvRawDataFromRemote(buf, len);
}

ssize_t DatanetOsc::sendMsgToOst(unsigned idx, const LnetMsg *msg)
{
  if (idx >= this->_toOsts.size()) return -1;
  return this->_toOsts.at(idx)->sendMsgToRemote(msg);
}

ssize_t DatanetOsc::recvMsgFromOst(unsigned idx, LnetMsg *msg)
{
  if (idx >= this->_toOsts.size()) return -1;
  return this->_toOsts.at(idx)->recvMsgFromRemote(msg);
}
