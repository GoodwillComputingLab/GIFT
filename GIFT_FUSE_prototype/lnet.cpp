#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <iostream>

#include "lnet.h"

std::ostream& operator<<(std::ostream &os, const FsRequestType &t)
{
  switch (t) {
    case Getattr: os << "Getattr"; break;
    case Readlink: os << "Readlink"; break;
    case Mknod: os << "Mknod"; break;
    case Mkdir: os << "Mkdir"; break;
    case Unlink: os << "Unlink"; break;
    case Rmdir: os << "Rmdir"; break;
    case Symlink: os << "Symlink"; break;
    case Rename: os << "Rename"; break;
    case Link: os << "Link"; break;
    case Chmod: os << "Chmod"; break;
    case Chown: os << "Chown"; break;
    case Truncate: os << "Truncate"; break;
    case Utime: os << "Utime"; break;
    case Open: os << "Open"; break;
    case Statfs: os << "Statfs"; break;
    case Flush: os << "Flush"; break;
    case Release: os << "Release"; break;
    case Fsync: os << "Fsync"; break;
    case Setxattr: os << "Setxattr"; break;
    case Getxattr: os << "Getxattr"; break;
    case Listxattr: os << "Listxattr"; break;
    case Removexattr: os << "Remotexattr"; break;
    case Opendir: os << "Opendir"; break;
    case Releasedir: os << "Releasedir"; break;
    case Readdir: os << "Readdir"; break;
    case Access: os << "Access"; break;
    case Ftruncate: os << "Ftruncate"; break;
    case Fgetattr: os << "Fgetattr"; break;
    case Read: os << "Read"; break;
    case Write: os << "Write"; break;
  }
  return os;
}

std::ostream& operator<<(std::ostream &os, const MsgType &t)
{
  switch (t) {
    case Unknown:    os << "Unknown"; break;
    case AddOsc:     os << "AddOsc"; break;
    case AddOst:     os << "AddOst"; break;
    case GetOstInfo: os << "GetOstInfo"; break;
    case OstList:    os << "OstList"; break;
    case Ack:        os << "Ack"; break;
    case Timer:      os << "Timer"; break;
    case TimerResp:  os << "TimerResp"; break;
    case Allocs:     os << "Allocs"; break;
    case FsRequest:  os << "FsRequest"; break;
    case FsResponse: os << "FsResponse"; break;
    case Data:       os << "Data"; break;
    default:         os << "Ununknown"; break;
  }
  return os;
}

std::ostream& operator<<(std::ostream &os, const LEntityType &t)
{
  switch (t) {
    case Invalid: os << "Invalid"; break;
    case Osc:     os << "OSC"; break;
    case Ost:     os << "OST"; break;
    case Mds:     os << "MDS"; break;
    default:      os << "Unknown"; break;
  }
  return os;
}

std::ostream& operator<<(std::ostream &os, const ActiveRequest &r)
{
  os << "AppId: " << r._info.id << "; ReqType: " << r._t;
  return os;
}

LnetMsg::LnetMsg()
{
  this->t = Unknown;
  this->src = NULL;
  this->extraData = false;
  this->len = 0;
  this->data = NULL;
  this->off = 0;
  this->_i = {0};
}

LnetMsg::LnetMsg(MsgType t)
 : LnetMsg()
{
  this->t = t;
}

LnetMsg::LnetMsg(MsgType t, size_t size)
 : LnetMsg(t)
{
  this->extraData = true;
  this->len = size;
  this->data = malloc(size);
}

LnetMsg::LnetMsg(MsgType t, FsRequestType f, size_t size)
 : LnetMsg(t, size)
 {
   this->f = f;
 }

LnetMsg::~LnetMsg()
{
  if (extraData && data) {
    free(data);
  }
}

void LnetMsg::clear()
{
  this->t = Unknown;
  this->src = NULL;
  this->extraData = false;
  this->len = 0;
  this->data = NULL;
}

void LnetMsg::copy(const LnetMsg *msg)
{
  this->off = msg->off;
  this->t = msg->t;
  this->f = msg->f;
  this->extraData = msg->extraData;
  this->src = msg->src;
  this->_i = msg->_i;
  this->len = msg->len;
  this->data = msg->data;
}

void LnetMsg::deepcopy(const LnetMsg *msg)
{
  this->copy(msg);
  if (this->extraData) {
    this->data = malloc(this->len);
    memcpy(this->data, msg->data, this->len);
  }
}

std::ostream& operator<<(std::ostream &os, const LnetMsg &m)
{
  if (m.t == FsRequest) {
    os << "(" << m.t << "[" << m.f << "], extraData: " << m.extraData << ", len: " << m.len << ", data: " << m.data << ")";
  } else {
    os << "(" << m.t << ", extraData: " << m.extraData << ", len: " << m.len << ", data: " << m.data << ")";
  }
  return os;
}

std::ostream& operator<<(std::ostream &os, const LnetEntity &m)
{
  os << "(" << m.type << ", " << m.id << ", name: "
     << m.name << ", host: " << m.hostname
     << ", listenport: " << m.listenport << ")";
  return os;
}

LnetClient::LnetClient(const LSockAddr &addr, int port)
{
  this->_sockToRemoteServer = new LClientSocket();
  this->connectToRemoteServer(addr, port);
}

LnetClient::~LnetClient()
{
  delete this->_sockToRemoteServer;
}

bool LnetClient::connectToRemoteServer(const LSockAddr &addr, int port)
{
  return this->_sockToRemoteServer->connect(addr, port);
}

ssize_t LnetClient::sendMsgToRemote(const LnetMsg *msg) const
{
  ssize_t totalData = 0;
  totalData += this->_sockToRemoteServer->writeAll(msg, sizeof(*msg));
  if (msg->extraData && msg->len > 0 && msg->data) {
    totalData += this->_sockToRemoteServer->writeAll(msg->data, msg->len);
  }
  return totalData;
}

ssize_t LnetClient::recvMsgFromRemote(LnetMsg *msg)
{
  ssize_t totalData = 0;
  totalData += this->_sockToRemoteServer->readAll(msg, sizeof(*msg));
  if (msg->extraData && msg->len > 0) {
    msg->data = malloc(msg->len);
    totalData += this->_sockToRemoteServer->readAll(msg->data, msg->len);
  }
  return totalData;
}

ssize_t LnetClient::sendRawDataToRemote(const void* buf, size_t len)
{
  return this->_sockToRemoteServer->writeAll(buf, len);
}

ssize_t LnetClient::recvRawDataFromRemote(void* buf, size_t len)
{
  return this->_sockToRemoteServer->readAll(buf, len);
}

LSocket *LnetClient::getSocket()
{
  return this->_sockToRemoteServer;
}