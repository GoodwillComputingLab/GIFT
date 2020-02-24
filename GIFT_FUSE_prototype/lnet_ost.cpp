#include <algorithm>
#include <cassert>
#include <iostream>

#include "lnet.h"
#include "ost.h"
#include "ost_ops.h"

LnetOst::LnetOst(const LSockAddr &addr, int port, const OstInfo *info, DatanetOst *dnet)
  : LnetServer(info->listenport),
    _info(info)
{
  this->_toMds = new LnetClient(addr, port);
  this->_mdsInfo = new MdsInfo();
  this->_mdsInfo->id = 0;
  this->_mdsInfo->listenport = port;
  this->_mdsInfo->sock = this->_toMds->getSocket();
  this->_dnet = dnet;
}

LnetOst::~LnetOst()
{
  delete this->_info;
  this->_oscs.clear();
}

bool LnetOst::pubOstInfoToMds()
{
  LnetMsg msg(AddOst, sizeof(*this->_info));
  memcpy(msg.data, this->_info, sizeof(*this->_info));
  if (this->_toMds->sendMsgToRemote(&msg) < 0) return false;
  msg.clear();
  this->addRemoteServer(this->_mdsInfo);
  return true;
}

ssize_t LnetOst::sendMsgToMds(const LnetMsg *msg)
{
  return this->_toMds->sendMsgToRemote(msg);
}

ssize_t LnetOst::recvMsgFromMds(LnetMsg *msg)
{
  return this->_toMds->recvMsgFromRemote(msg);
}

void LnetOst::onConnect()
{
  struct sockaddr_storage remoteAddr;
  socklen_t remoteLen = sizeof(remoteAddr);
  LSocket remote = this->_sock->accept(&remoteAddr, &remoteLen);

  LnetMsg msg(Unknown);
  if (!remote.isValid()) {
    remote.close();
    return;
  }
  remote >> msg;
  if (msg.extraData && msg.len > 0) {
    msg.data = malloc(msg.len);
    remote.readAll(msg.data, msg.len);
  }
  switch (msg.t) {
    case AddOsc:
      this->addOsc(remote, (OscInfo*)msg.data);
      break;
    case Unknown:
    default:
      std::cerr << "Unknown msg received from client: " << msg.t << std::endl;
      break;
  }
}

void LnetOst::onDisconnect(const LnetEntity *remote)
{
  auto osc = std::find(this->_oscs.begin(), this->_oscs.end(), remote);
  if (osc != std::end(this->_oscs)) {
    this->removeClient(remote);
    std::cerr << (*remote) << " disconnected" << std::endl;
    this->_oscs.erase(osc);
  }
}

void LnetOst::onClientRequest(const LnetEntity *remote)
{
  if (!remote || !remote->sock || !remote->sock->isValid()) return;
  if (remote->type == Mds) {
    return this->onRemoteServerRequest(remote);
  }
  LnetMsg msg(Unknown);
  remote->recvMsgFromRemote(&msg);
  std::cerr << "received msg " << msg << " from client " << *remote << std::endl;
  switch (msg.t) {
    default:
      std::cerr << "Unhandled msg type " << msg.t << std::endl;
      break;
  }
}

void LnetOst::onRemoteServerRequest(const LnetEntity *remote)
{
  if (!remote || !remote->sock || !remote->sock->isValid()) return;
  LnetMsg msg(Unknown);
  remote->recvMsgFromRemote(&msg);
  std::cerr << "received msg " << msg << " from mds " << *remote << std::endl;
  switch (msg.t) {
    case Timer:
      this->respondToMdsTimer(remote);
      break;
    case Allocs:
      this->_dnet->setAllocations(remote, &msg);
      break;
    case FsRequest:
      this->handleFsRequest(remote, &msg);
      break;
    default:
      std::cerr << "Unhandled msg type " << msg.t << std::endl;
      break;
  }
}

void LnetOst::addOsc(const LSocket &remote, const OscInfo *info)
{
  LClientSocket *sock = new LClientSocket(remote.sockfd());
  OscInfo *i = new OscInfo(info);
  i->sock = sock;
  this->_oscs.push_back(i);
  std::cerr << *i << " connected" << std::endl;
  this->addClient(i);
}

void LnetOst::respondToMdsTimer(const LnetEntity *remote)
{
  if (!remote || !remote->sock || !remote->sock->isValid()) return;

  std::vector<ActiveRequest> reqs;
  this->_dnet->getActiveRequests(reqs);
  auto sz = reqs.size();
  std::cerr << "OST: Responding to MDS timer request with " << sz << " pending reqs." << std::endl;

  LnetMsg msg(TimerResp, sizeof(sz) + sz * sizeof(ActiveRequest));
  msg.marshall(&sz); // First, send the length of vector
  for (auto i : reqs) {
    msg.marshall(&i);
  }
  remote->sendMsgToRemote(&msg);
}

void LnetOst::handleFsRequest(const LnetEntity *remote, const LnetMsg *msg)
{
  switch (msg->f) {
    case Access:
      OstOps::ost_access(this, remote, msg);
      break;
    case Getattr:
      OstOps::ost_getattr(this, remote, msg);
      break;
    case Fgetattr:
      OstOps::ost_fgetattr(this, remote, msg);
      break;
    case Opendir:
      OstOps::ost_opendir(this, remote, msg);
      break;
    case Mkdir:
      OstOps::ost_mkdir(this, remote, msg);
      break;
    case Readdir:
      OstOps::ost_readdir(this, remote, msg);
      break;
    case Releasedir:
      OstOps::ost_releasedir(this, remote, msg);
      break;
    case Mknod:
      OstOps::ost_mknod(this, remote, msg);
      break;
    case Unlink:
      OstOps::ost_unlink(this, remote, msg);
      break;
    case Rmdir:
      OstOps::ost_rmdir(this, remote, msg);
      break;
    case Chmod:
      OstOps::ost_chmod(this, remote, msg);
      break;
    case Chown:
      OstOps::ost_chown(this, remote, msg);
      break;
    case Truncate:
      OstOps::ost_truncate(this, remote, msg);
      break;
    case Utime:
      OstOps::ost_utime(this, remote, msg);
      break;
    case Open:
      OstOps::ost_open(this, remote, msg);
      break;
    case Statfs:
      OstOps::ost_statfs(this, remote, msg);
      break;
    case Flush:
      OstOps::ost_flush(this, remote, msg);
      break;
    case Release:
      OstOps::ost_release(this, remote, msg);
      break;
    case Fsync:
      OstOps::ost_fsync(this, remote, msg);
      break;
    case Readlink:
      OstOps::ost_readlink(this, remote, msg);
      break;
    case Symlink:
      OstOps::ost_symlink(this, remote, msg);
      break;
    case Rename:
      OstOps::ost_rename(this, remote, msg);
      break;
    case Link:
      OstOps::ost_link(this, remote, msg);
      break;
    case Getxattr:
      OstOps::ost_getxattr(this, remote, msg);
      break;
    case Setxattr:
      OstOps::ost_setxattr(this, remote, msg);
      break;
    case Listxattr:
      OstOps::ost_listxattr(this, remote, msg);
      break;
    default: 
      break;
  }
}
