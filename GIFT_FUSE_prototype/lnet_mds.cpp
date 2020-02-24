#include <algorithm>
#include <cassert>
#include <iostream>
#include <vector>
#include <set>

#include "lnet.h"
#include "mds.h"
#include "ClpSimplex.hpp"

static std::vector<AppCoup_t> coupDatabase;
static std::vector<AppData_t> rdmpDatabase;
static std::vector<double> effectiveSysBw;

LnetMds::LnetMds(int port, std::mutex *m, std::condition_variable *cv, bool *b)
  : LnetServer(port),
    _m(m),
    _waitForAllOsts(cv),
    _dataIsReady(b)
{
}

LnetMds::~LnetMds()
{
  this->_oscs.clear();
  this->_osts.clear();
  this->_dirToOst.clear();
  this->_ostReqs.clear();
}

const OstInfo *LnetMds::getOstFromPath(const std::string *path) const
{
  for (auto v: this->_dirToOst) {
    if (strstr(path->c_str(), v.first.c_str())) {
      return v.second;
    }
  }
  return NULL;
}

void LnetMds::onConnect()
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
    case AddOst:
      this->addOst(remote, (OstInfo*)msg.data);
      break;
    default:
      std::cerr << "Unknown msg received from client: " << msg.t << std::endl;
      break;
  }
}

void LnetMds::addOsc(const LSocket &remote, const OscInfo *info)
{
  LClientSocket *sock = new LClientSocket(remote.sockfd());
  OscInfo *i = new OscInfo(info);
  i->sock = sock;
  this->_oscs.push_back(i);
  std::cerr << "OSC " << *i << " connected" << std::endl;
  this->addClient(i);
}

void LnetMds::addOst(const LSocket &remote, const OstInfo *info)
{
  LClientSocket *sock = new LClientSocket(remote.sockfd());
  OstInfo *i = new OstInfo(info);
  i->sock = sock;
  this->_osts.push_back(i);
  std::cerr << *i << " connected" << std::endl;
  this->_dirToOst[i->name] = i;
  this->addClient(i);
}

void LnetMds::onDisconnect(const LnetEntity *remote)
{
  auto osc = std::find(this->_oscs.begin(), this->_oscs.end(), remote);
  if (osc != std::end(this->_oscs)) {
    this->removeClient(remote);
    this->_oscs.erase(osc);
    std::cerr << *remote << " disconnected" << std::endl;
    return;
  }
  auto ost = std::find(this->_osts.begin(), this->_osts.end(), remote);
  if (ost != std::end(this->_osts)) {
    this->removeClient(remote);
    std::cerr << *remote << " disconnected" << std::endl;
    this->_osts.erase(ost);
    return;
  }
}

void LnetMds::onClientRequest(const LnetEntity *remote)
{
  if (!remote || !remote->sock || !remote->sock->isValid()) return;
  LnetMsg msg(Unknown);
  remote->recvMsgFromRemote(&msg);
  std::cerr << "received msg " << msg << " from " << *remote << std::endl;
  switch (msg.t) {
    case GetOstInfo:
      this->sendOstsInfo(remote);
      break;
    case FsRequest:
      memcpy(&msg.src, &remote, sizeof(remote));
      this->handleFsRequest(remote, &msg);
      break;
    case FsResponse:
      assert (msg.extraData);
      msg.src->sendMsgToRemote(&msg);
      break;
    case TimerResp:
      assert (msg.extraData);
      this->addToTimerResponse(remote, &msg);
      break;
    default:
      std::cerr << "Unhandled msg " << msg << std::endl;
      break;
  }
}

void LnetMds::computeBwAllocations(Policy_t policy,
                                   const std::vector<std::vector<int>> &reqs,
                                   MapOstToAppAllocs_t &allocs)
{
  // Assert that all the OSTs are covered
  const size_t NUM_OSTS = this->_osts.size();
  assert (reqs.size() == NUM_OSTS);

  switch (policy) {
    case POFS:
      computeBwAllocationsPOFS(NUM_OSTS, reqs, allocs);
      break;
    case BSIP:
      computeBwAllocationsBSIP(NUM_OSTS, reqs, allocs);
      break;
	  case TSA:
      computeBwAllocationsTSA(NUM_OSTS, reqs, allocs);
      break;
	  case ESA:
      computeBwAllocationsESA(NUM_OSTS, reqs, allocs);
      break;
	  case TMF:
      computeBwAllocationsTMF(NUM_OSTS, reqs, allocs);
      break;
	  case RND:
      computeBwAllocationsRND(NUM_OSTS, reqs, allocs);
      break;
	  case MBW:
      computeBwAllocationsMBW(NUM_OSTS, reqs, allocs);
      break;
    case GIFT:
      computeBwAllocationsGIFT(NUM_OSTS, reqs, allocs);
      break;
  }

}

void LnetMds::addToTimerResponse(const LnetEntity *remote, const LnetMsg *msg)
{
  size_t count = 0;
  size_t offset = 0;
  msg->extractData(&count, offset);
  assert(count >= 0);
  std::vector<ActiveRequest> reqs;
  for (size_t i = 0; i < count; i++) {
    ActiveRequest r;
    msg->extractData(&r, offset);
    reqs.push_back(r);
  }
  this->_ostReqs[remote] = reqs;
  if (this->_ostReqs.size() == this->_osts.size()) {
    std::vector<std::vector<int>> out;
    MapOstToAppAllocs_t allocs;
    std::cerr << "Received timer responses from all OSTs" << std::endl;
    for (auto s : this->_ostReqs) {
      std::cerr << "OST: " << s.first->name << std::endl;
      std::vector<int> apps;
      for (auto r : s.second) {
        std::cerr << "\t" << r << std::endl;
        apps.push_back(r._info.id);
      }
      out.push_back(apps);
    }
    this->computeBwAllocations(GIFT, out, allocs);
    this->bcastAllocsToOsts(allocs);
    this->_ostReqs.clear();
    {
      std::lock_guard<std::mutex> lk(*this->_m);
      *this->_dataIsReady = true;
    }
    this->_waitForAllOsts->notify_one();
  }
}

void LnetMds::bcastAllocsToOsts(const MapOstToAppAllocs_t &allocs)
{
  size_t idx = 0;
  for (auto a : allocs) {
    std::cerr << "OST Allocs: " << idx << std::endl;
    size_t sz = a.size();
    LnetMsg msg(Allocs, sizeof (sz) + (sizeof(int) + sizeof(double)) * sz);
    msg.pack(&sz);
    std::cerr << "\t Allocs: ";
    for (auto i : a) {
      AppAlloc_t d = i;
      std::cerr << std::get<0>(d) << " -> " << std::get<1>(d) << "; ";
      msg.pack(&std::get<0>(d));
      msg.pack(&std::get<1>(d));
    }
    std::cerr << std::endl;
    this->sendMsgToOst(&msg, idx);
    idx++;
  }
}

void LnetMds::onRemoteServerRequest(const LnetEntity *remote)
{
  if (!remote || !remote->sock || !remote->sock->isValid()) return;
}

void LnetMds::sendOstsInfo(const LnetEntity *remote)
{
  if (!remote || !remote->sock || !remote->sock->isValid()) return;
  int count = this->_osts.size();
  LnetMsg msg(OstList, count * sizeof(OstInfo) + sizeof(int));
  msg.marshall(&count);
  for (auto s: this->_osts) {
    msg.marshall(s);
  }
  remote->sendMsgToRemote(&msg);
}

void LnetMds::handleFsRequest(const LnetEntity *remote, const LnetMsg *msg)
{
  switch (msg->f) {
    case Access:
      MdsOps::mds_access(this, remote, msg);
      break;
    case Opendir:
      MdsOps::mds_opendir(this, remote, msg);
      break;
    case Mkdir:
      MdsOps::mds_mkdir(this, remote, msg);
      break;
    case Releasedir:
      MdsOps::mds_releasedir(this, remote, msg);
      break;
    case Getattr:
      MdsOps::mds_getattr(this, remote, msg);
      break;
    case Readdir:
      MdsOps::mds_readdir(this, remote, msg);
      break;
    case Mknod:
      MdsOps::mds_mknod(this, remote, msg);
      break;
    case Unlink:
      MdsOps::mds_unlink(this, remote, msg);
      break;
    case Rmdir:
      MdsOps::mds_rmdir(this, remote, msg);
      break;
    case Chmod:
      MdsOps::mds_chmod(this, remote, msg);
      break;
    case Chown:
      MdsOps::mds_chown(this, remote, msg);
      break;
    case Truncate:
      MdsOps::mds_truncate(this, remote, msg);
      break;
    case Utime:
      MdsOps::mds_utime(this, remote, msg);
      break;
    case Open:
      MdsOps::mds_open(this, remote, msg);
      break;
    case Statfs:
      MdsOps::mds_statfs(this, remote, msg);
      break;
    case Flush:
      MdsOps::mds_flush(this, remote, msg);
      break;
    case Release:
      MdsOps::mds_release(this, remote, msg);
      break;
    case Fsync:
      MdsOps::mds_fsync(this, remote, msg);
      break;
    case Readlink:
      MdsOps::mds_readlink(this, remote, msg);
      break;
    case Symlink:
      MdsOps::mds_symlink(this, remote, msg);
      break;
    case Rename:
      MdsOps::mds_rename(this, remote, msg);
      break;
    case Link:
      MdsOps::mds_link(this, remote, msg);
      break;
    case Fgetattr:
      MdsOps::mds_fgetattr(this, remote, msg);
      break;
    case Getxattr:
      MdsOps::mds_getxattr(this, remote, msg);
      break;
    case Setxattr:
      MdsOps::mds_setxattr(this, remote, msg);
      break;
    case Listxattr:
      MdsOps::mds_listxattr(this, remote, msg);
      break;
    default: 
      break;
  }
}

ssize_t LnetMds::sendMsgToOst(const LnetMsg *msg, int id) const
{
  auto s = std::find_if(_osts.begin(), _osts.end(), [&id](const OstInfo &i) { return i.id == id; } );
  if (s == std::end(_osts) || !(*s)->sock || !(*s)->sock->isValid()) return -1;
  return (*s)->sendMsgToRemote(msg);
}

ssize_t LnetMds::recvMsgFromOst(LnetMsg *msg, int id) const
{
  auto s = std::find_if(_osts.begin(), _osts.end(), [&id](const OstInfo &i) { return i.id == id; } );
  if (s == std::end(_osts) || !(*s)->sock || !(*s)->sock->isValid()) return -1;
  return (*s)->recvMsgFromRemote(msg);
}

bool LnetMds::bcastMsgToOsts(const LnetMsg *msg)
{
  if (this->_osts.size() <= 0) return false;
  std::cerr << "Broadcasting timer msg to all OSTs" << std::endl;
  for (auto s: this->_osts) {
    if (this->sendMsgToOst(msg, s->id) < (ssize_t)sizeof(*msg)) {
      return false;
    }
  }
  return true;
}

double LnetMds::getEffectiveSysBw()
{
  double effectiveSystemBandwidth = 0.0;
  for (auto bw : effectiveSysBw) {
    effectiveSystemBandwidth += bw;
  }
  effectiveSystemBandwidth = effectiveSystemBandwidth / effectiveSysBw.size();
  return effectiveSystemBandwidth;
}
