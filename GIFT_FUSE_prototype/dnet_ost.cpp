#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <tuple>
#include <mutex>
#include <cassert>
#include <condition_variable>
#include <syscall.h>

#include "dnet_ost.h"
#include "ost_ops.h"

using namespace std;

DatanetOst::DatanetOst(const OstInfo *info)
  : LnetServer(info->dataport),
    _info(info)
{
  this->_reqLock = new std::mutex();
  this->_deletedReqsLock = new std::mutex();
}

DatanetOst::~DatanetOst()
{
  delete this->_reqLock;
  delete this->_deletedReqsLock;
  this->_oscs.clear();
  this->_oscSocks.clear();
  this->_reqs.clear();
  this->_deletedReqs.clear();
}

void DatanetOst::addOsc(const LSocket &remote, const OscInfo *info)
{
  LSocket *sock = new LSocket(remote.sockfd());
  this->_oscSocks.push_back(sock);
  OscInfo *i = new OscInfo(info);
  i->sock = sock;
  std::cerr << "[Datanet] OSC " << *i << " connected" << std::endl;
  this->_oscs.push_back(i);
  this->addClient(i);
}

void DatanetOst::onConnect()
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
      std::cerr << "[Datanet] Unknown msg received from client: " << msg.t << std::endl;
      break;
  }
}

void DatanetOst::onDisconnect(const LnetEntity *remote)
{
  auto osc = std::find(this->_oscs.begin(), this->_oscs.end(), remote);
  if (osc != std::end(this->_oscs)) {
    this->removeClient(remote);
    std::cerr << "[Datanet] OSC " << *remote << " disconnected" << std::endl;
    this->_oscs.erase(osc);
    return;
  }
}

void DatanetOst::onClientRequest(const LnetEntity *remote)
{
  if (!remote || !remote->sock || !remote->sock->isValid()) return;
  LnetMsg *msg = new LnetMsg(Unknown);
  remote->recvMsgFromRemote(msg);
  // std::cerr << "[Datanet] received msg " << *msg << " from client: " << remote->name << std::endl;
  switch (msg->t) {
    case FsRequest:
      this->handleClientFsRequest(remote, msg);
      break;
    default:
      std::cerr << "[Datanet] Unhandled msg type " << msg->t << std::endl;
      break;
  }
}

void DatanetOst::onRemoteServerRequest(const LnetEntity *remote)
{
  if (!remote || !remote->sock || !remote->sock->isValid()) return;
}

void DatanetOst::handleClientFsRequest(const LnetEntity *remote, const LnetMsg *msg)
{
  switch (msg->f) {
    case Read:
    case Write:
      // this->handleClientRwRequest(remote, msg, msg->f == Write);
      this->sendMsgToRwThread(remote, msg);
      break;
    default:
      break;
  }
}

void DatanetOst::sendMsgToRwThread(const LnetEntity *remote, const LnetMsg *msg)
{
  assert(msg->t == FsRequest);
  assert(msg->f == Read || msg->f == Write || msg->f == Release);

  size_t size;
  off_t offset;
  uint64_t fd;
  std::string fname;

  if (msg->f == Read || msg->f == Write) {
    msg->unmarshall(&size, &offset, &fd, &fname);
  } else { // msg->f == Close
    msg->unmarshall(&fd, &fname);
  }

  std::lock_guard<std::mutex> lock(*this->_reqLock);
  // Match on <remote, fd> to uniquely identify the open thread stream
  auto req = std::find_if(this->_reqs.begin(), this->_reqs.end(),
                         [=](const RwReq_t& elt)
                         { return memcmp(&std::get<9>(elt), &msg->_i, sizeof msg->_i)  == 0&&
                                  std::get<2>(elt) == (int)fd; });
  if (req != std::end(this->_reqs)) {
    LnetMsg *msgBuf = std::get<1>(*req);
    msgBuf->copy(msg); // TODO: Replace this with a pointer
    // Wake up the RW thread to process the msg
    // LnetEntity *ent = const_cast<LnetEntity*>(std::get<0>(*req));
    *std::get<0>(*req) = remote;
    std::mutex *m = std::get<5>(*req);
    std::condition_variable *cv = std::get<6>(*req);
    bool *p = std::get<7>(*req);
    bool *c = std::get<8>(*req);
    this->wakeUpForMsgProcess(m, cv, p);
    this->waitForMsgProcess(m, cv, c);
  }
}

void DatanetOst::waitForMsg(std::mutex *m, std::condition_variable *cv, bool *c)
{
  std::unique_lock<std::mutex> lk(*m);
  cv->wait(lk, [=]{return *c;});
  *c = false;
}

void DatanetOst::waitForMsgProcess(std::mutex *m, std::condition_variable *cv, bool *c)
{
  std::unique_lock<std::mutex> lk(*m);
  cv->wait(lk, [=]{return *c;});
  *c = false;
}

void DatanetOst::wakeUp(std::mutex *m, std::condition_variable *cv, bool *c)
{
  std::lock_guard<std::mutex> lk(*m);
  *c = true;
  cv->notify_one();
}

void DatanetOst::wakeUpForMsgProcess(std::mutex *m, std::condition_variable *cv, bool *c)
{
  std::lock_guard<std::mutex> lk(*m);
  *c = true;
  cv->notify_one();
}

// For one thread per write request
// void DatanetOst::handleClientRwRequest(const LnetEntity* remote,
//                                        const LnetMsg* msg,
//                                        bool isWrite)
// {
//   std::mutex *m = new std::mutex();
//   std::condition_variable *cv = new std::condition_variable();
//
//   pid_t tid = -1;
//   bool wThreadReady = false;
//   bool pThreadReady = false;
//
//   std::thread *wThread = new std::thread( [=, &tid, &wThreadReady, &pThreadReady]() mutable -> void {
//     tid = syscall(SYS_gettid);
//     {
//       std::lock_guard<std::mutex> lk(*m);
//       wThreadReady = true;
//     }
//     cv->notify_one();
//     if (isWrite) {
//       OstOps::ost_write(this, remote, msg);
//     } else {
//       OstOps::ost_read(this, remote, msg);
//     }
//     this->dequeue(remote, msg);
//     {
//       std::unique_lock<std::mutex> lk(*m);
//       cv->wait(lk, [&pThreadReady]{ return pThreadReady; });
//     }
//     this->addClient(remote);
//   });
//   {
//     std::unique_lock<std::mutex> lk(*m);
//     cv->wait(lk, [&wThreadReady]{return wThreadReady;});
//   }
//   this->enqueue(remote, msg, wThread, tid, m, cv);
//   this->removeClient(remote); // Prevent client from sending more requests
//   {
//     std::lock_guard<std::mutex> lk(*m);
//     pThreadReady = true;
//   }
//   cv->notify_one();
// }

void DatanetOst::enqueue(const LnetEntity **remote,
                         LnetMsg *msg,
                         int fd,
                         const std::thread *th,
                         pid_t tid,
                         std::mutex *m,
                         std::condition_variable *cv,
                         bool *pThreadReady,
                         bool *wThreadReqProcessed,
                         AppInfo i)
{
  std::lock_guard<std::mutex> lock(*this->_reqLock);
  this->_reqs.push_back(std::make_tuple(remote, msg, fd, th, tid,
                                        m, cv, pThreadReady,
                                        wThreadReqProcessed, i));
}

void DatanetOst::dequeue(const LnetEntity *remote, const LnetMsg *msg, AppInfo i)
{
  std::lock_guard<std::mutex> lock(*this->_reqLock);
  auto req = std::find_if(this->_reqs.begin(), this->_reqs.end(),
                         [=](const RwReq_t& elt)
                         { return memcmp(&std::get<9>(elt), &i, sizeof i) == 0 &&
                                  std::get<1>(elt) == msg; });
  if (req != std::end(this->_reqs)) {
    this->markReqForDeletion(*req);
    this->_reqs.erase(req);
  }
}

void DatanetOst::moveToCgroup(pid_t tid, size_t bw)
{
  // sudo mkdir -p /sys/fs/cgroup/blkio/g1
  // cat /proc/partitions  # To get the device major:minor number
  // sudo sh -c echo "8:0 1048576" > /sys/fs/cgroup/blkio/g1/blkio.throttle.write_bps_device
  // echo $$ > /sys/fs/cgroup/blkio/g1/cgroup.procs # For process
  // echo $TID > /sys/fs/cgroup/blkio/g1/tasks # For process

  // FIXME: This mkdir and write to tasks file should only happen once on
  // thread startup
  string dirname = "/sys/fs/cgroup/blkio/";
  dirname += to_string(tid);
  int ret = mkdir(dirname.c_str(), 0777);
  if (ret < 0) {
    perror("mkdir");
  }
  string filename = dirname + "/blkio.throttle.write_bps_device";
  ofstream bwfile(filename, ofstream::out);
  if (!bwfile.is_open()) {
    perror("open bwfile");
  }
  bwfile << "8:0 " << bw;
  bwfile.close();
  string procfname = dirname + "/tasks";
  ofstream procfile(procfname, ofstream::out);
  if (!procfile.is_open()) {
    perror("open procfile");
  }
  procfile << tid;
  procfile.close();
}

void DatanetOst::setAllocations(const LnetEntity *remote, const LnetMsg *msg)
{
  // Perhaps we can acquire the lock later
  std::lock_guard<std::mutex> lock(*this->_reqLock);

  const size_t MAX_BW = 100 * 1024 * 1024; // MB/s
  size_t count = 0;
  size_t offset = 0;
  msg->extractData(&count, offset);
  assert(count >= 0);
  if (count == 0) return;
  AppAllocs_t bws;
  for (size_t i = 0; i < count; i++) {
    int id;
    double r;
    msg->extractData(&id, offset);
    msg->extractData(&r, offset);
    bws.push_back(std::make_tuple(id, r));
  }

  for (auto a : bws) {
    auto r = std::find_if(this->_reqs.begin(), this->_reqs.end(),
                           [=](const RwReq_t& elt)
                           { return std::get<9>(elt).id == std::get<0>(a); });
    if (r != std::end(this->_reqs)) {
      pid_t tid  = std::get<4>(*r);
      cerr << "[OST] Assign " << tid << " to cgroup with bw: " << std::get<1>(a) << endl;
      moveToCgroup(tid, (int)(std::get<1>(a) * MAX_BW));
    }
  }
}

void DatanetOst::markReqForDeletion(const RwReq_t& req)
{
  std::lock_guard<std::mutex> lock(*this->_deletedReqsLock);
  this->_deletedReqs.push_back(req);
}

void DatanetOst::garbageCollectCompletedReqs()
{
  std::lock_guard<std::mutex> lock(*this->_deletedReqsLock);
  for (auto req : this->_deletedReqs) {
    const LnetMsg *msg = std::get<1>(req);
    delete std::get<0>(req); // delete remote object ptr
    std::cerr << "[Datanet] GC: " << msg->f << " for App-" << std::get<9>(req).id << std::endl;
    delete std::get<1>(req); // delete msg
    // delete std::get<2>(req); // delete thread
    delete std::get<5>(req); // delete m
    delete std::get<6>(req); // delete cv
    delete std::get<7>(req); // delete bool
    delete std::get<8>(req); // delete bool
    string dirname = "/sys/fs/cgroup/blkio/";
    dirname += to_string(std::get<4>(req));
    int ret = rmdir(dirname.c_str());
    if (ret < 0) {
      perror("rmdir");
    }
  }
  this->_deletedReqs.clear();
}

void DatanetOst::getActiveRequests(std::vector<ActiveRequest> &reqs) const
{
  std::lock_guard<std::mutex> lock(*this->_reqLock);
  for (auto req : this->_reqs) {
    const LnetMsg *msg = std::get<1>(req);
    AppInfo inf = std::get<9>(req);
    ActiveRequest rq = {._info = inf, ._t = msg->f};
    reqs.push_back(rq);
  }
}
