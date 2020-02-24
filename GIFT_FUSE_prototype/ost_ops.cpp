#include <cassert>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <iostream>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/statvfs.h>
#include <sys/xattr.h>
#include <utime.h>
#include <syscall.h>

#include "ost.h"
#include "ost_ops.h"
#include "dnet_ost.h"

// Data API
// ssize_t ost_read(const char *path, char *buf, size_t size, off_t offset)
void
OstOps::ost_read(const DatanetOst* dost, const LnetEntity *remote, const LnetMsg *msg)
{
  assert(msg->t == FsRequest);
  assert(msg->extraData && msg->len > 0);

  size_t size;
  off_t offset;
  uint64_t fd;
  std::string fname;
  msg->unmarshall(&size, &offset, &fd, &fname);

  char *buf = new char[size];

  int ret = ::pread(fd, buf, size, offset);
  if (ret < 0) {
    std::cerr << "ost: could not read: " << fname << ". Error: " << strerror(errno) << std::endl;
  }
  LnetMsg res(FsResponse, sizeof(ret) + sizeof(errno));
  res.marshall(&ret, &errno);
  remote->sendMsgToRemote(&res);
  if (ret > 0) {
    remote->sendDataToRemote(buf, ret);
  }
  delete[] buf;
}

// ssize_t ost_write(const char *path, const char *buf, size_t size, off_t offset)
void
OstOps::ost_write(const DatanetOst *dost, const LnetEntity *remote, const LnetMsg *msg)
{
  assert(msg->t == FsRequest);
  assert(msg->extraData && msg->len > 0);

  size_t size;
  off_t offset;
  uint64_t fd;
  std::string fname;
  msg->unmarshall(&size, &offset, &fd, &fname);

  char *buf = new char[size];
  // TODO: Spawn thread to do the write; put the thread in a cgroup
  // TODO: In open, always open with O_DIRECT
  remote->recvDataFromRemote(buf, size);

  int ret = ::pwrite(fd, buf, size, offset);
  if (ret < 0) {
    std::cerr << "ost: could not write: " << fname << ". Error: " << strerror(errno) << std::endl;
  }
  LnetMsg res(FsResponse, sizeof(ret) + sizeof(errno));
  res.marshall(&ret, &errno);
  remote->sendMsgToRemote(&res);
  delete[] buf;
}

// Metadata API
// int ost_readlink(const char *path, char *link, size_t size)
void
OstOps::ost_readlink(const LnetOst *lost, const LnetEntity *remote, const LnetMsg *msg)
{
  assert(msg->t == FsRequest);
  assert(msg->extraData && msg->len > 0);

  size_t size;
  std::string fname;
  msg->unmarshall(&size, &fname);

  char link[size + 1];
  int ret = ::readlink(fname.c_str(), link, size);
  if (ret < 0) {
    std::cerr << "ost: could not readlink: " << fname << ". Error: " << strerror(errno) << std::endl;
  }
  BufType mybuf = {.sz = size, .databuf = link};
  LnetMsg res(FsResponse, sizeof(ret) + sizeof(errno) + sizeof(mybuf.sz) + size);
  res.src = msg->src;
  res.marshall(&ret, &errno, &mybuf);
  remote->sendMsgToRemote(&res);
}

// int ost_mknod(const char *path, mode_t mode, dev_t dev)
void
OstOps::ost_mknod(const LnetOst *lost, const LnetEntity *remote, const LnetMsg *msg)
{
  assert(msg->t == FsRequest);
  assert(msg->extraData && msg->len > 0);

  mode_t mode;
  dev_t dev;
  std::string fname;
  msg->unmarshall(&mode, &dev, &fname);

  int ret = ::mknod(fname.c_str(), mode, dev);
  if (ret < 0) {
    std::cerr << "ost: could not mknod: " << fname << ". Error: " << strerror(errno) << std::endl;
  }
  LnetMsg res(FsResponse, sizeof(ret) + sizeof(errno));
  res.src = msg->src;
  res.marshall(&ret, &errno);
  remote->sendMsgToRemote(&res);
}

void
OstOps::ost_mkdir(const LnetOst *lost, const LnetEntity *remote, const LnetMsg *msg)
{
  assert(msg->t == FsRequest);
  assert(msg->extraData && msg->len > 0);

  mode_t mode;
  std::string dname;

  msg->unmarshall(&mode, &dname);

  int ret = ::mkdir(dname.c_str(), mode);
  if (ret < 0) {
    std::cerr << "ost: could not create dir: " << dname << ". Error: " << strerror(errno) << std::endl;
  }
  LnetMsg res(FsResponse, sizeof(ret) + sizeof(errno));
  res.src = msg->src;
  res.marshall(&ret, &errno);
  remote->sendMsgToRemote(&res);
}

// int ost_access(const char *path, int mask)
void
OstOps::ost_access(const LnetOst *lost, const LnetEntity *remote, const LnetMsg *msg)
{
  assert(msg->t == FsRequest);
  assert(msg->extraData && msg->len > 0);

  int mask;
  std::string fname;

  msg->unmarshall(&mask, &fname);

  int ret = ::access(fname.c_str(), mask);
  if (ret < 0) {
    std::cerr << "ost: could not access path: " << fname << ". Error: " << strerror(errno) << std::endl;
  }
  LnetMsg res(FsResponse, sizeof(ret) + sizeof(errno));
  res.src = msg->src;
  res.marshall(&ret, &errno);
  remote->sendMsgToRemote(&res);
}

// int ost_opendir(const char *path)
void
OstOps::ost_opendir(const LnetOst *lost, const LnetEntity *remote, const LnetMsg *msg)
{
  assert(msg->t == FsRequest);
  assert(msg->extraData && msg->len > 0);

  std::string dname;
  msg->unmarshall(&dname);

  DIR *dp = ::opendir(dname.c_str());
  if (!dp) {
    std::cerr << "ost: could not open directory path: " << dname << ". Error: " << strerror(errno) << std::endl;
  } else {
    std::cerr << "ost: opendir: " << dname << ". DP: " << dp << std::endl;
  }
  LnetMsg res(FsResponse, sizeof(dp) + sizeof(errno));
  res.src = msg->src;
  res.marshall(&dp, &errno);
  remote->sendMsgToRemote(&res);
}

// int ost_releasedir(const char *path)
void
OstOps::ost_releasedir(const LnetOst *lost, const LnetEntity *remote, const LnetMsg *msg)
{
  assert(msg->t == FsRequest);
  assert(msg->extraData && msg->len > 0);

  DIR *dp;
  std::string dname;
  msg->unmarshall(&dp, &dname);

  int ret = ::closedir(dp);
  if (ret < 0) {
    std::cerr << "ost: could not release directory path: " << dname << ". DP: " << dp << ". Error: " << strerror(errno) << std::endl;
  }
  LnetMsg res(FsResponse, res.len = sizeof(ret) + sizeof(errno));
  res.src = msg->src;
  res.marshall(&ret, &errno);
  remote->sendMsgToRemote(&res);
}

// int ost_getattr(const char *path, struct stat *statbuf)
void
OstOps::ost_getattr(const LnetOst *lost, const LnetEntity *remote, const LnetMsg *msg)
{
  assert(msg->t == FsRequest);
  assert(msg->extraData && msg->len > 0);

  struct stat statbuf = {0};
  std::string fname;
  msg->unmarshall(&fname);

  int ret = ::lstat(fname.c_str(), &statbuf);
  if (ret < 0) {
    std::cerr << "ost: could not lstat: " << fname << ". Error: " << strerror(errno) << std::endl;
  } else {
    std::cerr << "ost: lstat: " << fname << std::endl;
  }
  LnetMsg res(FsResponse, sizeof(ret) + sizeof(errno) + sizeof(statbuf));
  res.src = msg->src;
  res.marshall(&ret, &errno, &statbuf);
  remote->sendMsgToRemote(&res);
}

// int ost_unlink(const char *path)
void
OstOps::ost_unlink(const LnetOst *lost, const LnetEntity *remote, const LnetMsg *msg)
{
  assert(msg->t == FsRequest);
  assert(msg->extraData && msg->len > 0);

  std::string fname;
  msg->unmarshall(&fname);

  int ret = ::unlink(fname.c_str());
  if (ret < 0) {
    std::cerr << "ost: could not unlink: " << fname << ". Error: " << strerror(errno) << std::endl;
  } else {
    std::cerr << "ost: unlink: " << fname << std::endl;
  }
  LnetMsg res(FsResponse, sizeof(ret) + sizeof(errno));
  res.src = msg->src;
  res.marshall(&ret, &errno);
  remote->sendMsgToRemote(&res);
}

// int ost_rmdir(const char *path)
void
OstOps::ost_rmdir(const LnetOst *lost, const LnetEntity *remote, const LnetMsg *msg)
{
  assert(msg->t == FsRequest);
  assert(msg->extraData && msg->len > 0);

  std::string dname;
  msg->unmarshall(&dname);

  int ret = ::rmdir(dname.c_str());
  if (ret < 0) {
    std::cerr << "ost: could not rmdir: " << dname << ". Error: " << strerror(errno) << std::endl;
  } else {
    std::cerr << "ost: rmdir: " << dname << std::endl;
  }
  LnetMsg res(FsResponse, sizeof(ret) + sizeof(errno));
  res.src = msg->src;
  res.marshall(&ret, &errno);
  remote->sendMsgToRemote(&res);
}

// int ost_symlink(const char *path, const char *link)
void
OstOps::ost_symlink(const LnetOst *lost, const LnetEntity *remote, const LnetMsg *msg)
{
  assert(msg->t == FsRequest);
  assert(msg->extraData && msg->len > 0);

  char fn[PATH_MAX], ln[PATH_MAX];
  BufType fname = {.sz = PATH_MAX, .databuf = fn},
           link = {.sz = PATH_MAX, .databuf = ln};
  msg->unmarshall(&fname, &link);

  int ret = ::symlink(fn, ln);
  if (ret < 0) {
    std::cerr << "ost: could not symlink: " << fn << ". Error: " << strerror(errno) << std::endl;
  } else {
    std::cerr << "ost: symlink: " << ln << " --> " << fn << std::endl;
  }
  LnetMsg res(FsResponse, sizeof(ret) + sizeof(errno));
  res.src = msg->src;
  res.marshall(&ret, &errno);
  remote->sendMsgToRemote(&res);
}

// int ost_rename(const char *path, const char *newpath)
void
OstOps::ost_rename(const LnetOst *lost, const LnetEntity *remote, const LnetMsg *msg)
{
  assert(msg->t == FsRequest);
  assert(msg->extraData && msg->len > 0);

  char fn[PATH_MAX], ln[PATH_MAX];
  BufType fname = {.sz = PATH_MAX, .databuf = fn},
           link = {.sz = PATH_MAX, .databuf = ln};
  msg->unmarshall(&fname, &link);

  int ret = ::rename(fn, ln);
  if (ret < 0) {
    std::cerr << "ost: could not rename: " << fn << ". Error: " << strerror(errno) << std::endl;
  } else {
    std::cerr << "ost: rename: " << fn << std::endl;
  }
  LnetMsg res(FsResponse, sizeof(ret) + sizeof(errno));
  res.src = msg->src;
  res.marshall(&ret, &errno);
  remote->sendMsgToRemote(&res);
}

// int ost_link(const char *path, const char *newpath)
void
OstOps::ost_link(const LnetOst *lost, const LnetEntity *remote, const LnetMsg *msg)
{
  assert(msg->t == FsRequest);
  assert(msg->extraData && msg->len > 0);

  char fn[PATH_MAX], ln[PATH_MAX];
  BufType fname = {.sz = PATH_MAX, .databuf = fn},
           link = {.sz = PATH_MAX, .databuf = ln};
  msg->unmarshall(&fname, &link);

  int ret = ::link(fn, ln);
  if (ret < 0) {
    std::cerr << "ost: could not link: " << fn << ". Error: " << strerror(errno) << std::endl;
  } else {
    std::cerr << "ost: link: " << fn << std::endl;
  }
  LnetMsg res(FsResponse, sizeof(ret) + sizeof(errno));
  res.src = msg->src;
  res.marshall(&ret, &errno);
  remote->sendMsgToRemote(&res);
}

// int ost_chmod(const char *path, mode_t mode)
void
OstOps::ost_chmod(const LnetOst *lost, const LnetEntity *remote, const LnetMsg *msg)
{
  assert(msg->t == FsRequest);
  assert(msg->extraData && msg->len > 0);

  mode_t mode;
  std::string fname;
  msg->unmarshall(&mode, &fname);

  int ret = ::chmod(fname.c_str(), mode);
  if (ret < 0) {
    std::cerr << "ost: could not chmod: " << fname << ". Error: " << strerror(errno) << std::endl;
  } else {
    std::cerr << "ost: chmod: " << fname << std::endl;
  }
  LnetMsg res(FsResponse, sizeof(ret) + sizeof(errno));
  res.src = msg->src;
  res.marshall(&ret, &errno);
  remote->sendMsgToRemote(&res);
}

// int ost_chown(const char *path, uid_t uid, gid_t gid)
void
OstOps::ost_chown(const LnetOst *lost, const LnetEntity *remote, const LnetMsg *msg)
{
  assert(msg->t == FsRequest);
  assert(msg->extraData && msg->len > 0);

  uid_t uid;
  gid_t gid;
  std::string fname;
  msg->unmarshall(&uid, &gid, &fname);

  int ret = ::chown(fname.c_str(), uid, gid);
  if (ret < 0) {
    std::cerr << "ost: could not chown: " << fname << ". Error: " << strerror(errno) << std::endl;
  } else {
    std::cerr << "ost: chown: " << fname << std::endl;
  }
  LnetMsg res(FsResponse, sizeof(ret) + sizeof(errno));
  res.src = msg->src;
  res.marshall(&ret, &errno);
  remote->sendMsgToRemote(&res);
}

// int ost_truncate(const char *path, off_t newsize)
void
OstOps::ost_truncate(const LnetOst *lost, const LnetEntity *remote, const LnetMsg *msg)
{
  assert(msg->t == FsRequest);
  assert(msg->extraData && msg->len > 0);

  off_t newsize;
  std::string fname;
  msg->unmarshall(&newsize, &fname);

  int ret = ::truncate(fname.c_str(), newsize);
  if (ret < 0) {
    std::cerr << "ost: could not truncate: " << fname << ". Error: " << strerror(errno) << std::endl;
  } else {
    std::cerr << "ost: truncate: " << fname << std::endl;
  }
  LnetMsg res(FsResponse, sizeof(ret) + sizeof(errno));
  res.src = msg->src;
  res.marshall(&ret, &errno);
  remote->sendMsgToRemote(&res);
}

// int ost_utime(const char *path, struct utimbuf *ubuf)
void
OstOps::ost_utime(const LnetOst *lost, const LnetEntity *remote, const LnetMsg *msg)
{
  assert(msg->t == FsRequest);
  assert(msg->extraData && msg->len > 0);

  struct utimbuf ubuf;
  std::string fname;
  msg->unmarshall(&fname);

  int ret = ::utime(fname.c_str(), &ubuf);
  if (ret < 0) {
    std::cerr << "ost: could not utime: " << fname << ". Error: " << strerror(errno) << std::endl;
  } else {
    std::cerr << "ost: utime: " << fname << std::endl;
  }
  LnetMsg res(FsResponse, sizeof(ret) + sizeof(errno) + sizeof(ubuf));
  res.src = msg->src;
  res.marshall(&ret, &errno, &ubuf);
  remote->sendMsgToRemote(&res);
}

void
OstOps::startRwThread(const LnetOst *lost, int fd, const LnetEntity *remote, AppInfo i)
{
  pid_t tid = -1;
  std::mutex *m = new std::mutex();
  std::condition_variable *cv = new std::condition_variable();
  bool wThreadReady = false; // Indicates if the RW thread has finished initialization
  bool *pThreadReady = new bool(false); // Indicates if the parent thread has some data
  bool *wThreadReqProcessed = new bool(false); // Indicates if the RW thread has processed a RW request
  const LnetEntity **ent = new const LnetEntity*;
  LnetMsg *msg = new LnetMsg(Unknown);

  std::thread *wThread = new std::thread( [=, &tid, &wThreadReady]() mutable -> void
  {
    tid = syscall(SYS_gettid);
    {
      std::lock_guard<std::mutex> lk(*m);
      wThreadReady = true;
    }
    cv->notify_one();
    while (true) {
      // Wait for Read/Write/Close msg from the remote
      lost->_dnet->waitForMsg(m, cv, pThreadReady);
      if (msg->f == Release) {
        break;
      } else if (msg->f == Write) {
        OstOps::ost_write(lost->_dnet, *ent, msg);
        lost->_dnet->wakeUp(m, cv, wThreadReqProcessed);
      } else if (msg->f == Read) {
        OstOps::ost_read(lost->_dnet, *ent, msg);
        lost->_dnet->wakeUp(m, cv, wThreadReqProcessed);
      } else {
        continue;
      }
    }
    lost->_dnet->wakeUp(m, cv, wThreadReqProcessed);
    lost->_dnet->dequeue(remote, msg, i);
  });
  {
    std::unique_lock<std::mutex> lk(*m);
    cv->wait(lk, [&wThreadReady]{return wThreadReady;});
  }
  lost->_dnet->enqueue(ent, msg, fd, wThread, tid,
                       m, cv, pThreadReady, wThreadReqProcessed, i);
}

// int ost_open(const char *path, int flags, mode_t mode)
void
OstOps::ost_open(const LnetOst *lost, const LnetEntity *remote, const LnetMsg *msg)
{
  assert(msg->t == FsRequest);
  assert(msg->extraData && msg->len > 0);

  int flags;
  mode_t mode;
  std::string fname;
  msg->unmarshall(&flags, &mode, &fname);

  int ret = ::open(fname.c_str(), flags, mode);
  if (ret < 0) {
    std::cerr << "ost: could not open: " << fname << ". Error: " << strerror(errno) << std::endl;
  } else {
    std::cerr << "ost: open: " << fname << std::endl;
    OstOps::startRwThread(lost, ret, msg->src, msg->_i);
  }
  LnetMsg res(FsResponse, sizeof(ret) + sizeof(errno));
  res.src = msg->src;
  res.marshall(&ret, &errno);
  remote->sendMsgToRemote(&res);
}

// int ost_statfs(const char *path, struct statvfs *statv)
void
OstOps::ost_statfs(const LnetOst *lost, const LnetEntity *remote, const LnetMsg *msg)
{
  assert(msg->t == FsRequest);
  assert(msg->extraData && msg->len > 0);

  struct statvfs statbuf = {0};
  std::string fname;
  msg->unmarshall(&fname);

  int ret = ::statvfs(fname.c_str(), &statbuf);
  if (ret < 0) {
    std::cerr << "ost: could not stat: " << fname << ". Error: " << strerror(errno) << std::endl;
  } else {
    std::cerr << "ost: stat: " << fname << std::endl;
  }
  LnetMsg res(FsResponse, sizeof(ret) + sizeof(errno) + sizeof(statbuf));
  res.src = msg->src;
  res.marshall(&ret, &errno, &statbuf);
  remote->sendMsgToRemote(&res);
}

// int ost_flush(const char *path, struct fuse_file_info *fi)
void
OstOps::ost_flush(const LnetOst *lost, const LnetEntity *remote, const LnetMsg *msg)
{
  assert(msg->t == FsRequest);
  assert(msg->extraData && msg->len > 0);

  uint64_t fd;
  std::string fname;
  msg->unmarshall(&fd, &fname);

  int ret = ::fsync(fd);
  if (ret < 0) {
    std::cerr << "ost: could not flush: " << fname << ". Error: " << strerror(errno) << std::endl;
  } else {
    std::cerr << "ost: flush: " << fname << std::endl;
  }
  LnetMsg res(FsResponse, sizeof(ret) + sizeof(errno));
  res.src = msg->src;
  res.marshall(&ret, &errno);
  remote->sendMsgToRemote(&res);
}

// int ost_release(const char *path, struct fuse_file_info *fi)
void
OstOps::ost_release(const LnetOst *lost, const LnetEntity *remote, const LnetMsg *msg)
{
  assert(msg->t == FsRequest);
  assert(msg->extraData && msg->len > 0);

  uint64_t fd;
  std::string fname;
  msg->unmarshall(&fd, &fname);

  int ret = ::close(fd);
  if (ret < 0) {
    std::cerr << "ost: could not release: " << fname << ". Error: " << strerror(errno) << std::endl;
  } else {
    lost->_dnet->sendMsgToRwThread(msg->src, msg);
    std::cerr << "ost: release: " << fname << std::endl;
  }
  LnetMsg res(FsResponse, sizeof(ret) + sizeof(errno));
  res.src = msg->src;
  res.marshall(&ret, &errno);
  remote->sendMsgToRemote(&res);
}

// int ost_fsync(const char *path, int datasync)
void
OstOps::ost_fsync(const LnetOst *lost, const LnetEntity *remote, const LnetMsg *msg)
{
  assert(msg->t == FsRequest);
  assert(msg->extraData && msg->len > 0);

  uint64_t fd;
  int datasync;
  std::string fname;
  msg->unmarshall(&fd, &datasync, &fname);

  int ret;
  if (datasync) {
    ret = ::fdatasync(fd);
  } else {
    ret = ::fsync(fd);
  }
  if (ret < 0) {
    std::cerr << "ost: could not fsync: " << fname << ". Error: " << strerror(errno) << std::endl;
  } else {
    std::cerr << "ost: fsync: " << fname << std::endl;
  }
  LnetMsg res(FsResponse, sizeof(ret) + sizeof(errno));
  res.src = msg->src;
  res.marshall(&ret, &errno);
  remote->sendMsgToRemote(&res);
}

// int ost_setxattr(const char *path, const char *name, const char *value, size_t size, int flags)
void
OstOps::ost_setxattr(const LnetOst *lost, const LnetEntity *remote, const LnetMsg *msg)
{
  assert(msg->t == FsRequest);
  assert(msg->extraData && msg->len > 0);

  size_t size;
  int flags;
  char fn[PATH_MAX], ln[PATH_MAX], vn[PATH_MAX];
  BufType fname = {.sz = PATH_MAX, .databuf = fn},
           name = {.sz = PATH_MAX, .databuf = ln},
          vname = {.sz = PATH_MAX, .databuf = vn};
  msg->unmarshall(&size, &flags, &fname, &name, &vname);

  int ret = ::setxattr(fn, ln, vn, size, flags);
  if (ret < 0) {
    std::cerr << "ost: could not setxattr: " << fn << ": " << ln << " --> " << vn << ". Error: " << strerror(errno) << std::endl;
  } else {
    std::cerr << "ost: setxattr: " << fn << ": " << ln << ". Value: " << vn << std::endl;
  }
  LnetMsg res(FsResponse, sizeof(ret) + sizeof(errno));
  res.src = msg->src;
  res.marshall(&ret, &errno);
  remote->sendMsgToRemote(&res);
}

// int ost_getxattr(const char *path, const char *name, char *value, size_t size)
void
OstOps::ost_getxattr(const LnetOst *lost, const LnetEntity *remote, const LnetMsg *msg)
{
  assert(msg->t == FsRequest);
  assert(msg->extraData && msg->len > 0);

  size_t size;
  char fn[PATH_MAX], name[PATH_MAX];
  BufType fname = {.sz = PATH_MAX, .databuf = fn},
           link = {.sz = PATH_MAX, .databuf = name};
  msg->unmarshall(&size, &fname, &link);

  char value[size + 1];
  ssize_t ret = ::getxattr(fn, name, value, size);
  BufType mybuf = {.sz = size, .databuf = value};
  if (ret < 0) {
    std::cerr << "ost: could not getxattr: " << fn << ": " << name << ". Error: " << strerror(errno) << std::endl;
    mybuf.sz = 0;
  } else {
    std::cerr << "ost: getxattr: " << fn << ": " << name << ". Value: " << value << std::endl;
    mybuf.sz = ret;
  }
  LnetMsg res(FsResponse, sizeof(ret) + sizeof(errno) + sizeof(mybuf.sz) + mybuf.sz);
  res.src = msg->src;
  res.marshall(&ret, &errno, &mybuf);
  remote->sendMsgToRemote(&res);
}

// int ost_listxattr(const char *path, char *list, size_t size)
void
OstOps::ost_listxattr(const LnetOst *lost, const LnetEntity *remote, const LnetMsg *msg)
{
  assert(msg->t == FsRequest);
  assert(msg->extraData && msg->len > 0);

  size_t size;
  std::string fname;
  msg->unmarshall(&size, &fname);

  char list[size + 1];
  ssize_t ret = ::listxattr(fname.c_str(), list, size);
  BufType mybuf = {.sz = size, .databuf = list};
  if (ret < 0) {
    std::cerr << "ost: could not listxattr: " << fname << ". Error: " << strerror(errno) << std::endl;
    mybuf.sz = 0;
  } else {
    std::cerr << "ost: listxattr: " << fname << ". Return: " << ret << std::endl;
    mybuf.sz = ret;
  }
  LnetMsg res(FsResponse, sizeof(ret) + sizeof(errno) + sizeof(mybuf.sz) + mybuf.sz);
  res.src = msg->src;
  res.marshall(&ret, &errno, &mybuf);
  remote->sendMsgToRemote(&res);
}

int
ost_removexattr(const char *path, const char *name)
{
  return -1;
}

int
ost_fsyncdir(const char *path, int datasync)
{
  return -1;
}

// int ost_readdir(const char *path, void *buf, off_t offset)
void
OstOps::ost_readdir(const LnetOst *lost, const LnetEntity *remote, const LnetMsg *msg)
{
  assert(msg->t == FsRequest);
  assert(msg->extraData && msg->len > 0);

  DIR *dp;
  std::string dname;
  msg->unmarshall(&dp, &dname);

  errno = 0;
  std::vector<dirent> dirents;
  struct dirent *de = ::readdir(dp);
  if (de == 0) {
    std::cerr << "ost: could not readdir: " << dname << ". Error: " << strerror(errno) << std::endl;
    goto done;
  }
  do {
    dirents.push_back(*de);
  } while ((de = readdir(dp)) != NULL);

done:
  size_t len = dirents.size();
  LnetMsg res(FsResponse, sizeof(de) + sizeof(errno) + sizeof(len) + sizeof(*de) * dirents.size());
  res.src = msg->src;
  res.marshall(&de, &errno, &len);
  for (auto d: dirents) {
    res.marshall(&d);
  }
  remote->sendMsgToRemote(&res);
}

int
ost_ftruncate(const char *path, off_t offset)
{
  return -1;
}

// int ost_fgetattr(const char *path, struct stat *statbuf)
void
OstOps::ost_fgetattr(const LnetOst *lost, const LnetEntity *remote, const LnetMsg *msg)
{
  assert(msg->t == FsRequest);
  assert(msg->extraData && msg->len > 0);

  uint64_t fd;
  std::string fname;
  msg->unmarshall(&fd, &fname);

  struct stat statbuf = {0};
  int ret = ::fstat(fd, &statbuf);
  if (ret < 0) {
    std::cerr << "ost: could not fgetattr: " << fname << ". Error: " << strerror(errno) << std::endl;
  }
  LnetMsg res(FsResponse, sizeof(ret) + sizeof(errno) + sizeof(statbuf));
  res.src = msg->src;
  res.marshall(&ret, &errno, &statbuf);
  remote->sendMsgToRemote(&res);
}