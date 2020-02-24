#include <cassert>
#include <dirent.h>
#include <iostream>
#include <sys/types.h>

#include "lnet.h"
#include "mds.h"

bool MdsOps::mds_commonprolog(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg, FsRequestType t)
{
  if (msg->f != t || !msg->extraData || !msg->data || !remote->sock || !remote->sock->isValid()) {
    std::cerr << "mds: [ERROR]: Expected: " << msg->f << ". Actual: " << t << std::endl;
    return false;
  }
  return true;
}

void MdsOps::mds_commonepilog(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg, const std::string &dname)
{
  const OstInfo *s = lmds->getOstFromPath(&dname);
  if (s) {
    lmds->sendMsgToOst(msg, s->id);
  } else {
    std::cerr << "mds: [ERROR]: No OST corresponding to path: " << dname << std::endl;
    int ret = -1, myerror = -ENOENT;
    LnetMsg res(FsResponse);
    res.marshall(&ret, &myerror);
    remote->sendMsgToRemote(&res);
  }
}

void MdsOps::mds_mkdir(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg)
{
  if (!MdsOps::mds_commonprolog(lmds, remote, msg, Mkdir)) return;

  mode_t mode;
  std::string dname;
  msg->unmarshall(&mode, &dname);

  MdsOps::mds_commonepilog(lmds, remote, msg, dname);
}

void MdsOps::mds_access(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg)
{
  if (!MdsOps::mds_commonprolog(lmds, remote, msg, Access)) return;

  int mask;
  std::string dname;
  msg->unmarshall(&mask, &dname);

  MdsOps::mds_commonepilog(lmds, remote, msg, dname);
}

void MdsOps::mds_opendir(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg)
{
  if (!MdsOps::mds_commonprolog(lmds, remote, msg, Opendir)) return;

  std::string dname;
  msg->unmarshall(&dname);
  std::cerr << "mds: opendir: " << dname << std::endl;

  MdsOps::mds_commonepilog(lmds, remote, msg, dname);
}

void MdsOps::mds_releasedir(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg)
{
  if (!MdsOps::mds_commonprolog(lmds, remote, msg, Releasedir)) return;

  DIR *dp;
  std::string dname;
  msg->unmarshall(&dp, &dname);
  std::cerr << "mds: releasedir: " << dname << std::endl;

  MdsOps::mds_commonepilog(lmds, remote, msg, dname);
}

void MdsOps::mds_getattr(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg)
{
  if (!MdsOps::mds_commonprolog(lmds, remote, msg, Getattr)) return;

  std::string fname;
  msg->unmarshall(&fname);
  std::cerr << "mds: getattr: " << fname << std::endl;

  MdsOps::mds_commonepilog(lmds, remote, msg, fname);
}

void MdsOps::mds_fgetattr(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg)
{
  if (!MdsOps::mds_commonprolog(lmds, remote, msg, Fgetattr)) return;

  uint64_t fd;
  std::string fname;
  msg->unmarshall(&fd, &fname);
  std::cerr << "mds: fgetattr: " << fname << std::endl;

  MdsOps::mds_commonepilog(lmds, remote, msg, fname);
}

void MdsOps::mds_mknod(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg)
{
  if (!MdsOps::mds_commonprolog(lmds, remote, msg, Mknod)) return;

  mode_t mode;
  dev_t dev;
  std::string fname;
  msg->unmarshall(&mode, &dev, &fname);
  std::cerr << "mds: mknod: " << fname << std::endl;

  MdsOps::mds_commonepilog(lmds, remote, msg, fname);
}

void MdsOps::mds_unlink(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg)
{
  if (!MdsOps::mds_commonprolog(lmds, remote, msg, Unlink)) return;

  std::string fname;
  msg->unmarshall(&fname);
  std::cerr << "mds: unlink: " << fname << std::endl;

  MdsOps::mds_commonepilog(lmds, remote, msg, fname);
}

void MdsOps::mds_rmdir(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg)
{
  if (!MdsOps::mds_commonprolog(lmds, remote, msg, Rmdir)) return;

  std::string dname;
  msg->unmarshall(&dname);
  std::cerr << "mds: rmdir: " << dname << std::endl;

  MdsOps::mds_commonepilog(lmds, remote, msg, dname);
}

void MdsOps::mds_chmod(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg)
{
  if (!MdsOps::mds_commonprolog(lmds, remote, msg, Chmod)) return;

  mode_t mode;
  std::string fname;
  msg->unmarshall(&mode, &fname);
  std::cerr << "mds: chmod: " << fname << std::endl;

  MdsOps::mds_commonepilog(lmds, remote, msg, fname);
}

void MdsOps::mds_chown(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg)
{
  if (!MdsOps::mds_commonprolog(lmds, remote, msg, Chown)) return;

  uid_t uid;
  gid_t gid;
  std::string fname;
  msg->unmarshall(&uid, &gid, &fname);
  std::cerr << "mds: chown: " << fname << std::endl;

  MdsOps::mds_commonepilog(lmds, remote, msg, fname);
}

void MdsOps::mds_truncate(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg)
{
  if (!MdsOps::mds_commonprolog(lmds, remote, msg, Truncate)) return;

  off_t newsize;
  std::string fname;
  msg->unmarshall(&newsize, &fname);
  std::cerr << "mds: truncate: " << fname << std::endl;

  MdsOps::mds_commonepilog(lmds, remote, msg, fname);
}

void MdsOps::mds_utime(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg)
{
  if (!MdsOps::mds_commonprolog(lmds, remote, msg, Utime)) return;

  std::string fname;
  msg->unmarshall(&fname);
  std::cerr << "mds: utime: " << fname << std::endl;

  MdsOps::mds_commonepilog(lmds, remote, msg, fname);
}

void MdsOps::mds_open(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg)
{
  if (!MdsOps::mds_commonprolog(lmds, remote, msg, Open)) return;

  int flags;
  mode_t mode;
  std::string fname;
  msg->unmarshall(&flags, &mode, &fname);
  std::cerr << "mds: open: " << fname << std::endl;

  MdsOps::mds_commonepilog(lmds, remote, msg, fname);
}

void MdsOps::mds_statfs(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg)
{
  if (!MdsOps::mds_commonprolog(lmds, remote, msg, Statfs)) return;

  std::string fname;
  msg->unmarshall(&fname);
  std::cerr << "mds: statfs: " << fname << std::endl;

  MdsOps::mds_commonepilog(lmds, remote, msg, fname);
}

void MdsOps::mds_flush(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg)
{
  if (!MdsOps::mds_commonprolog(lmds, remote, msg, Flush)) return;

  uint64_t fd;
  std::string fname;
  msg->unmarshall(&fd, &fname);
  std::cerr << "mds: flush: " << fname << std::endl;

  MdsOps::mds_commonepilog(lmds, remote, msg, fname);
}

void MdsOps::mds_release(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg)
{
  if (!MdsOps::mds_commonprolog(lmds, remote, msg, Release)) return;

  uint64_t fd;
  std::string fname;
  msg->unmarshall(&fd, &fname);
  std::cerr << "mds: release: " << fname << std::endl;

  MdsOps::mds_commonepilog(lmds, remote, msg, fname);
}

void MdsOps::mds_fsync(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg)
{
  if (!MdsOps::mds_commonprolog(lmds, remote, msg, Fsync)) return;

  uint64_t fd;
  int datasync;
  std::string fname;
  msg->unmarshall(&fd, &datasync, &fname);
  std::cerr << "mds: fsync: " << fname << std::endl;

  MdsOps::mds_commonepilog(lmds, remote, msg, fname);
}

void MdsOps::mds_readlink(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg)
{
  if (!MdsOps::mds_commonprolog(lmds, remote, msg, Readlink)) return;

  size_t size;
  std::string fname;
  msg->unmarshall(&size, &fname);
  std::cerr << "mds: readlink: " << fname << std::endl;

  MdsOps::mds_commonepilog(lmds, remote, msg, fname);
}

void MdsOps::mds_symlink(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg)
{
  if (!MdsOps::mds_commonprolog(lmds, remote, msg, Symlink)) return;

  char fn[PATH_MAX], ln[PATH_MAX];
  BufType fname = {.sz = PATH_MAX, .databuf = fn},
           link = {.sz = PATH_MAX, .databuf = ln};
  msg->unmarshall(&fname, &link);
  std::cerr << "mds: symlink: " << fn << std::endl;

  // XXX: The assumption here is that both target and link are on the same OST
  MdsOps::mds_commonepilog(lmds, remote, msg, fn);
}

void MdsOps::mds_rename(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg)
{
  if (!MdsOps::mds_commonprolog(lmds, remote, msg, Rename)) return;

  char fn[PATH_MAX], ln[PATH_MAX];
  BufType fname = {.sz = PATH_MAX, .databuf = fn},
           link = {.sz = PATH_MAX, .databuf = ln};
  msg->unmarshall(&fname, &link);
  std::cerr << "mds: rename: " << fn << " --> " << ln << std::endl;

  MdsOps::mds_commonepilog(lmds, remote, msg, fn);
}

void MdsOps::mds_link(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg)
{
  if (!MdsOps::mds_commonprolog(lmds, remote, msg, Link)) return;

  char fn[PATH_MAX], ln[PATH_MAX];
  BufType fname = {.sz = PATH_MAX, .databuf = fn},
           link = {.sz = PATH_MAX, .databuf = ln};
  msg->unmarshall(&fname, &link);
  std::cerr << "mds: link: " << fn << std::endl;

  // XXX: The assumption here is that both target and link are on the same OST
  MdsOps::mds_commonepilog(lmds, remote, msg, fn);
}

void MdsOps::mds_readdir(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg)
{
  if (!MdsOps::mds_commonprolog(lmds, remote, msg, Readdir)) return;

  DIR *dp;
  std::string dname;
  msg->unmarshall(&dp, &dname);
  std::cerr << "mds: readdir: " << dname << std::endl;

  // XXX: The assumption here is that both target and link are on the same OST
  MdsOps::mds_commonepilog(lmds, remote, msg, dname);
}

void MdsOps::mds_listxattr(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg)
{
  if (!MdsOps::mds_commonprolog(lmds, remote, msg, Listxattr)) return;

  size_t size;
  std::string fname;
  msg->unmarshall(&size, &fname);
  std::cerr << "mds: listxattr: " << fname << std::endl;

  MdsOps::mds_commonepilog(lmds, remote, msg, fname);
}

void MdsOps::mds_getxattr(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg)
{
  if (!MdsOps::mds_commonprolog(lmds, remote, msg, Getxattr)) return;

  size_t size;
  char fn[PATH_MAX], ln[PATH_MAX];
  BufType fname = {.sz = PATH_MAX, .databuf = fn},
           link = {.sz = PATH_MAX, .databuf = ln};
  msg->unmarshall(&size, &fname, &link);
  std::cerr << "mds: getxattr: " << fn << ": " << ln << std::endl;

  MdsOps::mds_commonepilog(lmds, remote, msg, fn);
}

void MdsOps::mds_setxattr(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg)
{
  if (!MdsOps::mds_commonprolog(lmds, remote, msg, Setxattr)) return;

  size_t size;
  int flags;
  char fn[PATH_MAX], ln[PATH_MAX], vn[PATH_MAX];
  BufType fname = {.sz = PATH_MAX, .databuf = fn},
           link = {.sz = PATH_MAX, .databuf = ln},
          vname = {.sz = PATH_MAX, .databuf = vn};
  msg->unmarshall(&size, &flags, &fname, &link, &vname);
  std::cerr << "mds: setxattr: " << fn << ": " << ln << std::endl;

  MdsOps::mds_commonepilog(lmds, remote, msg, fn);
}