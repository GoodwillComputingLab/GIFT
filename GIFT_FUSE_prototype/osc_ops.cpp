#include "config.h"
#include "params.h"

#include <iostream>
#include <fstream>
#include <string>
#include <cassert>
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <libgen.h>
#include <limits.h>
#include <cstdlib>
#include <stdio.h>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>

#ifdef HAVE_SYS_XATTR_H
#include <sys/xattr.h>
#endif

#include "log.h"
#include "osc.h"
#include "osc_ops.h"

using namespace std;

#ifndef TEST_ENV
struct fuse_operations lemu_oper {
  .getattr            = lemu_getattr,
  .readlink           = lemu_readlink,
  .getdir             = lemu_getdir,
  .mknod              = lemu_mknod,
  .mkdir              = lemu_mkdir,
  .unlink             = lemu_unlink,
  .rmdir              = lemu_rmdir,
  .symlink            = lemu_symlink,
  .rename             = lemu_rename,
  .link               = lemu_link,
  .chmod              = lemu_chmod,
  .chown              = lemu_chown,
  .truncate           = lemu_truncate,
  .utime              = lemu_utime,
  .open               = lemu_open,
  .read               = lemu_read,
  .write              = lemu_write,
  .statfs             = lemu_statfs,
  .flush              = lemu_flush,
  .release            = lemu_release,
  .fsync              = lemu_fsync,
  .setxattr           = lemu_setxattr,
  .getxattr           = lemu_getxattr,
  .listxattr          = lemu_listxattr,
  .removexattr        = lemu_removexattr,
  .opendir            = lemu_opendir,
  .readdir            = lemu_readdir,
  .releasedir         = lemu_releasedir,
  .fsyncdir           = lemu_fsyncdir,
  .init               = lemu_init,
  .destroy            = lemu_destroy,
  .access             = lemu_access,
  .create             = lemu_create,
  .ftruncate          = lemu_ftruncate,
  .fgetattr           = lemu_fgetattr,
  .lock               = lemu_lock,
  .utimens            = lemu_utimens,
  .bmap               = lemu_bmap,
  .flag_nullpath_ok   = 0,
  .flag_nopath        = 0,
  .flag_utime_omit_ok = 0,
  .flag_reserved      = 0,
  .ioctl              = lemu_ioctl,
  .poll               = lemu_poll,
  .write_buf          = NULL, // lemu_write_buf,
  .read_buf           = NULL, // lemu_read_buf,
  .flock              = lemu_flock,
  .fallocate          = lemu_fallocate,
};
#endif // ifndef TEST_ENV

//  All the paths I see are relative to the root of the mounted
//  filesystem.  In order to get to the underlying filesystem, I need to
//  have the mountpoint.  I'll save it away early on in main(), and then
//  whenever I need a path for something I'll call this to construct
//  it.
static void
lemu_fullpath(char fpath[PATH_MAX], const char *path)
{
  strcpy(fpath, LEMU_DATA->rootdir);
  strncat(fpath, path, PATH_MAX); // ridiculously long paths will
  // break here

  log_msg("    lemu_fullpath:  rootdir = \"%s\", path = \"%s\", fpath = \"%s\"\n",
      LEMU_DATA->rootdir, path, fpath);
}

static int
handleRootDir(const char *fpath, void *buf,
              fuse_fill_dir_t filler, off_t offset,
              struct fuse_file_info *fi, FsRequestType f)
{
  switch (f) {
    case Readdir:
      log_msg("[Readdir]: on rootdir\n", f);
      for (auto s: LEMU_DATA->osc->getOsts()) {
        if (filler(buf, s->name, NULL, 0) != 0) {
          log_msg("    ERROR lemu_readdir filler:  buffer full");
          return -ENOMEM;
        }
      }
      break;
    case Opendir:
      log_msg("[Opendir]: on rootdir\n", f);
      fi->fh = (uint64_t)::opendir(fpath);
      return log_error("lemu_opendir opendir");
      break;
    case Releasedir:
      log_msg("[Releasedir]: on rootdir\n", f);
      return log_syscall("releasedir", ::closedir((DIR*)fi->fh), 0);
      break;
    default:
      log_msg("[ERROR]: Unknown fs operation: %d\n", f);
      break;
  }
  return 0;
}

static bool
areFilepathsEqual(const char *fpath, const char *npath)
{
  char rpath[PATH_MAX] = {0};
  ::realpath(fpath, rpath);

  return strncmp(rpath, npath, sizeof(rpath)) == 0;
}

///////////////////////////////////////////////////////////
//
// Prototypes for all these functions, and the C-style comments,
// come from /usr/include/fuse.h
//
/** Get file attributes.
 *
 * Similar to stat().  The 'st_dev' and 'st_blksize' fields are
 * ignored.  The 'st_ino' field is ignored except if the 'use_ino'
 * mount option is given.
 */
EXTERNC int
lemu_getattr(const char *path, struct stat *statbuf)
{
  int retstat;
  char fpath[PATH_MAX];

  log_msg("\nlemu_getattr(path=\"%s\", statbuf=0x%08x)\n",
      path, statbuf);
  lemu_fullpath(fpath, path);

  LnetMsg msg(FsRequest, Getattr, strlen(fpath) + 1);
  msg.marshall(fpath);
  LnetMsg res(Unknown);
  if (LEMU_DATA->osc->sendMsgToMds(&msg)) {
    if (LEMU_DATA->osc->recvMsgFromMds(&res)) {
      assert(res.t == FsResponse);
      res.unmarshall(&retstat, &errno, statbuf);
    }
  }

  retstat = log_syscall("lstat", retstat, 0);

  log_stat(statbuf);

  return retstat;
}

/** Read the target of a symbolic link
 *
 * The buffer should be filled with a null terminated string.  The
 * buffer size argument includes the space for the terminating
 * null character.  If the linkname is too long to fit in the
 * buffer, it should be truncated.  The return value should be 0
 * for success.
 */
// Note the system readlink() will truncate and lose the terminating
// null.  So, the size passed to to the system readlink() must be one
// less than the size passed to lemu_readlink()
// lemu_readlink() code by Bernardo F Costa (thanks!)
EXTERNC int
lemu_readlink(const char *path, char *link, size_t size)
{
  int retstat;
  char fpath[PATH_MAX];

  log_msg("\nlemu_readlink(path=\"%s\", link=\"%s\", size=%d)\n",
      path, link, size);
  lemu_fullpath(fpath, path);

  LnetMsg msg(FsRequest, Readlink, strlen(fpath) + sizeof(size) + 1);
  msg.marshall(&size, fpath);
  LnetMsg res(Unknown);
  BufType mybuf = {.sz = size, .databuf = link};
  auto ret = 0;
  if (LEMU_DATA->osc->sendMsgToMds(&msg)) {
    if (LEMU_DATA->osc->recvMsgFromMds(&res)) {
      assert(res.t == FsResponse);
      res.unmarshall(&ret, &errno, &mybuf);
    }
  }

  retstat = log_syscall("readlink", ret, 0);
  if (retstat >= 0) {
    link[retstat] = '\0';
    retstat = 0;
    log_msg("    link=\"%s\"\n", link);
  }

  return 0;
}

/** Create a file node
 *
 * There is no create() operation, mknod() will be called for
 * creation of all non-directory, non-symlink nodes.
 */
// shouldn't that comment be "if" there is no.... ?
EXTERNC int
lemu_mknod(const char *path, mode_t mode, dev_t dev)
{
  int retstat;
  char fpath[PATH_MAX];

  log_msg("\nlemu_mknod(path=\"%s\", mode=0%3o, dev=%lld)\n",
      path, mode, dev);
  lemu_fullpath(fpath, path);

  LnetMsg msg(FsRequest, Mknod, strlen(fpath) + sizeof(mode) + sizeof(dev) + 1);
  msg.marshall(&mode, &dev, fpath);
  LnetMsg res(Unknown);
  auto ret = 0;
  if (LEMU_DATA->osc->sendMsgToMds(&msg)) {
    if (LEMU_DATA->osc->recvMsgFromMds(&res)) {
      assert(res.t == FsResponse);
      res.unmarshall(&ret, &errno);
    }
  }

  retstat = log_syscall("mknod", ret, 0);

  return retstat;
}

/** Create a directory */
EXTERNC int
lemu_mkdir(const char *path, mode_t mode)
{
  char fpath[PATH_MAX];

  log_msg("\nlemu_mkdir(path=\"%s\", mode=0%3o)\n",
      path, mode);
  lemu_fullpath(fpath, path);

  LnetMsg msg(FsRequest, Mkdir, strlen(fpath) + sizeof(mode) + 1);
  msg.marshall(&mode, fpath);
  LnetMsg res(Unknown);
  auto ret = 0;
  if (LEMU_DATA->osc->sendMsgToMds(&msg)) {
    if (LEMU_DATA->osc->recvMsgFromMds(&res)) {
      assert(res.t == FsResponse);
      res.unmarshall(&ret, &errno);
    }
  }

  return log_syscall("mkdir", ret, 0); // mkdir(fpath, mode), 0);
}

/** Remove a file */
EXTERNC int
lemu_unlink(const char *path)
{
  char fpath[PATH_MAX];

  log_msg("lemu_unlink(path=\"%s\")\n",
      path);
  lemu_fullpath(fpath, path);

  LnetMsg msg(FsRequest, Unlink, strlen(fpath) + 1);
  msg.marshall(fpath);
  LnetMsg res(Unknown);
  auto ret = 0;
  if (LEMU_DATA->osc->sendMsgToMds(&msg)) {
    if (LEMU_DATA->osc->recvMsgFromMds(&res)) {
      assert(res.t == FsResponse);
      res.unmarshall(&ret, &errno);
    }
  }

  return log_syscall("unlink", ret, 0);
}

/** Remove a directory */
EXTERNC int
lemu_rmdir(const char *path)
{
  char fpath[PATH_MAX];

  log_msg("lemu_rmdir(path=\"%s\")\n",
      path);
  lemu_fullpath(fpath, path);

  LnetMsg msg(FsRequest, Rmdir, strlen(fpath) + 1);
  msg.marshall(fpath);
  LnetMsg res(Unknown);
  auto ret = 0;
  if (LEMU_DATA->osc->sendMsgToMds(&msg)) {
    if (LEMU_DATA->osc->recvMsgFromMds(&res)) {
      assert(res.t == FsResponse);
      res.unmarshall(&ret, &errno);
    }
  }

  return log_syscall("rmdir", ret, 0);
}

/** Create a symbolic link */
// The parameters here are a little bit confusing, but do correspond
// to the symlink() system call.  The 'path' is where the link points,
// while the 'link' is the link itself.  So we need to leave the path
// unaltered, but insert the link into the mounted directory.
EXTERNC int
lemu_symlink(const char *path, const char *link)
{
  char flink[PATH_MAX];

  log_msg("\nlemu_symlink(path=\"%s\", link=\"%s\")\n",
      path, link);
  lemu_fullpath(flink, link);

  char fn[PATH_MAX], ln[PATH_MAX];
  memcpy(fn, path, strlen(path) + 1);
  memcpy(ln, flink, strlen(flink) + 1);
  BufType fname = {.sz = strlen(path) + 1, .databuf = fn},
          lname = {.sz = strlen(flink) + 1, .databuf = ln};
  LnetMsg msg(FsRequest, Symlink, sizeof(fname.sz) + sizeof(lname.sz) + strlen(path) + strlen(flink) + 2);
  msg.marshall(&fname, &lname);
  LnetMsg res(Unknown);
  auto ret = 0;
  if (LEMU_DATA->osc->sendMsgToMds(&msg)) {
    if (LEMU_DATA->osc->recvMsgFromMds(&res)) {
      assert(res.t == FsResponse);
      res.unmarshall(&ret, &errno);
    }
  }

  return log_syscall("symlink", ret, 0);
}

/** Rename a file */
// both path and newpath are fs-relative
EXTERNC int
lemu_rename(const char *path, const char *newpath)
{
  char fpath[PATH_MAX];
  char fnewpath[PATH_MAX];

  log_msg("\nlemu_rename(fpath=\"%s\", newpath=\"%s\")\n",
      path, newpath);
  lemu_fullpath(fpath, path);
  lemu_fullpath(fnewpath, newpath);

  char fn[PATH_MAX], ln[PATH_MAX];
  memcpy(fn, fpath, strlen(fpath) + 1);
  memcpy(ln, fnewpath, strlen(fnewpath) + 1);
  BufType fname = {.sz = strlen(fpath) + 1, .databuf = fn},
          lname = {.sz = strlen(fnewpath) + 1, .databuf = ln};
  LnetMsg msg(FsRequest, Rename, sizeof(fname.sz) + sizeof(lname.sz) + strlen(fpath) + strlen(fnewpath) + 2);
  msg.marshall(&fname, &lname);
  LnetMsg res(Unknown);
  auto ret = 0;
  if (LEMU_DATA->osc->sendMsgToMds(&msg)) {
    if (LEMU_DATA->osc->recvMsgFromMds(&res)) {
      assert(res.t == FsResponse);
      res.unmarshall(&ret, &errno);
    }
  }

  return log_syscall("rename", ret, 0);
}

/** Create a hard link to a file */
EXTERNC int
lemu_link(const char *path, const char *newpath)
{
  char fpath[PATH_MAX], fnewpath[PATH_MAX];

  log_msg("\nlemu_link(path=\"%s\", newpath=\"%s\")\n",
      path, newpath);
  lemu_fullpath(fpath, path);
  lemu_fullpath(fnewpath, newpath);

  char fn[PATH_MAX], ln[PATH_MAX];
  memcpy(fn, fpath, strlen(fpath) + 1);
  memcpy(ln, fnewpath, strlen(fnewpath) + 1);
  BufType fname = {.sz = strlen(fpath) + 1, .databuf = fn},
          lname = {.sz = strlen(fnewpath) + 1, .databuf = ln};
  LnetMsg msg(FsRequest, Link, sizeof(fname.sz) + sizeof(lname.sz) + strlen(fpath) + strlen(fnewpath) + 2);
  msg.marshall(&fname, &lname);
  LnetMsg res(Unknown);
  auto ret = 0;
  if (LEMU_DATA->osc->sendMsgToMds(&msg)) {
    if (LEMU_DATA->osc->recvMsgFromMds(&res)) {
      assert(res.t == FsResponse);
      res.unmarshall(&ret, &errno);
    }
  }

  return log_syscall("link", ret, 0);
}

/** Change the permission bits of a file */
EXTERNC int
lemu_chmod(const char *path, mode_t mode)
{
  char fpath[PATH_MAX];

  log_msg("\nlemu_chmod(fpath=\"%s\", mode=0%03o)\n",
      path, mode);
  lemu_fullpath(fpath, path);

  LnetMsg msg(FsRequest, Chmod, strlen(fpath) + sizeof(mode) + 1);
  msg.marshall(&mode, fpath);
  LnetMsg res(Unknown);
  auto ret = 0;
  if (LEMU_DATA->osc->sendMsgToMds(&msg)) {
    if (LEMU_DATA->osc->recvMsgFromMds(&res)) {
      assert(res.t == FsResponse);
      res.unmarshall(&ret, &errno);
    }
  }

  return log_syscall("chmod", ret, 0);
}

/** Change the owner and group of a file */
EXTERNC int
lemu_chown(const char *path, uid_t uid, gid_t gid)

{
  char fpath[PATH_MAX];

  log_msg("\nlemu_chown(path=\"%s\", uid=%d, gid=%d)\n",
      path, uid, gid);
  lemu_fullpath(fpath, path);

  LnetMsg msg(FsRequest, Chown, strlen(fpath) + sizeof(uid) + sizeof(gid) + 1);
  msg.marshall(&uid, &gid, fpath);
  LnetMsg res(Unknown);
  auto ret = 0;
  if (LEMU_DATA->osc->sendMsgToMds(&msg)) {
    if (LEMU_DATA->osc->recvMsgFromMds(&res)) {
      assert(res.t == FsResponse);
      res.unmarshall(&ret, &errno);
    }
  }

  return log_syscall("chown", ret, 0);
}

/** Change the size of a file */
EXTERNC int
lemu_truncate(const char *path, off_t newsize)
{
  char fpath[PATH_MAX];

  log_msg("\nlemu_truncate(path=\"%s\", newsize=%lld)\n",
      path, newsize);
  lemu_fullpath(fpath, path);

  LnetMsg msg(FsRequest, Truncate, strlen(fpath) + sizeof(newsize) + 1);
  msg.marshall(&newsize, fpath);
  LnetMsg res(Unknown);
  auto ret = 0;
  if (LEMU_DATA->osc->sendMsgToMds(&msg)) {
    if (LEMU_DATA->osc->recvMsgFromMds(&res)) {
      assert(res.t == FsResponse);
      res.unmarshall(&ret, &errno);
    }
  }

  return log_syscall("truncate", ret, 0);
}

/** Change the access and/or modification times of a file */
/* note -- I'll want to change this as soon as 2.6 is in debian testing */
EXTERNC int
lemu_utime(const char *path, struct utimbuf *ubuf)
{
  char fpath[PATH_MAX];

  log_msg("\nlemu_utime(path=\"%s\", ubuf=0x%08x)\n",
      path, ubuf);
  lemu_fullpath(fpath, path);

  LnetMsg msg(FsRequest, Utime, strlen(fpath) + 1);
  msg.marshall(fpath);
  LnetMsg res(Unknown);
  auto ret = 0;
  if (LEMU_DATA->osc->sendMsgToMds(&msg)) {
    if (LEMU_DATA->osc->recvMsgFromMds(&res)) {
      assert(res.t == FsResponse);
      res.unmarshall(&ret, &errno, ubuf);
    }
  }

  return log_syscall("utime", ret, 0);
}


static const char*
findPattern(const char *hay, size_t len, const char *p, size_t sz)
{
  const char *ptr = hay;
  for (size_t i = 0; i < len; i++) {
    if (ptr[i] == '\0') {
      continue;
    } else if (strncmp(ptr + i, p, sz) == 0) {
      return ptr + i;
    }
  }
  return NULL;
}

static char*
getenvByPid(const char *env, char *val, size_t len, pid_t pid)
{
  string pathname = "/proc/";
  pathname += to_string(pid);
  pathname += "/environ";
  ifstream f(pathname.c_str(), ofstream::in);
  if (f.is_open()) {
    string envvar;
    while (getline (f, envvar)) {
      size_t sz = strlen(env);
      const char *pos = findPattern(envvar.c_str(), envvar.length(), env, sz);
      if (pos != 0) {
        const char *ptr = pos + sz + 1; // + 1 for "="
        strncpy(val, ptr, len);
        return val;
      }
    }
  }
  return NULL;
}

void
setAppInfo(LnetMsg *msg)
{
  fuse_context *ctx = fuse_get_context();
  if (ctx) {
    char val[60] = {0};
    char *ptr = getenvByPid("APP_ID", val, sizeof val, ctx->pid);
    if (ptr) {
      msg->_i.id = atoi(ptr);
    }
    ptr = getenvByPid("APP_NAME", val, sizeof val, ctx->pid);
    if (ptr) {
      strncpy(msg->_i.name, ptr, sizeof msg->_i.name);
    }
    msg->_i.name[59] = 0;
  }
}

/** File open operation
 *
 * No creation, or truncation flags (O_CREAT, O_EXCL, O_TRUNC)
 * will be passed to open().  Open should check if the operation
 * is permitted for the given flags.  Optionally open may also
 * return an arbitrary filehandle in the fuse_file_info structure,
 * which will be passed to all file operations.
 *
 * Changed in version 2.2
 */
EXTERNC int
lemu_open(const char *path, struct fuse_file_info *fi)
{
  int retstat = 0;
  int fd;
  char fpath[PATH_MAX];

  log_msg("\nlemu_open(path\"%s\", fi=0x%08x)\n",
      path, fi);
  lemu_fullpath(fpath, path);

  mode_t mode = 0;
  LnetMsg msg(FsRequest, Open, sizeof(mode) + sizeof(fi->flags) + strlen(fpath) + 1);
  setAppInfo(&msg);
  msg.marshall(&fi->flags, &mode, fpath);
  LnetMsg res(Unknown);
  auto ret = 0;
  if (LEMU_DATA->osc->sendMsgToMds(&msg)) {
    if (LEMU_DATA->osc->recvMsgFromMds(&res)) {
      assert(res.t == FsResponse);
      res.unmarshall(&ret, &errno);
    }
  }

  // if the open call succeeds, my retstat is the file descriptor,
  // else it's -errno.  I'm making sure that in that case the saved
  // file descriptor is exactly -1.
  fd = log_syscall("open", ret, 0);
  if (fd < 0) {
    retstat = log_error("open");
  } else {
    LEMU_DATA->osc->insertFdToApp(fd, msg._i);
  }

  fi->fh = fd;

  log_fi(fi);

  return retstat;
}

/** Read data from an open file
 *
 * Read should return exactly the number of bytes requested except
 * on EOF or error, otherwise the rest of the data will be
 * substituted with zeroes.  An exception to this is when the
 * 'direct_io' mount option is specified, in which case the return
 * value of the read system call will reflect the return value of
 * this operation.
 *
 * Changed in version 2.2
 */
// I don't fully understand the documentation above -- it doesn't
// match the documentation for the read() system call which says it
// can return with anything up to the amount of data requested. nor
// with the fusexmp code which returns the amount of data also
// returned by read.
EXTERNC int
lemu_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
  char fpath[PATH_MAX];
  log_msg("\nlemu_read(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n",
      path, buf, size, offset, fi);
  lemu_fullpath(fpath, path);
  log_fi(fi);

  LnetMsg msg(FsRequest, Read, strlen(fpath) + 1 + sizeof(size) + sizeof(offset) + sizeof(fi->fh));
  LEMU_DATA->osc->getAppForFd(fi->fh, &msg._i);
  msg.marshall(&size, &offset, &fi->fh, fpath);
  auto ret = -1;
  LnetMsg res(Unknown);
  const OstInfo *ost = LEMU_DATA->osc->getOstFromPath(fpath);
  if (LEMU_DATA->osc->sendDatanetMsgToOst(&msg, ost->id) > 0) {
    if (LEMU_DATA->osc->recvDatanetMsgFromOst(&res, ost->id) > 0) {
      assert(res.t == FsResponse);
      res.unmarshall(&ret, &errno);
      if (ret > 0) {
        LEMU_DATA->osc->recvDataFromOst(ost->id, buf, ret);
      } else if (ret == 0) {
        ret = 0;
      }
    }
  }
  return log_syscall("pread", ret, 0);
}

/** Write data to an open file
 *
 * Write should return exactly the number of bytes requested
 * except on error.  An exception to this is when the 'direct_io'
 * mount option is specified (see read operation).
 *
 * Changed in version 2.2
 */
// As  with read(), the documentation above is inconsistent with the
// documentation for the write() system call.
EXTERNC int
lemu_write(const char *path, const char *buf, size_t size, off_t offset,
    struct fuse_file_info *fi)
{
  char fpath[PATH_MAX];
  log_msg("\nlemu_write(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n",
      path, buf, size, offset, fi
      );
  lemu_fullpath(fpath, path);
  log_fi(fi);

  LnetMsg msg(FsRequest, Write, strlen(fpath) + 1 + sizeof(size) + sizeof(offset) + sizeof(fi->fh));
  LEMU_DATA->osc->getAppForFd(fi->fh, &msg._i);
  msg.marshall(&size, &offset, &fi->fh, fpath);
  auto ret = -1;
  LnetMsg res(Unknown);
  const OstInfo *ost = LEMU_DATA->osc->getOstFromPath(fpath);
  if (LEMU_DATA->osc->sendDatanetMsgToOst(&msg, ost->id)) {
    LEMU_DATA->osc->sendDataToOst(ost->id, buf, size);
    if (LEMU_DATA->osc->recvDatanetMsgFromOst(&res, ost->id)) {
      assert(res.t == FsResponse);
      res.unmarshall(&ret, &errno);
    }
  }

  return log_syscall("pwrite", ret, 0);
}

/** Get file system statistics
 *
 * The 'f_frsize', 'f_favail', 'f_fsid' and 'f_flag' fields are ignored
 *
 * Replaced 'struct statfs' parameter with 'struct statvfs' in
 * version 2.5
 */
EXTERNC int
lemu_statfs(const char *path, struct statvfs *statv)
{
  int retstat = 0;
  char fpath[PATH_MAX];

  log_msg("\nlemu_statfs(path=\"%s\", statv=0x%08x)\n",
      path, statv);
  lemu_fullpath(fpath, path);

  // get stats for underlying filesystem
  LnetMsg msg(FsRequest, Statfs, strlen(fpath) + 1);
  msg.marshall(fpath);
  auto ret = 0;
  LnetMsg res(Unknown);
  if (LEMU_DATA->osc->sendMsgToMds(&msg)) {
    if (LEMU_DATA->osc->recvMsgFromMds(&res)) {
      assert(res.t == FsResponse);
      res.unmarshall(&ret, &errno, statv);
    }
  }
  retstat = log_syscall("statvfs", ret, 0);

  log_statvfs(statv);

  return retstat;
}

/** Possibly flush cached data
 *
 * BIG NOTE: This is not equivalent to fsync().  It's not a
 * request to sync dirty data.
 *
 * Flush is called on each close() of a file descriptor.  So if a
 * filesystem wants to return write errors in close() and the file
 * has cached dirty data, this is a good place to write back data
 * and return any errors.  Since many applications ignore close()
 * errors this is not always useful.
 *
 * NOTE: The flush() method may be called more than once for each
 * open().  This happens if more than one file descriptor refers
 * to an opened file due to dup(), dup2() or fork() calls.  It is
 * not possible to determine if a flush is final, so each flush
 * should be treated equally.  Multiple write-flush sequences are
 * relatively rare, so this shouldn't be a problem.
 *
 * Filesystems shouldn't assume that flush will always be called
 * after some writes, or that if will be called at all.
 *
 * Changed in version 2.2
 */
// this is a no-op in BBFS.  It just logs the call and returns success
EXTERNC int
lemu_flush(const char *path, struct fuse_file_info *fi)
{
  char fpath[PATH_MAX];
  log_msg("\nlemu_flush(path=\"%s\", fi=0x%08x)\n", path, fi);
  lemu_fullpath(fpath, path);

  LnetMsg msg(FsRequest, Flush, sizeof(fi->fh) + strlen(fpath) + 1);
  msg.marshall(&fi->fh, fpath);
  auto ret = 0;
  LnetMsg res(Unknown);
  if (LEMU_DATA->osc->sendMsgToMds(&msg)) {
    if (LEMU_DATA->osc->recvMsgFromMds(&res)) {
      assert(res.t == FsResponse);
      res.unmarshall(&ret, &errno);
    }
  }

  log_fi(fi);

  return ret;
}

/** Release an open file
 *
 * Release is called when there are no more references to an open
 * file: all file descriptors are closed and all memory mappings
 * are unmapped.
 *
 * For every open() call there will be exactly one release() call
 * with the same flags and file descriptor.  It is possible to
 * have a file opened more than once, in which case only the last
 * release will mean, that no more reads/writes will happen on the
 * file.  The return value of release is ignored.
 *
 * Changed in version 2.2
 */
EXTERNC int
lemu_release(const char *path, struct fuse_file_info *fi)
{
  char fpath[PATH_MAX];
  log_msg("\nlemu_release(path=\"%s\", fi=0x%08x)\n",
      path, fi);
  lemu_fullpath(fpath, path);
  log_fi(fi);

  // We need to close the file.  Had we allocated any resources
  // (buffers etc) we'd need to free them here as well.
  LnetMsg msg(FsRequest, Release, sizeof(fi->fh) + strlen(fpath) + 1);
  LEMU_DATA->osc->getAppForFd(fi->fh, &msg._i);
  msg.marshall(&fi->fh, fpath);
  auto ret = 0;
  LnetMsg res(Unknown);
  if (LEMU_DATA->osc->sendMsgToMds(&msg)) {
    if (LEMU_DATA->osc->recvMsgFromMds(&res)) {
      assert(res.t == FsResponse);
      res.unmarshall(&ret, &errno);
    }
  }
  LEMU_DATA->osc->cleanFdMap(fi->fh);
  return log_syscall("close", ret, 0);
}

/** Synchronize file contents
 *
 * If the datasync parameter is non-zero, then only the user data
 * should be flushed, not the meta data.
 *
 * Changed in version 2.2
 */
EXTERNC int
lemu_fsync(const char *path, int datasync, struct fuse_file_info *fi)
{
  char fpath[PATH_MAX];
  log_msg("\nlemu_fsync(path=\"%s\", datasync=%d, fi=0x%08x)\n",
      path, datasync, fi);
  lemu_fullpath(fpath, path);
  log_fi(fi);

  LnetMsg msg(FsRequest, Fsync, sizeof(fi->fh) + sizeof(datasync) + strlen(fpath) + 1);
  msg.marshall(&fi->fh, &datasync, fpath);
  auto ret = 0;
  LnetMsg res(Unknown);
  if (LEMU_DATA->osc->sendMsgToMds(&msg)) {
    if (LEMU_DATA->osc->recvMsgFromMds(&res)) {
      assert(res.t == FsResponse);
      res.unmarshall(&ret, &errno);
    }
  }

  return log_syscall("fsync", ret, 0);
}

/** Note that my implementations of the various xattr functions use
  the 'l-' versions of the functions (eg lemu_setxattr() calls
  lsetxattr() not setxattr(), etc).  This is because it appears any
  symbolic links are resolved before the actual call takes place, so
  I only need to use the system-provided calls that don't follow
  them */

/** Set extended attributes */
EXTERNC int
lemu_setxattr(const char *path, const char *name, const char *value, size_t size, int flags)
{
  char fpath[PATH_MAX];

  log_msg("\nlemu_setxattr(path=\"%s\", name=\"%s\", value=\"%s\", size=%d, flags=0x%08x)\n",
      path, name, value, size, flags);
  lemu_fullpath(fpath, path);

  if (areFilepathsEqual(fpath, LEMU_DATA->rootdir)) {
    return log_syscall("lsetxattr on rootdir", ::lsetxattr(fpath, name, value, size, flags), 0);
  }

  char fn[PATH_MAX], ln[PATH_MAX], vn[size + 1];
  memcpy(fn, fpath, strlen(fpath) + 1);
  memcpy(ln, name, strlen(name) + 1);
  memcpy(vn, value, size);
  BufType fname = {.sz = strlen(fpath) + 1, .databuf = fn},
          lname = {.sz = strlen(name) + 1, .databuf = ln},
          vname = {.sz = size, .databuf = vn};
  LnetMsg msg(FsRequest, Setxattr, sizeof(size) + sizeof(flags) + sizeof(fname.sz) + sizeof(lname.sz) + sizeof(vname.sz) + strlen(fpath) + strlen(name) + size + 2);
  msg.marshall(&size, &flags, &fname, &lname, &vname);
  LnetMsg res(Unknown);
  int ret = 0;
  if (LEMU_DATA->osc->sendMsgToMds(&msg)) {
    if (LEMU_DATA->osc->recvMsgFromMds(&res)) {
      assert(res.t == FsResponse);
      res.unmarshall(&ret, &errno);
    }
  }

  return log_syscall("lsetxattr", ret, 0);
}

/** Get extended attributes */
EXTERNC int
lemu_getxattr(const char *path, const char *name, char *value, size_t size)
{
  int retstat = 0;
  char fpath[PATH_MAX];

  log_msg("\nlemu_getxattr(path = \"%s\", name = \"%s\", value = 0x%08x, size = %d)\n",
      path, name, value, size);
  lemu_fullpath(fpath, path);

  if (areFilepathsEqual(fpath, LEMU_DATA->rootdir)) {
    return log_syscall("lgetxattr on rootdir", ::lgetxattr(fpath, name, value, size), 0);
  }

  char fn[PATH_MAX], ln[PATH_MAX];
  memcpy(fn, fpath, strlen(fpath) + 1);
  memcpy(ln, name, strlen(name) + 1);
  BufType fname = {.sz = strlen(fpath) + 1, .databuf = fn},
          lname = {.sz = strlen(name) + 1, .databuf = ln};
  LnetMsg msg(FsRequest, Getxattr, sizeof(size) + sizeof(fname.sz) + sizeof(lname.sz) + strlen(fpath) + strlen(name) + 2);
  msg.marshall(&size, &fname, &lname);
  LnetMsg res(Unknown);
  ssize_t ret = 0;
  BufType mybuf = {.sz = size, .databuf = value};
  if (LEMU_DATA->osc->sendMsgToMds(&msg)) {
    if (LEMU_DATA->osc->recvMsgFromMds(&res)) {
      assert(res.t == FsResponse);
      res.unmarshall(&ret, &errno, &mybuf);
    }
  }

  retstat = log_syscall("lgetxattr", ret, 0);
  if (retstat >= 0)
    log_msg("    value = \"%s\"\n", value);

  return retstat;
}

/** List extended attributes */
EXTERNC int
lemu_listxattr(const char *path, char *list, size_t size)
{
  int retstat = 0;
  char fpath[PATH_MAX];
  char *ptr;

  log_msg("\nlemu_listxattr(path=\"%s\", list=0x%08x, size=%d)\n",
      path, list, size
      );
  lemu_fullpath(fpath, path);

  if (areFilepathsEqual(fpath, LEMU_DATA->rootdir)) {
    return log_syscall("listxattr on rootdir", ::llistxattr(fpath, list, size), 0);
  }

  LnetMsg msg(FsRequest, Listxattr, strlen(fpath) + sizeof(size) + 1);
  msg.marshall(&size, fpath);
  LnetMsg res(Unknown);
  BufType mybuf = {.sz = size, .databuf = list};
  ssize_t ret = 0;
  if (LEMU_DATA->osc->sendMsgToMds(&msg)) {
    if (LEMU_DATA->osc->recvMsgFromMds(&res)) {
      assert(res.t == FsResponse);
      res.unmarshall(&ret, &errno, &mybuf);
    }
  }

  retstat = log_syscall("llistxattr", ret, 0);
  if (retstat >= 0) {
    log_msg("    returned attributes (length %d):\n", retstat);
    if (list != NULL)
      for (ptr = list; ptr < list + retstat; ptr += strlen(ptr)+1)
        log_msg("    \"%s\"\n", ptr);
    else
      log_msg("    (null)\n");
  }

  return retstat;
}

/** Remove extended attributes */
EXTERNC int
lemu_removexattr(const char *path, const char *name)
{
  char fpath[PATH_MAX];

  log_msg("\nlemu_removexattr(path=\"%s\", name=\"%s\")\n",
      path, name);
  lemu_fullpath(fpath, path);

  if (areFilepathsEqual(fpath, LEMU_DATA->rootdir)) {
    return log_syscall("removexattr on rootdir", ::lremovexattr(fpath, name), 0);
  }

  return -1;
}

/** Open directory
 *
 * This method should check if the open operation is permitted for
 * this  directory
 *
 * Introduced in version 2.3
 */
int lemu_opendir(const char *path, struct fuse_file_info *fi)
{
  DIR *dp = NULL;
  char fpath[PATH_MAX];

  log_msg("\nlemu_opendir(path=\"%s\", fi=0x%08x)\n",
      path, fi);
  lemu_fullpath(fpath, path);

  if (areFilepathsEqual(fpath, LEMU_DATA->rootdir)) {
    return handleRootDir(path, NULL, NULL, 0, fi, Opendir);
  }

  LnetMsg msg(FsRequest, Opendir, strlen(fpath) + 1);
  msg.marshall(fpath);
  LnetMsg res(Unknown);
  auto ret = 0;
  if (LEMU_DATA->osc->sendMsgToMds(&msg)) {
    if (LEMU_DATA->osc->recvMsgFromMds(&res)) {
      assert(res.t == FsResponse);
      res.unmarshall(&dp, &errno);
    }
  }
  if (dp == NULL)
     ret = log_error("lemu_opendir opendir");

  fi->fh = (intptr_t) dp;

  log_fi(fi);

  return ret;
}

EXTERNC int
lemu_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset,
    struct fuse_file_info *fi)
{
  int retstat = 0;
  DIR *dp;
  struct dirent *de;
  char fpath[PATH_MAX];

  log_msg("\nlemu_readdir(path=\"%s\", buf=0x%08x, filler=0x%08x, offset=%lld, fi=0x%08x)\n",
      path, buf, filler, offset, fi);
  lemu_fullpath(fpath, path);

  if (areFilepathsEqual(fpath, LEMU_DATA->rootdir)) {
    return handleRootDir(path, buf, filler, offset, fi, Readdir);
  }

  dp = (DIR *) (uintptr_t) fi->fh;

  LnetMsg msg(FsRequest, Readdir, sizeof(dp) + strlen(fpath) + 1);
  msg.marshall(&dp, fpath);
  LnetMsg res(Unknown);
  if (LEMU_DATA->osc->sendMsgToMds(&msg)) {
    if (LEMU_DATA->osc->recvMsgFromMds(&res)) {
      assert(res.t == FsResponse);
      size_t count = 0;
      size_t offset = 0;
      res.extractData(&de, offset);
      res.extractData(&errno, offset);
      res.extractData(&count, offset);
      for (size_t i = 0; i < count; i++) {
        struct dirent des;
        res.extractData(&des, offset);
        if (filler(buf, des.d_name, NULL, 0) != 0) {
          log_msg("    ERROR lemu_readdir filler:  buffer full");
          return -ENOMEM;
        }
      }
    }
  }

  log_fi(fi);

  return retstat;
}

/** Release directory
 *
 * Introduced in version 2.3
 */
EXTERNC int
lemu_releasedir(const char *path, struct fuse_file_info *fi)
{
  char fpath[PATH_MAX];
  int retstat = 0;

  log_msg("\nlemu_releasedir(path=\"%s\", fi=0x%08x)\n",
      path, fi);
  log_fi(fi);
  lemu_fullpath(fpath, path);

  if (areFilepathsEqual(fpath, LEMU_DATA->rootdir)) {
    return handleRootDir(path, NULL, NULL, 0, fi, Releasedir);
  }

  LnetMsg msg(FsRequest, Releasedir, sizeof(fi->fh) + strlen(fpath) + 1);
  msg.marshall(&fi->fh, fpath);
  LnetMsg res(Unknown);
  if (LEMU_DATA->osc->sendMsgToMds(&msg)) {
    if (LEMU_DATA->osc->recvMsgFromMds(&res)) {
      assert(res.t == FsResponse);
      res.unmarshall(&retstat, &errno);
    }
  }

  return retstat;
}

/** Synchronize directory contents
 *
 * If the datasync parameter is non-zero, then only the user data
 * should be flushed, not the meta data
 *
 * Introduced in version 2.3
 */
// when exactly is this called?  when a user calls fsync and it
// happens to be a directory? ??? >>> I need to implement this...
EXTERNC int
lemu_fsyncdir(const char *path, int datasync, struct fuse_file_info *fi)
{
  int retstat = 0;

  log_msg("\nlemu_fsyncdir(path=\"%s\", datasync=%d, fi=0x%08x)\n",
      path, datasync, fi);
  log_fi(fi);

  return retstat;
}

// Initialize filesystem mount
EXTERNC void*
lemu_init(struct fuse_conn_info *conn)
{
  log_msg("\nlemu_init()\n");

  log_conn(conn);
#ifndef TEST_ENV
  log_fuse_context(fuse_get_context());
#endif // ifndef TEST_ENV
  if (!LEMU_DATA->osc) {
    log_error(__FUNCTION__);
  }
  // LEMU_DATA->osc->createDirStructure(LEMU_DATA->rootdir);
  return LEMU_DATA;
}

// Unmount filesystem
EXTERNC void
lemu_destroy(void *userdata)
{
  log_msg("\nlemu_destroy(userdata=0x%08x)\n", userdata);
}

// access() system call
EXTERNC int
lemu_access(const char *path, int mask)
{
  char fpath[PATH_MAX];

  log_msg("\nlemu_access(path=\"%s\", mask=0%o)\n",
      path, mask);
  lemu_fullpath(fpath, path);

  LnetMsg msg(FsRequest, Access, strlen(fpath) + sizeof(mask) + 1);
  msg.marshall(&mask, fpath);
  LnetMsg res(Unknown);
  auto ret = 0;
  if (LEMU_DATA->osc->sendMsgToMds(&msg)) {
    if (LEMU_DATA->osc->recvMsgFromMds(&res)) {
      assert(res.t == FsResponse);
      res.unmarshall(&ret, &errno);
    }
  }
  if (ret < 0)
     ret = log_error("lemu_access access");

  return ret;
}

/**
 * Create and open a file
 *
 * If the file does not exist, first create it with the specified
 * mode, and then open it.
 *
 * If this method is not implemented or under Linux kernel
 * versions earlier than 2.6.15, the mknod() and open() methods
 * will be called instead.
 *
 * Introduced in version 2.5
 */
// Not implemented.  I had a version that used creat() to create and
// open the file, which it turned out opened the file write-only.

/**
 * Change the size of an open file
 *
 * This method is called instead of the truncate() method if the
 * truncation was invoked from an ftruncate() system call.
 *
 * If this method is not implemented or under Linux kernel
 * versions earlier than 2.6.15, the truncate() method will be
 * called instead.
 *
 * Introduced in version 2.5
 */
EXTERNC int
lemu_ftruncate(const char *path, off_t offset, struct fuse_file_info *fi)
{
  int retstat = 0;

  log_msg("\nlemu_ftruncate(path=\"%s\", offset=%lld, fi=0x%08x)\n",
      path, offset, fi);
  log_fi(fi);

  retstat = ftruncate(fi->fh, offset);
  if (retstat < 0)
    retstat = log_error("lemu_ftruncate ftruncate");

  return retstat;
}

/**
 * Get attributes from an open file
 *
 * This method is called instead of the getattr() method if the
 * file information is available.
 *
 * Currently this is only called after the create() method if that
 * is implemented (see above).  Later it may be called for
 * invocations of fstat() too.
 *
 * Introduced in version 2.5
 */
EXTERNC int
lemu_fgetattr(const char *path, struct stat *statbuf, struct fuse_file_info *fi)
{
  char fpath[PATH_MAX];

  log_msg("\nlemu_fgetattr(path=\"%s\", statbuf=0x%08x, fi=0x%08x)\n",
      path, statbuf, fi);
  log_fi(fi);
  lemu_fullpath(fpath, path);

  LnetMsg msg(FsRequest, Fgetattr, sizeof(fi->fh) + strlen(fpath) + 1);
  msg.marshall(&fi->fh, fpath);
  auto ret = 0;
  LnetMsg res(Unknown);
  if (LEMU_DATA->osc->sendMsgToMds(&msg) > 0) {
    if (LEMU_DATA->osc->recvMsgFromMds(&res) > 0) {
      assert(res.t == FsResponse);
      res.unmarshall(&ret, &errno, statbuf);
    }
  }

  // retstat = fstat(fi->fh, statbuf);
  if (ret < 0)
    ret = log_error("lemu_fgetattr fstat");

  log_stat(statbuf);

  return ret;
}

EXTERNC int
lemu_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
  log_msg("Call: %s\n", __FUNCTION__);
  int ret = lemu_mknod(path, mode, 0);
  if (ret == 0) {
    ret = lemu_open(path, fi);
  }
  return ret;
}

EXTERNC int
lemu_lock(const char *path, struct fuse_file_info *fi, int cmd, struct flock *fl)
{
  // TODO
  log_msg("Unimplemented call: %s\n", __FUNCTION__);
  return -1;
}

EXTERNC int
lemu_utimens(const char *path, const struct timespec tv[2])
{
  struct utimbuf ubuf = {0};
  log_msg("\nlemu_utimens(path=\"%s\")\n",
      path);
  // times[0] specifies the new  "last access time" (atime);
  // times[1] specifies the new "last modification time" (mtime)
  ubuf.actime = tv[0].tv_sec;
  ubuf.modtime = tv[1].tv_sec;
  return lemu_utime(path, &ubuf);
}

EXTERNC int
lemu_bmap(const char *path, size_t blocksize, uint64_t *idx)
{
  // TODO
  log_msg("Unimplemented call: %s\n", __FUNCTION__);
  return -1;
}

EXTERNC int
lemu_getdir(const char *path, fuse_dirh_t dirh, fuse_dirfil_t filler)
{
  // TODO
  log_msg("Unimplemented call: %s\n", __FUNCTION__);
  return -1;
}

EXTERNC int
lemu_ioctl(const char *path, int cmd, void *arg, struct fuse_file_info *fi, unsigned int flags, void *data)
{
  // TODO
  log_msg("Unimplemented call: %s\n", __FUNCTION__);
  return -1;
}

EXTERNC int lemu_poll(const char *path, struct fuse_file_info *fi, struct fuse_pollhandle *ph, unsigned *reventsp)
{
  // TODO
  log_msg("Unimplemented call: %s\n", __FUNCTION__);
  return -1;
}

EXTERNC int lemu_write_buf(const char *path, struct fuse_bufvec *buf, off_t off, struct fuse_file_info *fi)
{
  // TODO
  log_msg("Unimplemented call: %s\n", __FUNCTION__);
  return -1;
}

EXTERNC int lemu_read_buf(const char *path, struct fuse_bufvec **bufp, size_t size, off_t off, struct fuse_file_info *fi)
{
  // TODO
  log_msg("Unimplemented call: %s\n", __FUNCTION__);
  return -1;
}

EXTERNC int lemu_flock(const char *path, struct fuse_file_info *fi, int op)
{
  // TODO
  log_msg("Unimplemented call: %s\n", __FUNCTION__);
  return -1;
}

EXTERNC int lemu_fallocate(const char *path, int, off_t off1, off_t off2, struct fuse_file_info *fi)
{
  // TODO
  log_msg("Unimplemented call: %s\n", __FUNCTION__);
  return -1;
}
