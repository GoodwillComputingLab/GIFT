#ifndef _OSC_OPS_H
#define _OSC_OPS_H

#include <ctype.h>
#include <unistd.h>
#include <sys/types.h>
#include <fuse.h>

#include "osc.h"

extern struct fuse_operations lemu_oper;

EXTERNC int lemu_getattr(const char *path, struct stat *statbuf);
EXTERNC int lemu_readlink(const char *path, char *link, size_t size);
EXTERNC int lemu_mknod(const char *path, mode_t mode, dev_t dev);
EXTERNC int lemu_mkdir(const char *path, mode_t mode);
EXTERNC int lemu_unlink(const char *path);
EXTERNC int lemu_rmdir(const char *path);
EXTERNC int lemu_symlink(const char *path, const char *link);
EXTERNC int lemu_rename(const char *path, const char *newpath);
EXTERNC int lemu_link(const char *path, const char *newpath);
EXTERNC int lemu_chmod(const char *path, mode_t mode);
EXTERNC int lemu_chown(const char *path, uid_t uid, gid_t gid);
EXTERNC int lemu_truncate(const char *path, off_t newsize);
EXTERNC int lemu_utime(const char *path, struct utimbuf *ubuf);
EXTERNC int lemu_open(const char *path, struct fuse_file_info *fi);
EXTERNC int lemu_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi);
EXTERNC int lemu_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi);
EXTERNC int lemu_statfs(const char *path, struct statvfs *statv);
EXTERNC int lemu_flush(const char *path, struct fuse_file_info *fi);
EXTERNC int lemu_release(const char *path, struct fuse_file_info *fi);
EXTERNC int lemu_fsync(const char *path, int datasync, struct fuse_file_info *fi);
EXTERNC int lemu_setxattr(const char *path, const char *name, const char *value, size_t size, int flags);
EXTERNC int lemu_getxattr(const char *path, const char *name, char *value, size_t size);
EXTERNC int lemu_listxattr(const char *path, char *list, size_t size);
EXTERNC int lemu_removexattr(const char *path, const char *name);
EXTERNC int lemu_opendir(const char *path, struct fuse_file_info *fi);
EXTERNC int lemu_releasedir(const char *path, struct fuse_file_info *fi);
EXTERNC int lemu_fsyncdir(const char *path, int datasync, struct fuse_file_info *fi);
EXTERNC int lemu_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi);
EXTERNC int lemu_releasedir(const char *path, struct fuse_file_info *fi);
EXTERNC int lemu_fsyncdir(const char *path, int datasync, struct fuse_file_info *fi);
EXTERNC void* lemu_init(struct fuse_conn_info *conn);
EXTERNC void lemu_destroy(void *userdata);
EXTERNC int lemu_access(const char *path, int mask);
EXTERNC int lemu_ftruncate(const char *path, off_t offset, struct fuse_file_info *fi);
EXTERNC int lemu_fgetattr(const char *path, struct stat *statbuf, struct fuse_file_info *fi);

EXTERNC int lemu_setxattr(const char *path, const char *name, const char *value, size_t size, int flags);
EXTERNC int lemu_getxattr(const char *path, const char *name, char *value, size_t size);
EXTERNC int lemu_listxattr(const char *path, char *list, size_t size);
EXTERNC int lemu_removexattr(const char *path, const char *name);

EXTERNC int lemu_getdir(const char *, fuse_dirh_t, fuse_dirfil_t);
EXTERNC int lemu_create(const char *, mode_t , struct fuse_file_info *);
EXTERNC int lemu_lock(const char *, struct fuse_file_info *, int cmd, struct flock *);
EXTERNC int lemu_utimens(const char *, const struct timespec tv[2]);
EXTERNC int lemu_bmap(const char *, size_t blocksize, uint64_t *idx);
EXTERNC int lemu_ioctl(const char *, int cmd, void *arg, struct fuse_file_info *, unsigned int flags, void *data);
EXTERNC int lemu_poll(const char *, struct fuse_file_info *, struct fuse_pollhandle *ph, unsigned *reventsp);
EXTERNC int lemu_write_buf(const char *, struct fuse_bufvec *buf, off_t off, struct fuse_file_info *);
EXTERNC int lemu_read_buf(const char *, struct fuse_bufvec **bufp, size_t size, off_t off, struct fuse_file_info *);
EXTERNC int lemu_flock(const char *, struct fuse_file_info *, int op);
EXTERNC int lemu_fallocate(const char *, int, off_t, off_t, struct fuse_file_info *);

#endif // ifndef _OSC_OPS_H
