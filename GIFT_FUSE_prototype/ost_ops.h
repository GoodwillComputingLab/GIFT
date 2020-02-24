#ifndef _OST_OPS_H
#define _OST_OPS_H

#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "util.h"
#include "ost.h"

class OstOps
{
  public:
    // Data API
    static void ost_read(const DatanetOst* , const LnetEntity* , const LnetMsg* );
    static void ost_write(const DatanetOst* , const LnetEntity* , const LnetMsg* );

    // Metadata API
    static void ost_mkdir(const LnetOst* , const LnetEntity* , const LnetMsg* );
    static void ost_access(const LnetOst* , const LnetEntity* , const LnetMsg* );
    static void ost_opendir(const LnetOst* , const LnetEntity* , const LnetMsg* );
    static void ost_releasedir(const LnetOst* , const LnetEntity* , const LnetMsg* );
    static void ost_getattr(const LnetOst* , const LnetEntity* , const LnetMsg* );
    static void ost_fgetattr(const LnetOst* , const LnetEntity* , const LnetMsg* );
    static void ost_mknod(const LnetOst* , const LnetEntity* , const LnetMsg* );
    static void ost_unlink(const LnetOst* , const LnetEntity* , const LnetMsg* );
    static void ost_rmdir(const LnetOst* , const LnetEntity* , const LnetMsg* );
    static void ost_chmod(const LnetOst* , const LnetEntity* , const LnetMsg* );
    static void ost_chown(const LnetOst* , const LnetEntity* , const LnetMsg* );
    static void ost_truncate(const LnetOst* , const LnetEntity* , const LnetMsg* );
    static void ost_utime(const LnetOst* , const LnetEntity* , const LnetMsg* );
    static void ost_open(const LnetOst* , const LnetEntity* , const LnetMsg* );
    static void ost_statfs(const LnetOst* , const LnetEntity* , const LnetMsg* );
    static void ost_flush(const LnetOst* , const LnetEntity* , const LnetMsg* );
    static void ost_release(const LnetOst* , const LnetEntity* , const LnetMsg* );
    static void ost_fsync(const LnetOst* , const LnetEntity* , const LnetMsg* );
    static void ost_readlink(const LnetOst* , const LnetEntity* , const LnetMsg* );
    static void ost_symlink(const LnetOst* , const LnetEntity* , const LnetMsg* );
    static void ost_rename(const LnetOst* , const LnetEntity* , const LnetMsg* );
    static void ost_link(const LnetOst* , const LnetEntity* , const LnetMsg* );
    static void ost_readdir(const LnetOst* , const LnetEntity* , const LnetMsg* );
    static void ost_getxattr(const LnetOst* , const LnetEntity* , const LnetMsg* );
    static void ost_setxattr(const LnetOst* , const LnetEntity* , const LnetMsg* );
    static void ost_listxattr(const LnetOst* , const LnetEntity* , const LnetMsg* );

    // Other
    static void startRwThread(const LnetOst* , int , const LnetEntity* , AppInfo );
};

// Used during mount
// lemu_access
// lemu_getattr
// lemu_init
// lemu_opendir
// lemu_readdir
// lemu_releasedir

// At umount
// lemu_destroy

// Metadata API

int ost_fsyncdir(const char *path, int datasync);
int ost_ftruncate(const char *path, off_t offset);
int ost_fgetattr(const char *path, struct stat *statbuf);

#endif // ifndef _OST_OPS_H
