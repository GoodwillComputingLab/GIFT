#define CATCH_CONFIG_MAIN

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <string>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <sys/xattr.h>
#include <unistd.h>

#include "catch.hpp"
#include "subprocess.hpp"

struct lemu_state *lemu_data;

#include "osc.h"
#include "osc_ops.h"
#include "params.h"

using namespace subprocess;
using namespace std;

class SafeDirectory
{
  private:
    string _dname;

    int removeDir(const char *path)
    {
      DIR *d = opendir(path);
      size_t path_len = strlen(path);
      int r = -1;

      if (d) {
        struct dirent *p;
        r = 0;
        while (!r && (p=readdir(d))) {
          int r2 = -1;
          char *buf;
          size_t len;

          /* Skip the names "." and ".." as we don't want to recurse on them. */
          if (!strcmp(p->d_name, ".") || !strcmp(p->d_name, "..")) {
            continue;
          }

          len = path_len + strlen(p->d_name) + 2;
          buf = new char[len];

          if (buf) {
            struct stat statbuf;
            snprintf(buf, len, "%s/%s", path, p->d_name);

            if (!lstat(buf, &statbuf)) {
              if (S_ISDIR(statbuf.st_mode)) {
                r2 = this->removeDir(buf);
              } else {
                r2 = unlink(buf);
              }
            }
            delete buf;
          }
          r = r2;
        }
        closedir(d);
      }
      if (!r) {
        r = rmdir(path);
      }
      return r;
    }

  public:
    SafeDirectory(string dname)
    {
      this->_dname = dname;
      bool expr = (mkdir(this->_dname.c_str(), 0755) == 0) || errno == EEXIST;
      REQUIRE(expr);
    }

    ~SafeDirectory()
    {
      this->removeDir();
    }

    int removeDir()
    {
      return this->removeDir(this->_dname.c_str());
    }
};

static int
dirCleanup(SafeDirectory *d)
{
  return d->removeDir();
}

static void
pcleanup(Popen *ptr)
{
  ptr->kill(2);
  ptr->wait();
}

int myfiller(void *buf, const char *name, const struct stat *stbuf, off_t off)
{
  std::cerr << name << std::endl;
  return 0;
}

TEST_CASE("Test OSC FS implementations", "[OSC]")
{
  typedef unique_ptr<Popen, decltype(&pcleanup)> SafeProcess;
  typedef unique_ptr<SafeDirectory, decltype(&dirCleanup)> SafeDir;

  const char *hostname = "localhost";
  const int port = 7779;
  const int id = 0;
  const char *name = "c1";
  const char *mdname = "./mountdir";
  string pwd = get_current_dir_name();
  string mdsbin = pwd + "/mds";
  string ostbin = pwd + "/ost";
  string rootdir = "./rootdir/";

  SafeDir cd(new SafeDirectory(mdname), &dirCleanup);
  SafeDir sd(new SafeDirectory(rootdir.c_str()), &dirCleanup);
  SafeProcess mds(new Popen({mdsbin.c_str(), "7779"}, output("mds_out.log"), error("mds_out.log")), &pcleanup);
  SafeProcess ost(new Popen({ostbin.c_str(), "localhost", "7779", "0", "s1", "7000", "8000"}, output("ost_out.log"), error("ost_out.log")), &pcleanup);
  ::sleep(1); // Give mds and ost some time to set up and connect

  LSockAddr addr(hostname, port);
  auto *osc = new OSC(addr, port, id, name);
  REQUIRE (osc->start());

  lemu_data = new lemu_state;
  REQUIRE(lemu_data != NULL);

  lemu_data->rootdir = realpath(rootdir.c_str(), NULL);
  lemu_data->logfile = NULL;
  lemu_data->osc = osc;

  SECTION("Test mkdir", "[mkdir]")
  {
    REQUIRE(lemu_mkdir("/s1", 0755) == 0);
    REQUIRE(lemu_mkdir("/s1/testdir", 0755) == 0);
    SECTION("Test access after mkdir", "[access]")
    {
      REQUIRE(lemu_access("/s1", F_OK) == 0);
      REQUIRE(lemu_access("/s1/testdir", F_OK | R_OK | W_OK | X_OK) == 0);
      SECTION("Test opendir after mkdir", "[opendir]")
      {
        struct fuse_file_info fi = {0};
        REQUIRE(lemu_opendir("/s1/testdir", &fi) == 0);
        REQUIRE(fi.fh != 0);
        SECTION("Test releasedir after opendir", "[releasedir]")
        {
          REQUIRE(lemu_releasedir("/s1/testdir", &fi) == 0);
        }
      }
      SECTION("Test getattr after mkdir", "[getattr]")
      {
        struct stat statbuf = {0};
        REQUIRE(lemu_getattr("/s1/testdir", &statbuf) == 0);
        REQUIRE(S_ISDIR(statbuf.st_mode));
      }
    }
    SECTION("Test mknod after mkdir", "[mknod]")
    {
      REQUIRE(lemu_mknod("/s1/testdir/file.txt", S_IFREG | 0777, 0) == 0);
      SECTION("Test open after mknod", "[open]")
      {
        struct fuse_file_info fi = {0};
        fi.flags = O_RDWR;
        REQUIRE(lemu_open("/s1/testdir/file.txt", &fi) == 0);
        REQUIRE(fi.fh > 0);
        SECTION("Test flush after open", "[release]")
        {
          REQUIRE(lemu_flush("/s1/testdir/file.txt", &fi) == 0);
        }
        SECTION("Test write after open", "[write]")
        {
          char buf[] = "hello world\n";
          REQUIRE(lemu_write("/s1/testdir/file.txt", buf, sizeof(buf), 0, &fi) == sizeof(buf));
          SECTION("Test read after write", "[read]")
          {
            char rbuf[sizeof(buf) + 1] = {0};
            REQUIRE(lemu_read("/s1/testdir/file.txt", rbuf, sizeof(buf), 0, &fi) == sizeof(buf));
            REQUIRE(strncmp(buf, rbuf, sizeof(buf)) == 0);
          }
        }
        SECTION("Test fsync after open", "[fsync]")
        {
          REQUIRE(lemu_fsync("/s1/testdir/file.txt", 1, &fi) == 0);
          REQUIRE(lemu_fsync("/s1/testdir/file.txt", 0, &fi) == 0);
        }
        SECTION("Test flush after open", "[flush]")
        {
          REQUIRE(lemu_flush("/s1/testdir/file.txt", &fi) == 0);
        }
        SECTION("Test release after open", "[release]")
        {
          REQUIRE(lemu_release("/s1/testdir/file.txt", &fi) == 0);
        }
      }
      SECTION("Test statfs after mknod", "[statfs]")
      {
        struct statvfs statbuf = {0};
        REQUIRE(lemu_statfs("/s1/testdir/file.txt", &statbuf) == 0);
        // TODO: check statbuf
      }
      SECTION("Test chmod after mknod", "[chmod]")
      {
        REQUIRE(lemu_chmod("/s1/testdir/file.txt", 0766) == 0);
      }
      SECTION("Test chown after mknod", "[chown]")
      {
        REQUIRE(lemu_chown("/s1/testdir/file.txt", getuid(), getgid()) == 0);
      }
      SECTION("Test unlink after mknod", "[unlink]")
      {
        REQUIRE(lemu_unlink("/s1/testdir/file.txt") == 0);
      }
      SECTION("Test rename after mknod", "[rename]")
      {
        REQUIRE(lemu_rename("/s1/testdir/file.txt", "/s1/testdir/newfile.txt") == 0);
        SECTION("Test symlink after rename", "[symlink]")
        {
          REQUIRE(lemu_symlink("/s1/testdir/newfile.txt", "/s1/testdir/filelink.txt") == 0);
          SECTION("Test readlink after symlink", "[readlink]")
          {
            char ln[PATH_MAX];
            REQUIRE(lemu_readlink("/s1/testdir/filelink.txt", ln, sizeof(ln)) == 0);
            REQUIRE(strncmp(ln, "/s1/testdir/newfile.txt", sizeof(ln)) == 0);
          }
          SECTION("Test readdir after symlink", "[readdir]")
          {
            struct fuse_file_info fi = {0};
            REQUIRE(lemu_opendir("/s1/testdir/", &fi) == 0);
            REQUIRE(fi.fh != 0);
            char buf[sizeof(dirent) * 5] = {0};
            REQUIRE(lemu_readdir("/s1/testdir", buf, &myfiller, 0, &fi) == 0);
          }
        }
      }
      SECTION("Test setxattr after mknod", "[setxattr]")
      {
        int ret = lemu_setxattr("/s1/testdir/file.txt", "type", "txt", 4, 0);
        bool expr = ret == 0 || ret == -ENOTSUP;
        REQUIRE(expr);
        if (ret != -ENOTSUP) {
          SECTION("Test getxattr after setxattr", "[getxattr]")
          {
            char attr[4] = {0};
            REQUIRE(lemu_getxattr("/s1/testdir/file.txt", "type", attr, 4) == 0);
            REQUIRE(strncmp(attr, "txt", sizeof(attr)) == 0);
          }
        }
      }
      SECTION("Test listxattr after mknod", "[listxattr]")
      {
        char list[PATH_MAX] = {0};
        REQUIRE(lemu_listxattr("/s1/testdir/file.txt", list, sizeof(list)) == 0);
      }
    }
    SECTION("Test readdir on rootdir", "[readdir]")
    {
      struct fuse_file_info fi = {0};
      char buf[sizeof(dirent) * 5] = {0};
      REQUIRE(lemu_opendir("/", &fi) == 0);
      REQUIRE(fi.fh != 0);
      REQUIRE(lemu_readdir("/", buf, &myfiller, 0, &fi) == 0);
      REQUIRE(lemu_releasedir("/", &fi) == 0);
    }
    SECTION("Test rmdir after mkdir", "[rmdir]")
    {
      REQUIRE(lemu_rmdir("/s1/testdir") == 0);
    }
  }
  delete lemu_data;
  delete osc;
}