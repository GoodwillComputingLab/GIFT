#include "config.h"
#include "params.h"

#include <cassert>
#include <cstring>
#include <fuse.h>
#include <iostream>
#include <string>
#include <unistd.h>
#include <sys/types.h>

#include "log.h"
#include "lnet.h"
#include "osc.h"
#include "osc_ops.h"

#define shift argc--; argv++

using namespace std;

void
lemu_usage()
{
  cerr << "Usage: osc [mds-hostname] [mds-port] [osc-id] [osc-name] [rootDir] [mountPoint]" << endl;
}

int
main(int argc, char *argv[])
{
  int fuse_stat;
  struct lemu_state *lemu_data;

  if ((getuid() == 0) || (geteuid() == 0)) {
    cerr << "Do not run as root" << endl;
    return -1;
  }

  // See which version of fuse we're running
  cerr << "Fuse version: "<< FUSE_MAJOR_VERSION << "." << FUSE_MINOR_VERSION << endl;

  if (argc < 6) {
    lemu_usage();
    return -1;
  }
  const char *hostname = argv[1];
  int port = atoi(argv[2]);
  int id = atoi(argv[3]);
  const char *name = argv[4];

  LSockAddr addr(hostname, port);
  OSC *osc = new OSC(addr, port, id, name);
  if (!osc->start()) return -1;

  lemu_data = new lemu_state;
  if (!lemu_data) {
    perror("main malloc");
    return -1;
  }

  shift;
  shift;
  shift;
  shift;
  shift;
  // fuse_main wants rootdir to be arg[0]
  const char *rootdir = argv[0];
  lemu_data->rootdir = realpath(rootdir, NULL);
  lemu_data->logfile = log_open();
  lemu_data->osc = osc;

  cerr << "Entering fuse_main" << endl;
  fuse_stat = fuse_main(argc, argv, &lemu_oper, lemu_data);
  cerr << "fuse_main returned " << fuse_stat << endl;

  delete lemu_data;
  delete osc;
  return fuse_stat;
}
