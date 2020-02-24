#include <iostream>
#include <unistd.h>
#include <thread>

#include "ost.h"

using namespace std;

static void
printUsage()
{
  cerr << "Usage: ost [mds-host] [mds-port] [ost-id] [ost-name] [ost-lnetport] [ost-dataport]" << endl;
}

int
main(int argc, char *argv[])
{
  if (argc < 6) {
    printUsage();
    return -1;
  }
  const char *hostname = argv[1];
  int port = atoi(argv[2]);
  int id = atoi(argv[3]);
  const char *name = argv[4];
  int lnetport = atoi(argv[5]);
  int dataport = atoi(argv[6]);

  LSockAddr addr(hostname, port);
  OST *ost = new OST(addr, port, id, name, lnetport, dataport);
  ost->eventLoop();
  delete ost;
  return 0;
}
