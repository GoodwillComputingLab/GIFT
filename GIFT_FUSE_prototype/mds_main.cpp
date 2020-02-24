#include <iostream>
#include <thread>
#include <unistd.h>
#include <signal.h>

#include "mds.h"

const int DEFAULT_PORT = 7779;
static MDS *mds;

static void
signal_handler(int signum)
{
  std::cout << "Exiting..." << std::endl;
  if (mds) delete mds;
  exit(0);
}

int
main(int argc, char *argv[])
{
  mds = new MDS(argc > 1 ? atoi(argv[1]) : DEFAULT_PORT);
  signal(SIGINT, signal_handler);
  mds->eventLoop();
  delete mds;
  return 0;
}
