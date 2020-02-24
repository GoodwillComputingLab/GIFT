#ifndef _PARAMS_H_
#define _PARAMS_H_

#ifndef TEST_ENV
// The FUSE API has been changed a number of times.  So, our code
// needs to define the version of the API that we assume.  As of this
// writing, the most current API version is 26
#define FUSE_USE_VERSION 26

// need this to get pwrite().  I have to use setvbuf() instead of
// setlinebuf() later in consequence.
#define _XOPEN_SOURCE 500
#endif // ifndef TEST_ENV

// maintain lemufs state in here
#include <limits.h>
#include <stdio.h>

#include "osc.h"

struct lemu_state
{
  FILE *logfile;
  char *rootdir;
  OSC *osc;
};

#ifndef TEST_ENV
# define LEMU_DATA ((struct lemu_state *) fuse_get_context()->private_data)
#else // ifndef TEST_ENV
extern struct lemu_state *lemu_data;
# define LEMU_DATA lemu_data
#endif // ifndef TEST_ENV

#endif
