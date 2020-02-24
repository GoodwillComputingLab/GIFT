#ifndef _UTIL_H
#define _UTIL_H

#include <ctype.h>
#include <sys/types.h>

#ifndef EXTERNC
# ifdef __cplusplus
#  define EXTERNC extern "C"
# else // ifdef __cplusplus
#  define EXTERNC
# endif // ifdef __cplusplus
#endif // ifndef EXTERNC

ssize_t writeAll(int fd, const void *buf, size_t len);
ssize_t readAll(int fd, void *buf, size_t len);

#endif // ifndef _UTIL_H
