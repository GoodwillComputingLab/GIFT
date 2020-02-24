#include <cassert>
#include <cstdlib>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

ssize_t
writeAll(int fd, const void *buf, size_t len)
{
  const char *ptr = (const char *)buf;
  size_t num_written = 0;

  do {
    ssize_t rc = write(fd, ptr + num_written, len - num_written);
    if (rc == -1) {
      if (errno == EINTR || errno == EAGAIN) {
        continue;
      } else {
        return rc;
      }
    } else if (rc == 0) {
      break;
    } else { // else rc > 0
      num_written += rc;
    }
  } while (num_written < len);
  // assert(num_written == len);
  return num_written;
}

ssize_t
readAll(int fd, void *buf, size_t len)
{
  ssize_t rc;
  char *ptr = (char *)buf;
  size_t num_read = 0;

  for (num_read = 0; num_read < len;) {
    rc = read(fd, ptr + num_read, len - num_read);
    if (rc == -1) {
      if (errno == EINTR || errno == EAGAIN) {
        continue;
      } else {
        return -1;
      }
    } else if (rc == 0) {
      break;
    } else { // else rc > 0
      num_read += rc;
    }
  }
  return num_read;
}
