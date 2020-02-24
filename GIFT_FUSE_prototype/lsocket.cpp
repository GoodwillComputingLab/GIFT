#include <cassert>
#include <cstring>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>

#include "lsocket.h"
#include "util.h"

const LSockAddr LSockAddr::ANY(NULL);

LSockAddr::LSockAddr(const char *hostname /* == NULL*/,
                            int port /* == -1*/)
{
  // Initialization for any case
  memset((void *)&_addr, 0, sizeof(_addr));
  for (unsigned int i = 0; i < (max_count + 1); i++) {
    _addr[i].sin_family = AF_INET;
  }
  _count = 0;

  if (hostname == NULL) {
    _count = 1;
    _addr[0].sin_addr.s_addr = INADDR_ANY;
    if (port != -1) {
      _addr[0].sin_port = htons(port);
    }
    return;
  }

  struct addrinfo hints;
  struct addrinfo *res;
  bzero(&hints, sizeof hints);
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_ADDRCONFIG;

  /* FIXME: Ulrich Drepper states the following about getaddrinfo():
   *   The most important thing when using getaddrinfo is to make sure that all
   *   results are used in order. To stress the important words again: all and
   *   order. Too many (incorrect) programs only use the first result.
   * Source: http://www.akkadia.org/drepper/userapi-ipv6.html
   *
   * This would require some sort of redesign of JSockAddr and JSocket classes.
   */
  int e = getaddrinfo(hostname, NULL, &hints, &res);
  if (e == EAI_NONAME) {
    /*
     * If the AI_ADDRCONFIG flag is passed to getaddrinfo() with AF_INET or
     * AF_INET6 address families,
     * addresses in /etc/hosts are not resolved properly.
     * According to following bug reports:
     * https://bugs.launchpad.net/ubuntu/+source/glibc/+bug/583278
     * https://bugzilla.mozilla.org/show_bug.cgi?id=467497#c10
     *
     * As a temporary workaround, just call it without AI_ADDRCONFIG again
     */
    hints.ai_flags = 0;
    e = getaddrinfo(hostname, NULL, &hints, &res);
  }

  if (e == 0) {
    assert(sizeof(*_addr) >= res->ai_addrlen);

    // 1. count number of addresses returned
    struct addrinfo *r;
    for (r = res, _count = 0; r != NULL; r = r->ai_next, _count++) {}
    if (_count > max_count) {
      _count = max_count;
    }

    // 2. array for storing all necessary addresses
    int i;
    for (r = res, i = 0; r != NULL; r = r->ai_next, i++) {
      memcpy(_addr + i, r->ai_addr, r->ai_addrlen);
      if (port != -1) {
        _addr[i].sin_port = htons(port);
      }
    }
  } else { // else (hostname, port) not valid; poison the port number
    _addr[0].sin_port = (unsigned short)-2;
  }

  freeaddrinfo(res);
}

LSocket::LSocket()
{
  _sockfd = ::socket(AF_INET, SOCK_STREAM, 0);
}


bool
LSocket::connect(const LSockAddr &addr, int port)
{
  bool ret = false;

  // jalib::JSockAddr::JSockAddr used -2 to poison port (invalid host)
  if (addr._addr->sin_port == (unsigned short)-2) {
    return false;
  }
  for (unsigned int i = 0; i < addr._count; i++) {
    ret = LSocket::connect((sockaddr *)(addr._addr + i),
                           sizeof(addr._addr[0]),
                           port);
    if (ret || errno != ECONNREFUSED) {
      break;
    }
  }
  return ret;
}

bool
LSocket::connect(const struct  sockaddr *addr,
                 socklen_t addrlen,
                 int port)
{
  struct sockaddr_storage addrbuf;

  memset(&addrbuf, 0, sizeof(addrbuf));
  assert(addrlen <= sizeof(addrbuf));
  memcpy(&addrbuf, addr, addrlen);
  // JWARNING(addrlen == sizeof(sockaddr_in)) (addrlen)
  //   (sizeof(sockaddr_in)).Text("may not be correct socket type");
  if (port != -1) {
    ((sockaddr_in *)&addrbuf)->sin_port = htons(port);
  }
  int count = 0;
  int ret;
  while (count++ < 10) {
    ret = ::connect(_sockfd, (sockaddr *)&addrbuf, addrlen);
    if (ret == 0 ||
        (ret == -1 && errno != ECONNREFUSED && errno != ETIMEDOUT)) {
      break;
    }
    if (ret == -1 && (errno == ECONNREFUSED || errno == ETIMEDOUT)) {
      struct timespec ts = { 0, 100 * 1000 * 1000 };
      nanosleep(&ts, NULL);
    }
  }
  return ret == 0;
}

bool
LSocket::bind(const LSockAddr &addr, int port)
{
  bool ret = false;

  for (unsigned int i = 0; i < addr._count; i++) {
    struct sockaddr_in addrbuf = addr._addr[i];
    addrbuf.sin_port = htons(port);
    int retval = bind((sockaddr *)&addrbuf, sizeof(addrbuf));
    ret = ret || retval;
  }
  return ret;
}

bool
LSocket::bind(const struct sockaddr *addr, socklen_t addrlen)
{
  return ::bind(_sockfd, addr, addrlen) == 0;
}

bool
LSocket::listen(int backlog /* = 32*/)
{
  return ::listen(_sockfd, backlog) == 0;
}

LSocket
LSocket::accept(struct sockaddr_storage *remoteAddr,
                       socklen_t *remoteLen)
{
  if (remoteAddr == NULL || remoteLen == NULL) {
    return LSocket(::accept(_sockfd, NULL, NULL));
  } else {
    return LSocket(::accept(_sockfd, (sockaddr *)remoteAddr, remoteLen));
  }
}

void
LSocket::enablePortReuse()
{
  int one = 1;
  // http://stackoverflow.com/a/14388707/1136967
  if (::setsockopt(_sockfd, SOL_SOCKET, SO_REUSEADDR, &one,
                        sizeof(one)) < 0) {
  }
}

bool
LSocket::close()
{
  if (!isValid()) {
    return false;
  }
  int ret = ::close(_sockfd);
  _sockfd = -1;
  return ret == 0;
}

ssize_t
LSocket::read(void *buf, size_t len)
{
  return ::read(_sockfd, buf, len);
}

ssize_t
LSocket::write(const void *buf, size_t len)
{
  return ::write(_sockfd, buf, len);
}

ssize_t
LSocket::readAll(void *buf, size_t len)
{
  return ::readAll(_sockfd, buf, len);
}

ssize_t
LSocket::writeAll(const void *buf, size_t len)
{
  return ::writeAll(_sockfd, buf, len);
}

bool
LSocket::isValid() const
{
  return _sockfd >= 0;
}