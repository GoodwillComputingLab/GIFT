#ifndef _LSOCKET_H_
#define _LSOCKET_H_

#include <errno.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <vector>

#include "util.h"

class LSocket;

class LSockAddr
{
  friend class LSocket;

  public:
    LSockAddr(const char *hostname = NULL, int port = -1);
    ~LSockAddr() {}

    static const LSockAddr ANY;

    const struct sockaddr_in *addr(unsigned int index = 0) const
    {
      if (index >= _count) {
        return &_addr[max_count];
      } else {
        return _addr + index;
      }
    }

    unsigned int addrcnt() const { return _count; }

    socklen_t addrlen() const { return sizeof(sockaddr_in); }

  private:
    const static unsigned int max_count = 32;

    // Allocate max_count + 1 to be able to return
    // zeroed socket structure if wrong addres index requested
    struct sockaddr_in _addr[max_count + 1];
    unsigned int _count;
};


class LSocket
{
  protected:
    LSocket();

  public:
    // so we don't leak FDs
    inline static LSocket Create() { return LSocket(); }

    ///
    /// Use existing socket
    LSocket(int fd) : _sockfd(fd) {}

    bool connect(const LSockAddr &addr, int port);
    bool connect(const struct  sockaddr *addr, socklen_t addrlen,
                 int port = -1);
    bool bind(const LSockAddr &addr, int port);
    bool bind(const struct  sockaddr *addr, socklen_t addrlen);
    bool listen(int backlog = 32);
    LSocket accept(struct sockaddr_storage *remoteAddr = NULL,
                   socklen_t *remoteLen = NULL);
    bool close();
    ssize_t read(void *buf, size_t len);
    ssize_t write(const void *buf, size_t len);
    ssize_t readAll(void *buf, size_t len);
    ssize_t writeAll(const void *buf, size_t len);
    bool isValid() const;

    void enablePortReuse();

    template<typename T>
    LSocket&operator<<(const T &t)
    {
      writeAll((const char *)&t, sizeof(T));
      return *this;
    }

    template<typename T>
    LSocket&operator>>(T &t) { readAll((char *)&t, sizeof(T)); return *this; }

    int sockfd() const { return _sockfd; }

    // If socket originally bound to port 0, we need this to find actual port
    int port() const
    {
      struct sockaddr_in addr;
      socklen_t addrlen = sizeof(addr);

      if (-1 == getsockname(_sockfd,
                            (struct sockaddr *)&addr, &addrlen)) {
        return -1;
      } else {
        return (int)ntohs(addr.sin_port);
      }
    }

    operator int() { return _sockfd; }
    void changeFd(int newFd);

  protected:
    int _sockfd;
};

class LClientSocket: public LSocket
{
  public:
    LClientSocket(const struct sockaddr *addr, socklen_t addrlen, int port = -1)
    {
      if (!connect(addr, addrlen, port)) {
        close();
      }
    }

    LClientSocket(int sockfd)
      : LSocket(sockfd)
    {
    }

    LClientSocket()
    {

    }
};

class LServerSocket: public LSocket
{
  public:
    LServerSocket(int sockfd)
      : LSocket(sockfd)
    {
      enablePortReuse();
    }

    LServerSocket(const LSockAddr &addr, int port, int backlog = 32)
    {
      enablePortReuse();
      if (!bind(addr, port) || !listen(backlog)) {
        close();
      }
    }
};

#endif // ifndef _LSOCKET_H_