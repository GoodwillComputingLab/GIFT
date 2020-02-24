#include <cassert>

#include "lnet.h"

Epoll::Epoll()
{
  this->_epollFd = epoll_create(MAX_EVENTS);
  assert(this->_epollFd > 0);
  memset(this->_events, 0, sizeof(this->_events));
}

Epoll::~Epoll()
{
  this->_epollFd = -1;
  memset(this->_events, 0, sizeof(this->_events));
}

void Epoll::addRemoteServer(const LnetEntity *remote)
{
  if (!remote || !remote->sock->isValid()) return;
  struct epoll_event ev;

#ifdef EPOLLRDHUP
  ev.events = EPOLLIN | EPOLLRDHUP;
#else // ifdef EPOLLRDHUP
  ev.events = EPOLLIN;
#endif // ifdef EPOLLRDHUP
  ev.data.ptr = (void*)remote;
  assert(epoll_ctl(this->_epollFd, EPOLL_CTL_ADD, remote->sock->sockfd(), &ev) != -1);
}

void Epoll::addClient(const LnetEntity *remote)
{
  if (!remote || !remote->sock) return;
  struct epoll_event ev;

#ifdef EPOLLRDHUP
  ev.events = EPOLLIN | EPOLLRDHUP;
#else // ifdef EPOLLRDHUP
  ev.events = EPOLLIN;
#endif // ifdef EPOLLRDHUP
  ev.data.ptr = (void*)remote;
  assert(epoll_ctl(this->_epollFd, EPOLL_CTL_ADD, remote->sock->sockfd(), &ev) != -1);
}

void Epoll::removeClient(const LnetEntity *remote)
{
  if (!remote || !remote->sock) return;
  struct epoll_event ev;

  ev.data.ptr = (void*)remote;
  assert(epoll_ctl(this->_epollFd, EPOLL_CTL_DEL, remote->sock->sockfd(), &ev) != -1);
}

LnetServer::LnetServer(int port)
{
  this->_sock = new LServerSocket(LSockAddr::ANY, port);
  assert(this->_sock->isValid());
}

LnetServer::~LnetServer()
{
  this->_sock->close();
  delete this->_sock;
}

void LnetServer::eventLoop()
{
  struct epoll_event ev;

  ev.events = EPOLLIN;
  ev.data.ptr = this->_sock;
  assert(epoll_ctl(this->_epollFd, EPOLL_CTL_ADD, this->_sock->sockfd(), &ev) != -1);
  while (true) {
    int nfds = epoll_wait(this->_epollFd, this->_events, MAX_EVENTS, -1);
    if (nfds == -1 && errno == EINTR) {
      continue;
    }
    for (int n = 0; n < nfds; ++n) {
      void *ptr = this->_events[n].data.ptr;
      if ((this->_events[n].events & EPOLLHUP) ||
#ifdef EPOLLRDHUP
          (this->_events[n].events & EPOLLRDHUP) ||
#endif // ifdef EPOLLRDHUP
          (this->_events[n].events & EPOLLERR)) {
        assert(ptr != this->_sock);
        this->onDisconnect((LnetEntity*)ptr);
      } else if (this->_events[n].events & EPOLLIN) {
        if (ptr == (void *)this->_sock) {
          this->onConnect();
        } else {
          this->onClientRequest((LnetEntity*)ptr);
        }
      }
    }
  }
}
