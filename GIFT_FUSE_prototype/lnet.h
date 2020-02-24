#ifndef _LNET_H_
#define _LNET_H_

#include <vector>
#include <tuple>
#include <map>
#include <cstring>
#ifdef __linux__
# include <sys/epoll.h>
#else // ifdef __linux__
# include "epoll.h" // For macOS
#endif // ifdef __linux__
#include <string>

#include "lsocket.h"

const int MAX_EVENTS = 10000;

enum FsRequestType
{
  Getattr,
  Readlink,
  Mknod,
  Mkdir,
  Unlink,
  Rmdir,
  Symlink,
  Rename,
  Link,
  Chmod,
  Chown,
  Truncate,
  Utime,
  Open,
  Statfs,
  Flush,
  Release,
  Fsync,
  Setxattr,
  Getxattr,
  Listxattr,
  Removexattr,
  Opendir,
  Releasedir,
  Readdir,
  Access,
  Ftruncate,
  Fgetattr,
  Read,
  Write,
};
std::ostream& operator<<(std::ostream &os, const FsRequestType &t);

enum MsgType
{
  Unknown,
  AddOsc,
  AddOst,
  GetOstInfo,
  OstList,
  Ack,
  Timer,
  TimerResp,
  Allocs,
  FsRequest,
  FsResponse,
  Data,
};
std::ostream& operator<<(std::ostream &os, const MsgType &t);

struct BufType
{
  size_t sz;
  void *databuf;
};

struct LnetEntity;
struct AppInfo
{
  int id;
  char name[60];
};

using AppAlloc_t = std::tuple<int , double >;
using AppAllocs_t = std::vector<AppAlloc_t>;
using MapOstToAppAllocs_t = std::vector<AppAllocs_t>;

struct LnetMsg
{
  private:
    size_t off;

  public:
    MsgType t;
    FsRequestType f;
    bool extraData;
    LnetEntity *src;
    AppInfo _i;
    size_t len;
    void *data;

    LnetMsg();
    LnetMsg(MsgType );
    LnetMsg(MsgType , size_t );
    LnetMsg(MsgType , FsRequestType , size_t );
    ~LnetMsg();
    void clear();
    void copy(const LnetMsg *);
    void deepcopy(const LnetMsg *);
    template<typename T>
    LnetMsg &operator+=(const T* buf)
    {
      size_t slen = sizeof(T);
      if (!this->data || !this->extraData || !this->len || this->off + slen > this->len) return *this;
      memcpy((char*)this->data + this->off, buf, sizeof(T));
      this->off += sizeof(T);
      return *this;
    }
    LnetMsg &operator+=(const char* buf)
    {
      size_t slen = strlen(buf);
      if (!this->data || !this->extraData || !this->len || this->off + slen > this->len) return *this;
      memcpy((char*)this->data + this->off, buf, slen + 1);
      this->off += slen + 1;
      return *this;
    }
    LnetMsg &operator+=(const BufType *buf)
    {
      size_t slen = sizeof(buf->sz) + buf->sz;
      if (!this->data || !this->extraData || !this->len || this->off + slen > this->len) return *this;
      memcpy((char*)this->data + this->off, &buf->sz, sizeof(buf->sz));
      this->off += sizeof(buf->sz);
      memcpy((char*)this->data + this->off, buf->databuf, buf->sz);
      this->off += buf->sz;
      return *this;
    }
    template<typename T>
    void extractData(T *buf, size_t &offset) const
    {
      size_t slen = sizeof(T);
      if (!this->data || !this->extraData || !this->len || offset + slen > this->len) return;
      memcpy(buf, (char*)this->data + offset, sizeof(T));
      offset += sizeof(T);
    }
    void extractData(std::string *buf, size_t &offset) const
    {
      if (!this->data || !this->extraData || !this->len) return;
      *buf = (char*)this->data + offset;
      offset += buf->size() + 1;
    }
    void extractData(BufType *buf, size_t &offset) const
    {
      if (!this->data || !this->extraData || !this->len || ((offset + sizeof(buf->sz)) > this->len)) return;
      memcpy(&buf->sz, (char*)this->data + offset, sizeof(buf->sz));
      offset += sizeof(buf->sz);
      if (offset + buf->sz > this->len) return;
      memcpy(buf->databuf, (char*)this->data + offset, buf->sz);
      offset += buf->sz;
    }
    // Base function to stop the recursion
    void pack()
    {
    }
    template<typename T, typename... Targs>
    void pack(const T* arg0, const Targs*... args)
    {
      *this += arg0;
      this->pack(args...);
    }
    template<typename T, typename... Targs>
    void marshall(const T* arg0, const Targs*... args)
    {
      this->pack(arg0, args...);
    }
    // Base function to stop the recursion
    void unpack(size_t &offset) const
    {
    }
    template<typename T, typename... Targs>
    void unpack(size_t &offset, T* arg0, Targs*... args) const
    {
      this->extractData(arg0, offset);
      this->unpack(offset, args...);
    }
    template<typename T, typename... Targs>
    void unmarshall(T* arg0, Targs*... args) const
    {
      size_t offset = 0;
      this->unpack(offset, arg0, args...);
    }
};
std::ostream& operator<<(std::ostream &os, const LnetMsg &m);

enum LEntityType
{
  Invalid = -1,
  Mds,
  Ost,
  Osc,
};
std::ostream& operator<<(std::ostream &os, const LEntityType &t);

struct LnetEntity
{
  LEntityType type;
  int id;
  int listenport;
  char name[64];
  char hostname[64];
  LSocket *sock;

  LnetEntity()
  {
    this->type = Invalid;
    this->id = -1;
    this->listenport = -1;
    memset(this->name, 0, sizeof(this->name));
    memset(this->hostname, 0, sizeof(this->hostname));
    this->sock = NULL;
  }

  LnetEntity(LEntityType type)
   : LnetEntity()
  {
    this->type = type;
  }

  LnetEntity(const LnetEntity *ptr)
  {
    if (ptr) {
      this->type = ptr->type;
      this->id = ptr->id;
      this->listenport = ptr->listenport;
      memcpy(this->name, ptr->name, sizeof(this->name));
      memcpy(this->hostname, ptr->hostname, sizeof(this->name));
      this->sock = ptr->sock;
    }
  }

  ssize_t sendMsgToRemote(const LnetMsg *msg) const
  {
    if (!msg || !this->sock || !this->sock->isValid()) return -1;
    ssize_t totalData = 0;
    totalData += this->sock->writeAll(msg, sizeof(*msg));
    if (msg->extraData && msg->len > 0 && msg->data) {
      totalData += this->sock->writeAll(msg->data, msg->len);
    }
    return totalData;
  }

  ssize_t recvMsgFromRemote(LnetMsg *msg) const
  {
    if (!msg || !this->sock || !this->sock->isValid()) return -1;
    ssize_t totalData = 0;
    totalData += this->sock->readAll(msg, sizeof(*msg));
    if (msg->extraData && msg->len > 0) {
      msg->data = malloc(msg->len);
      totalData += this->sock->readAll(msg->data, msg->len);
    }
    return totalData;
  }

  ssize_t recvDataFromRemote(void *buf, size_t len) const
  {
    if (!buf || !this->sock || !this->sock->isValid()) return -1;
    return this->sock->readAll(buf, len);
  }

  ssize_t sendDataToRemote(const void *buf, size_t len) const
  {
    if (!buf || !this->sock || !this->sock->isValid()) return -1;
    return this->sock->writeAll(buf, len);
  }
};
std::ostream& operator<<(std::ostream &os, const LnetEntity &m);

struct MdsInfo: public LnetEntity
{
  MdsInfo()
   : LnetEntity(Mds)
   {
   }

  MdsInfo(const MdsInfo *ptr)
   : LnetEntity(ptr)
  {
  }
};

struct MdsClient: public LnetEntity
{
  MdsClient()
   : LnetEntity()
  {
  }

  MdsClient(LEntityType type)
   : LnetEntity(type)
   {
   }

  MdsClient(const MdsClient *ptr)
   : LnetEntity(ptr)
  {
  }
};

struct OscInfo: public MdsClient
{
  OscInfo()
   : MdsClient(Osc)
  {
  }

  OscInfo(const OscInfo *ptr)
   : MdsClient(ptr)
  {
  }
};

struct OstInfo: public MdsClient
{
  int dataport;

  OstInfo()
   : MdsClient(Ost)
  {
    this->dataport = -1;
  }

  OstInfo(const OstInfo *ptr)
    : MdsClient(ptr)
  {
    if (ptr) {
      this->dataport = ptr->dataport;
    }
  }
};

struct ActiveRequest
{
  struct AppInfo _info;
  FsRequestType _t;
};
std::ostream& operator<<(std::ostream &os, const ActiveRequest &r);

// class Epoll
// class LnetServer: public Epoll
// class LnetClient
// class LnetOsc
// class LnetOst: public LnetServer
// class LnetMds: public LnetServer

class Epoll
{
  protected:
    struct epoll_event _events[MAX_EVENTS];
    int _epollFd;

    virtual void addClient(const LnetEntity *);
    virtual void addRemoteServer(const LnetEntity *);
    virtual void removeClient(const LnetEntity *);

    virtual void onConnect() = 0;
    virtual void onDisconnect(const LnetEntity *) = 0;
    virtual void onClientRequest(const LnetEntity *) = 0;
    virtual void onRemoteServerRequest(const LnetEntity *) = 0;

  public:
    Epoll();
    virtual ~Epoll();
    virtual void eventLoop() = 0;
};

class LnetServer: public Epoll
{
  protected:
    LSocket *_sock;

    virtual void onConnect() = 0;
    virtual void onDisconnect(const LnetEntity *) = 0;
    virtual void onClientRequest(const LnetEntity *) = 0;
    virtual void onRemoteServerRequest(const LnetEntity *) = 0;

  public:
    LnetServer(int port);
    virtual ~LnetServer();
    virtual void eventLoop();
};

class LnetClient
{
  private:
    LSocket *_sockToRemoteServer;

  public:
    LnetClient(const LSockAddr &, int );
    virtual ~LnetClient();
    virtual bool connectToRemoteServer(const LSockAddr &, int );
    virtual ssize_t sendMsgToRemote(const LnetMsg *) const;
    virtual ssize_t recvMsgFromRemote(LnetMsg *);
    virtual ssize_t sendRawDataToRemote(const void*, size_t );
    virtual ssize_t recvRawDataFromRemote(void*, size_t );
    LSocket *getSocket();
};

#endif // ifndef _LNET_H_
