#ifndef _OSC_H_
#define _OSC_H_

#include <vector>
#include <map>

#include "lnet.h"
#include "dnet_osc.h"
#include "util.h"

class LnetOsc
{
  private:
    const OscInfo *_info;
    LnetClient *_toMds;
    std::vector<LnetClient*> _toOsts;

  public:
    LnetOsc(const LSockAddr &, int , const OscInfo *);
    ~LnetOsc();
    bool pubOscInfoToMds() const;
    bool pubOscInfoToOst(const LnetClient *) const;
    bool connectToOsts(std::vector<OstInfo*> &);
    ssize_t sendMsgToOst(const LnetMsg *, int );
    ssize_t recvMsgFromOst(LnetMsg *, int );
    ssize_t sendMsgToMds(const LnetMsg *);
    ssize_t recvMsgFromMds(LnetMsg *);
};

class OSC
{
  private:
    // fields
    OscInfo *_info;
    LnetOsc *_oscNet;
    DatanetOsc *_dataNet;
    std::vector<OstInfo*> _osts;
    bool _mdsConnected;
    std::map<int, AppInfo> fdToApp;

    // methods
    bool connectToOsts();

  public:
    // constructor(s)
    OSC(const LSockAddr &addr, int port, int id, const char *name);
    ~OSC();

    // methods
    bool start();
    ssize_t sendDataToOst(int idx, const void *buf, size_t len);
    ssize_t recvDataFromOst(int idx, void *buf, size_t len);
    bool createDirStructure(const std::string &rootdir);
    ssize_t sendMsgToMds(const LnetMsg *msg);
    ssize_t recvMsgFromMds(LnetMsg *msg);
    ssize_t sendMsgToOst(const LnetMsg *msg, int id);
    ssize_t recvMsgFromOst(LnetMsg *msg, int id);
    ssize_t sendDatanetMsgToOst(const LnetMsg *msg, int id);
    ssize_t recvDatanetMsgFromOst(LnetMsg *msg, int id);
    const OstInfo *getOstFromPath(const char *path) const;
    const std::vector<OstInfo*>& getOsts() { return this->_osts; }
    void insertFdToApp(int , AppInfo );
    bool getAppForFd(int , AppInfo *);
    void cleanFdMap(int );
};

#endif // ifndef _OSC_H_