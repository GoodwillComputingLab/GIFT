#ifndef _OST_H_
#define _OST_H_

#include <vector>

#include "lnet.h"
#include "dnet_ost.h"

class LnetOst: public LnetServer
{
  private:
    const OstInfo *_info;
    MdsInfo *_mdsInfo;
    LnetClient *_toMds;
    std::vector<OscInfo*> _oscs;
    DatanetOst *_dnet;
  
    void addOsc(const LSocket &, const OscInfo *);
    void respondToMdsTimer(const LnetEntity *);
    void handleFsRequest(const LnetEntity *, const LnetMsg *);

  protected:
    virtual void onConnect();
    virtual void onDisconnect(const LnetEntity *);
    virtual void onClientRequest(const LnetEntity *);
    virtual void onRemoteServerRequest(const LnetEntity *);

  public:
    LnetOst(const LSockAddr &, int , const OstInfo *, DatanetOst *);
    ~LnetOst();
    bool pubOstInfoToMds();
    ssize_t sendMsgToMds(const LnetMsg *);
    ssize_t recvMsgFromMds(LnetMsg *);

  friend class OstOps;
};

class OST
{
  private:
    // fields
    OstInfo *_info;
    LnetOst *_ostNet;
    DatanetOst *_dataNet;
    std::vector<OscInfo*> _oscs;
    bool _mdsConnected;

  public:
    // constructor(s)
    OST(const LSockAddr &addr, int port, int id, const char *name, int lnetport, int dataport);
    ~OST();

    // methods
    void eventLoop();
    ssize_t sendDataToMds(const void *buf, size_t len);
    ssize_t recvDataFromMds(void *buf, size_t len);
};

#endif // ifndef _OST_H_