#ifndef _DATANET_OST_H_
#define _DATANET_OST_H_

#include <ctype.h>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <thread>

#include "lsocket.h"
#include "lnet.h"

class DatanetOst: public LnetServer
{
  private:
    using RwReq_t = std::tuple<const LnetEntity**,
                               LnetMsg*,
                               int ,
                               const std::thread*,
                               pid_t ,
                               std::mutex* ,
                               std::condition_variable* ,
                               bool *,
                               bool *,
                               AppInfo>;
    const OstInfo *_info;
    std::vector<OscInfo*> _oscs;
    std::vector<LSocket*> _oscSocks;
    std::vector<RwReq_t> _reqs;
    std::vector<RwReq_t> _deletedReqs;
    std::mutex *_reqLock;
    std::mutex *_deletedReqsLock;

    void addOsc(const LSocket &, const OscInfo *);
    void handleClientFsRequest(const LnetEntity *remote, const LnetMsg *msg);
    void markReqForDeletion(const RwReq_t& );
    void moveToCgroup(pid_t , size_t );

    // void handleClientRwRequest(const LnetEntity* , const LnetMsg* , bool );

  protected:
    virtual void onConnect();
    virtual void onDisconnect(const LnetEntity *);
    virtual void onClientRequest(const LnetEntity *);
    virtual void onRemoteServerRequest(const LnetEntity *);

  public:
    DatanetOst(const OstInfo *);
    virtual ~DatanetOst();
    void enqueue(const LnetEntity **, LnetMsg *, int , const std::thread *,
                 pid_t , std::mutex* , std::condition_variable* ,
                 bool *, bool *, AppInfo );
    void dequeue(const LnetEntity *, const LnetMsg *, AppInfo );
    void sendMsgToRwThread(const LnetEntity *, const LnetMsg *);
    void setAllocations(const LnetEntity *, const LnetMsg *);
    void garbageCollectCompletedReqs();
    void getActiveRequests(std::vector<ActiveRequest>& ) const;
    void waitForMsg(std::mutex *, std::condition_variable *, bool *);
    void waitForMsgProcess(std::mutex *, std::condition_variable *, bool *);
    void wakeUp(std::mutex *, std::condition_variable *, bool *);
    void wakeUpForMsgProcess(std::mutex *, std::condition_variable *, bool *);
};


#endif // ifndef _DATANET_OST_H_
