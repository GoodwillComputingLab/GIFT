#ifndef _MDS_H_
#define _MDS_H_

#include <vector>
#include <mutex>
#include <tuple>
#include <condition_variable>

#include "lnet.h"

enum Policy_t {
  POFS,
  BSIP,
  TSA,
  ESA,
  TMF,
  RND,
  MBW,
  GIFT
};

using Coupon_t = std::tuple<double, int>;
struct AppCoup_t {
  int app;
  std::vector<Coupon_t> coupons;
};

struct AppData_t {
  int    app_id;
  double bw_issued;
  double bw_redeemed;
};

using AppAlloc_t          = std::tuple<int , double>;
using AppAllocs_t         = std::vector<AppAlloc_t>;
using MapOstToAppAllocs_t = std::vector<AppAllocs_t>;

class LnetMds: public LnetServer
{
  private:
    std::vector<OscInfo*> _oscs;
    std::vector<OstInfo*> _osts;
    std::map<std::string, OstInfo*> _dirToOst;
    std::map<const LnetEntity* , std::vector<ActiveRequest>> _ostReqs;
    std::mutex *_m;
    std::condition_variable *_waitForAllOsts;
    bool *_dataIsReady;

    void addOsc(const LSocket &remote, const OscInfo *info);
    void addOst(const LSocket &remote, const OstInfo *info);
    void sendOstsInfo(const LnetEntity *remote);
    void handleFsRequest(const LnetEntity *remote, const LnetMsg *msg);
    const OstInfo *getOstFromPath(const std::string *path) const;
    void addToTimerResponse(const LnetEntity *remote, const LnetMsg *msg);
    void computeBwAllocations(Policy_t , const std::vector<std::vector<int>> &,
                              MapOstToAppAllocs_t &);
    void bcastAllocsToOsts(const MapOstToAppAllocs_t &);

    void computeBwAllocationsGIFT(const size_t ,
                                  const std::vector<std::vector<int>> &,
                                  MapOstToAppAllocs_t &);
    void computeBwAllocationsBSIP(const size_t ,
                                  const std::vector<std::vector<int>> &,
                                  MapOstToAppAllocs_t &);
    void computeBwAllocationsPOFS(const size_t ,
                                  const std::vector<std::vector<int>> &,
                                  MapOstToAppAllocs_t &);
    void computeBwAllocationsTSA(const size_t ,
                                 const std::vector<std::vector<int>> &,
                                 MapOstToAppAllocs_t &);
    void computeBwAllocationsESA(const size_t ,
                                 const std::vector<std::vector<int>> &,
                                 MapOstToAppAllocs_t &);
    void computeBwAllocationsTMF(const size_t ,
                                 const std::vector<std::vector<int>> &,
                                 MapOstToAppAllocs_t &);
    void computeBwAllocationsRND(const size_t ,
                                 const std::vector<std::vector<int>> &,
                                 MapOstToAppAllocs_t &);
    void computeBwAllocationsMBW(const size_t ,
                                 const std::vector<std::vector<int>> &,
                                 MapOstToAppAllocs_t &);
    double getEffectiveSysBw();

  protected:
    virtual void onConnect();
    virtual void onDisconnect(const LnetEntity *);
    virtual void onClientRequest(const LnetEntity *);
    virtual void onRemoteServerRequest(const LnetEntity *);

  public:
    LnetMds(int port, std::mutex *m, std::condition_variable *cv, bool *b);
    ~LnetMds();
    ssize_t sendMsgToOst(const LnetMsg *msg, int id) const;
    ssize_t recvMsgFromOst(LnetMsg *msg, int id) const;
    bool bcastMsgToOsts(const LnetMsg *msg);
    void printStats();
  
  friend class MdsOps;
};

class MdsOps
{
  public: 
    static bool mds_commonprolog(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg, FsRequestType );
    static void mds_commonepilog(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg, const std::string &dname);

    static void mds_access(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg);
    static void mds_mkdir(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg);
    static void mds_opendir(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg);
    static void mds_releasedir(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg);
    static void mds_getattr(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg);
    static void mds_fgetattr(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg);
    static void mds_mknod(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg);
    static void mds_unlink(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg);
    static void mds_rmdir(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg);
    static void mds_chmod(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg);
    static void mds_chown(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg);
    static void mds_truncate(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg);
    static void mds_utime(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg);
    static void mds_open(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg);
    static void mds_statfs(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg);
    static void mds_flush(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg);
    static void mds_release(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg);
    static void mds_fsync(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg);
    static void mds_readlink(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg);
    static void mds_symlink(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg);
    static void mds_rename(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg);
    static void mds_link(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg);
    static void mds_readdir(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg);
    static void mds_getxattr(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg);
    static void mds_setxattr(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg);
    static void mds_listxattr(const LnetMds *lmds, const LnetEntity *remote, const LnetMsg *msg);
};

class MDS
{
  private:
    // fields
    LnetMds *_mdsNet;
    std::mutex *m;
    std::condition_variable *waitForAllOsts;
    bool dataIsReady;

  public:
    // constructor(s)
    MDS(int port);
    ~MDS();

    // methods
    void eventLoop();
    void startTimer();
    ssize_t sendDataToOst(int idx, const void *buf, size_t len);
    ssize_t recvDataFromOst(int idx, void *buf, size_t len);
};

extern void printStats();

#endif // ifndef _MDS_H_
