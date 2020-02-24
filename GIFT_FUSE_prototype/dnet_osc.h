#ifndef _DATANET_H_
#define _DATANET_H_

#include <ctype.h>
#include <vector>

#include "lsocket.h"
#include "lnet.h"

class DatanetOsc
{
  private:
    const OscInfo *_info;
    std::vector<LnetClient*> _toOsts;

    bool pubOscInfoToOst(const LnetClient *);

  public:
    DatanetOsc(const OscInfo *);
    ~DatanetOsc();
    bool connectToOsts(const std::vector<OstInfo*>& , const OscInfo *);
    ssize_t sendDataToOst(unsigned , const void *, size_t );
    ssize_t recvDataFromOst(unsigned , void *, size_t );
    ssize_t sendMsgToOst(unsigned , const LnetMsg* );
    ssize_t recvMsgFromOst(unsigned , LnetMsg* );
};

#endif // ifndef _DATANET_H_
