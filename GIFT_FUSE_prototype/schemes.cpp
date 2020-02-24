#include <algorithm>
#include <cassert>
#include <iostream>
#include <vector>
#include <set>

#include "lnet.h"
#include "mds.h"
#include "ClpSimplex.hpp"

// Required for all policies
#define ALLOC_XTRA_BW   false
#define ALLOC_X_BW_EQ   true

// Required for TSA policy
#define TSA_B_THRESHOLD 0.1

// Required for ESA policy
#define ESA_B_THRESHOLD 0.1

// Required for TMF policy
#define TMF_B_THRESHOLD 0.1

// Required for RND policy
#define RND_B_THRESHOLD 0.1

// Required for GIFT policy
#define GIFT_SYS_RDMP_T 0.8
#define GIFT_RDMP_RT_TH 0.8
#define GIFT_B_THRESOLD 0.1
#define GIFT_COUP_BW_SZ 1e-6
#define GIFT_WINDOW_LEN 1000000
#define GIFT_RESET_TIME 86400

using namespace std;

// Required for TMF policy
struct AppFreq_t {
	unsigned int app;
	unsigned int num;
};

vector<AppFreq_t> freqDatabase;

// Required for GIFT policy
double sys_coup_issu = 0.0;
double sys_coup_rdmp = 0.0;

vector<AppData_t> appDatabase;

vector<int>    numCouponsIssued;
vector<double> valCouponsIssued;

// Track the effective storage system utilization for all policies
vector<double> runEffStorageSysUtil;

void LnetMds::computeBwAllocationsPOFS(const size_t NUM_OSTS,
                                       const std::vector<std::vector<int>> &reqs,
                                       MapOstToAppAllocs_t &allocs)
{

  // Create a flat vector
  vector<int> flat;
  for (size_t i = 0; i < NUM_OSTS; i++) {
    for (auto req : reqs[i]) {
      flat.push_back(req);
    }
  }

  // Determine the number of applications doing I/O
  set<int> s = set<int>(flat.begin(), flat.end());
  vector<int> apps(s.begin(), s.end());
  const int numApps = (int)apps.size();

  // Determine the minimum bandwidth of each app after fair share
  double* minBw = NULL;
  minBw = new double[numApps];
  fill_n(minBw, numApps, numeric_limits<double>::infinity());

  for (size_t i = 0; i < NUM_OSTS; i++) {
    double alloc = 1.0 / (double)reqs[i].size();
    for (auto req : reqs[i]) {
      int ind = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      minBw[ind] = min(minBw[ind], alloc);
    }
  }

  // Determine effective storage system utilization and allocation of the bandwidth
  double totalUsedBw = 0.0;
  for (size_t i = 0; i < NUM_OSTS; i++) {
    double usedBw = 0.0;
    for (auto req : reqs[i]) {
      int ind = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      usedBw += minBw[ind];
    }
    totalUsedBw += usedBw;

    AppAllocs_t alloc;
    double perAppBw = 1.0 / (double)reqs[i].size();
    for (auto req : reqs[i]) {
      alloc.push_back(make_tuple(req, perAppBw));
    }
    allocs.push_back(alloc);
  }

  runEffStorageSysUtil.push_back(totalUsedBw);

  delete [] minBw;
}

void LnetMds::computeBwAllocationsGIFT(const size_t NUM_OSTS,
                                       const std::vector<std::vector<int>> &reqs,
                                       MapOstToAppAllocs_t &allocs)
{
  // Create a flat vector
  vector<int> flat;
  for (size_t i = 0; i < NUM_OSTS; i++) {
    for (auto req : reqs[i]) {
      flat.push_back(req);
    }
  }

  // Determine the number of applications doing I/O
  set<int> s = set<int>(flat.begin(), flat.end());
  vector<int> apps(s.begin(), s.end());
  const int numApps = (int)apps.size();

  // Row Order matrix
  const bool cOrd = false;

  // Row Indices
  int* rInd = NULL;
  rInd = new int[NUM_OSTS * numApps];
  for (size_t i = 0; i < NUM_OSTS; i++) {
    for (int j = 0; j < numApps; j++) {
      rInd[(i * numApps) + j] = i;
    }
  }

  // Col Indices
  int* cInd = NULL;
  cInd = new int[NUM_OSTS * numApps];
  for (size_t i = 0; i < NUM_OSTS; i++) {
    for (int j = 0; j < numApps; j++) {
      cInd[(i * numApps) + j] = j;
    }
  }

  // Constraints matrix
  double* ele = NULL;
  ele = new double[NUM_OSTS * numApps];
  memset(ele, 0, (NUM_OSTS * numApps) * sizeof(double));
  for (size_t i = 0; i < NUM_OSTS; i++) {
    for (auto req : reqs[i]) {
      int app = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      ele[(i * numApps) + app] += 1;
    }
  }

  // Number of elements in the constraints matrix
  CoinBigIndex numels = NUM_OSTS * numApps;

  // Make a matrix of constraints
  const CoinPackedMatrix matrix(cOrd, rInd, cInd, ele, numels);

  // Lower allocation bound for each app
  double* alcs = NULL;
  alcs = new double[numApps];	
  fill_n(alcs, numApps, numeric_limits<double>::infinity());

  int* osts = NULL;
  osts = new int[numApps];
  memset(osts, 0, numApps * sizeof(int));

  vector<double> apTh; // = {true, false, true, false, true};
  bool found = false;
  for (auto app : apps) {
    found = false;
    for (auto base : appDatabase) {
      if (base.app_id == app) {
        if ((base.bw_redeemed / base.bw_issued) >= GIFT_RDMP_RT_TH) {
          apTh.push_back(base.bw_redeemed - (base.bw_issued * GIFT_RDMP_RT_TH));
        } else {
          apTh.push_back(0.0);
        }
        found = true;
        break;
      }
    }

    if (!found) {
      appDatabase.push_back(AppData_t());
      appDatabase.back().app_id = app;
      appDatabase.back().bw_issued = GIFT_COUP_BW_SZ * GIFT_WINDOW_LEN;
      appDatabase.back().bw_redeemed = GIFT_COUP_BW_SZ * GIFT_WINDOW_LEN;
      apTh.push_back(appDatabase.back().bw_redeemed - (appDatabase.back().bw_issued * GIFT_RDMP_RT_TH));
    }
  }

  for (size_t i = 0; i < NUM_OSTS; i++) {
    double alc = 1.0 / (double)reqs[i].size();
    for (auto req : reqs[i]) {
      int app = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      alcs[app] = min(alcs[app], alc);
      osts[app] += 1;
    }
  }

  double efbw[NUM_OSTS];
  memset(efbw, 0.0, NUM_OSTS * sizeof(double));
  for (size_t i = 0; i < NUM_OSTS; i++) {
    for (auto req : reqs[i]) {
      int app = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      efbw[i] += alcs[app];
    }
    //cout << efbw[i] << endl;
  }

  /* for (int i = 0; i < numApps; i++) {
     cout << alcs[i] << endl;
     cout << osts[i] << endl;
     } */

  int    numCoups = 0;
  double valCoups = 0.0;

  vector<double> bwRd;
  vector<int> apps_cpy = apps;
  random_shuffle(apps_cpy.begin(), apps_cpy.end());
  for (auto app : apps_cpy) {
    double bwRq = 0.0;
    for (auto base : appDatabase) {
      if (base.app_id == app) {
        bwRq = base.bw_issued - base.bw_redeemed;
        break;
      }
    }

    if (bwRq == 0.0) {
      bwRd.push_back(0.0);
      continue;
    }

    double bwAv = numeric_limits<double>::infinity();
    for (size_t i = 0; i < NUM_OSTS; i++) {
      int numReqs = 0;
      for (auto req : reqs[i]) {
        if (req == app) {
          numReqs += 1;
        }
      }
      if (numReqs == 0) {
        continue;
      }
      if (efbw[i] == 1.0) {
        bwAv = 0.0;
        break;
      }
      bwAv = min(bwAv, ((1.0 - efbw[i])/(double)numReqs));
    }

    if (bwAv == 0.0) {
      bwRd.push_back(0.0);
      continue;
    }

    int index = 0;
    for (auto a : apps) {
      if (a == app) {
        break;
      }
      index++;
    }

    double bwGv = min(bwRq, bwAv * osts[index]);
    for (auto& base : appDatabase) {
      if (base.app_id == app) {
        for (size_t i = 0; i < NUM_OSTS; i++) {
          int numReqs = 0;
          for (auto req : reqs[i]) {
            if (req == app) {
              numReqs += 1;
            }
          }
          if (numReqs == 0) {
            continue;
          }
          efbw[i] -= ((bwGv / (double)osts[index]) * (double)numReqs);
        }
        base.bw_redeemed += (double)((int)(bwGv / GIFT_COUP_BW_SZ)) * GIFT_COUP_BW_SZ;
        break;
      }
    }
    valCoups -= (double)((int)(bwGv / GIFT_COUP_BW_SZ)) * GIFT_COUP_BW_SZ;
    sys_coup_rdmp += (double)((int)(bwGv / GIFT_COUP_BW_SZ)) * GIFT_COUP_BW_SZ;
    bwRd.push_back(bwGv/(double)osts[index]);
  }

  double* collb = NULL;
  collb = new double[numApps];
  double* fsbw = NULL;
  fsbw = new double[numApps];
  fill_n(collb, numApps, numeric_limits<double>::infinity());
  fill_n(fsbw, numApps, numeric_limits<double>::infinity());

  for (int app = 0; app < numApps; app++) {
    if (((sys_coup_issu == 0.0) or ((sys_coup_issu != 0.0) && ((sys_coup_rdmp/sys_coup_issu) > GIFT_SYS_RDMP_T))) && (apTh[app] > 0.0)) {
      if (apTh[app] > ((alcs[app] * GIFT_B_THRESOLD) * osts[app])) {
        collb[app] = (alcs[app] * (1.0 - GIFT_B_THRESOLD)) + bwRd[app];
      } else {
        collb[app] = alcs[app] - (apTh[app] / (double)osts[app]) + bwRd[app];
      }
    } else {
      collb[app] = alcs[app] + bwRd[app];
    }
    fsbw[app] = alcs[app] + bwRd[app];
  }

  /*
     for (size_t i = 0; i < NUM_OSTS; i++) {
     double alloc = 1.0 / (double)reqs[i].size();
     for (auto req : reqs[i]) {
     int app = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
     if (apTh[app]) {
     collb[app] = min(collb[app], (alloc * (1.0 - GIFT_B_THRESOLD)) + bwRd[app]);
     } else {
     collb[app] = min(collb[app], alloc + bwRd[app]);
     }
     fsbw[app] = min(fsbw[app], alloc + bwRd[app]);
     }
     }*/

  // Upper allocation bound for each app
  double* colub = NULL;
  colub = new double[numApps];
  fill_n(colub, numApps, 1);

  // The cooefficients of the minimum objective function
  double* obj = NULL;
  obj = new double[numApps];
  memset(obj, 0, numApps * sizeof(double));
  for (auto f : flat) {
    int app = (int)(find(apps.begin(), apps.end(), f) - apps.begin());
    obj[app] -= 1;
  }

  // The upper and lower bounds of each OST
  double rowlb[NUM_OSTS], rowub[NUM_OSTS];
  memset(rowlb, 0, NUM_OSTS * sizeof(double));
  fill_n(rowub, NUM_OSTS, 1);

  ClpSimplex model;
  model.loadProblem(matrix, collb, colub, obj, rowlb, rowub, NULL);
  model.primal();
  double* cVal = model.primalColumnSolution();

  bool* done = NULL;
  done = new bool[numApps];
  fill_n(done, numApps, false);

  // Determine allocation of the bandwidth
  for (size_t i = 0; i < NUM_OSTS; i++) {
    double usedBw = 0.0;
    for (auto req : reqs[i]) {
      int ind = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      usedBw += cVal[ind];
    }

    double xtraBw = 0.0;
    if ((ALLOC_XTRA_BW == true) && (ALLOC_X_BW_EQ == true)) {
      xtraBw = (1.0 - usedBw) / (double)reqs[i].size();
    }

    AppAllocs_t alloc;

    for (auto req : reqs[i]) {
      int ind = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      if ((ALLOC_XTRA_BW == true) && (ALLOC_X_BW_EQ == false)) {
        xtraBw = (1.0 - usedBw) * (cVal[ind] / usedBw);
      }
      alloc.push_back(make_tuple(req, cVal[ind] + xtraBw));

      if ((!done[ind]) && (cVal[ind] < fsbw[ind])) {
        done[ind] = true;
        for (auto& base : appDatabase) {
          if (base.app_id == req) {
            base.bw_redeemed -= (double)((int)(((fsbw[ind] - cVal[ind]) * osts[ind]) / GIFT_COUP_BW_SZ)) * GIFT_COUP_BW_SZ;
            numCoups += (int)(((fsbw[ind] - cVal[ind]) * osts[ind]) / GIFT_COUP_BW_SZ);
            valCoups += (double)((int)(((fsbw[ind] - cVal[ind]) * osts[ind]) / GIFT_COUP_BW_SZ)) * GIFT_COUP_BW_SZ;
            sys_coup_issu += (double)((int)(((fsbw[ind] - cVal[ind]) * osts[ind]) / GIFT_COUP_BW_SZ)) * GIFT_COUP_BW_SZ;
            //cout << "APP: " << base.app_id << endl;
            //cout << "ZOM: " << fsbw[ind] << endl;
            //cout << "RED: " << cVal[ind] << endl;
            //cout << "GOD: " << osts[ind] << endl;
            break;
          }
        }
      }
    }
    allocs.push_back(alloc);
  }

  numCouponsIssued.push_back(numCouponsIssued.back() + numCoups);
  valCouponsIssued.push_back(valCouponsIssued.back() + valCoups);

  // Determine effective storage system utilization	
  if (model.getObjValue() < 0) {
    runEffStorageSysUtil.push_back(-1 * model.getObjValue());
  }

  delete [] rInd;
  delete [] cInd;
  delete [] ele;
  delete [] alcs;
  delete [] osts;
  delete [] collb;
  delete [] fsbw;
  delete [] colub;
  delete [] obj;
  delete [] done;
}

void LnetMds::computeBwAllocationsBSIP(const size_t NUM_OSTS,
                                       const std::vector<std::vector<int>> &reqs,
                                       MapOstToAppAllocs_t &allocs)
{
  // Create a flat vector
  vector<int> flat;
  for (size_t i = 0; i < NUM_OSTS; i++) {
    for (auto req : reqs[i]) {
      flat.push_back(req);
    }
  }

  // Determine the number of applications doing I/O
  set<int> s = set<int>(flat.begin(), flat.end());
  vector<int> apps(s.begin(), s.end());
  const int numApps = (int)apps.size();

  // Determine the minimum bandwidth of each app after fair share
  double* minBw = NULL;
  minBw = new double[numApps];
  fill_n(minBw, numApps, numeric_limits<double>::infinity());

  for (size_t i = 0; i < NUM_OSTS; i++) {
    double alloc = 1.0 / (double)reqs[i].size();
    for (auto req : reqs[i]) {
      int ind = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      minBw[ind] = min(minBw[ind], alloc);
    }
  }

  // Determine effective storage system utilization and allocation of the bandwidth
  double totalUsedBw = 0.0;
  for (size_t i = 0; i < NUM_OSTS; i++) {
    double usedBw = 0.0;
    for (auto req : reqs[i]) {
      int ind = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      usedBw += minBw[ind];
    }
    totalUsedBw += usedBw;

    double xtraBw = 0.0;
    if ((ALLOC_XTRA_BW == true) && (ALLOC_X_BW_EQ == true)) {
      xtraBw = (1.0 - usedBw) / (double)reqs[i].size();
    }

    AppAllocs_t alloc;
    for (auto req : reqs[i]) {
      int ind = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      if ((ALLOC_XTRA_BW == true) && (ALLOC_X_BW_EQ == false)) {
        xtraBw = (1.0 - usedBw) * (minBw[ind] / usedBw);
      }
      alloc.push_back(make_tuple(req, minBw[ind] + xtraBw));
    }
    allocs.push_back(alloc);
  }

  runEffStorageSysUtil.push_back(totalUsedBw);

  delete [] minBw;
}

void LnetMds::computeBwAllocationsTSA(const size_t NUM_OSTS,
                                      const std::vector<std::vector<int>> &reqs,
                                      MapOstToAppAllocs_t &allocs)
{
  // Create a flat vector
  vector<int> flat;
  for (size_t i = 0; i < NUM_OSTS; i++) {
    for (auto req : reqs[i]) {
      flat.push_back(req);
    }
  }

  // Determine the number of applications doing I/O
  set<int> s = set<int>(flat.begin(), flat.end());
  vector<int> apps(s.begin(), s.end());
  const int numApps = (int)apps.size();

  unsigned int* appSize = NULL;
  appSize = new unsigned int[numApps];
  fill_n(appSize, numApps, 0);
  double meanSize = 0.0;

  // Determine the size of the applications
  for (size_t i = 0; i < NUM_OSTS; i++) {
    for (auto req : reqs[i]) {
      int ind = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      appSize[ind] += 1;
      meanSize += 1;
    }
  }

  /* cout << "NEW" << endl;
     for (int i = 0; i < numApps; i++) {
     cout << "App " << apps[i] << " has size " << appSize[i] << "." << endl;
     } */
  meanSize = meanSize / (double)numApps;
  // cout << "Mean size " << meanSize << endl;

  // Determine the minimum bandwidth of each app after fair share
  double* minBw = NULL;
  minBw = new double[numApps];
  fill_n(minBw, numApps, numeric_limits<double>::infinity());

  for (size_t i = 0; i < NUM_OSTS; i++) {
    double alloc = 1.0 / (double)reqs[i].size();
    for (auto req : reqs[i]) {
      int ind = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      minBw[ind] = min(minBw[ind], alloc);
    }
  }

  // Throttle some jobs' bandwidth
  bool* thtBw = NULL;
  thtBw = new bool[numApps];
  for (int i = 0; i < numApps; i++) {
    if (appSize[i] <= meanSize) {
      minBw[i] = minBw[i] * (1.0 - TSA_B_THRESHOLD);
      thtBw[i] = true;
    } else {
      minBw[i] = numeric_limits<double>::infinity();
      thtBw[i] = false;
    }
    // cout << "App " << apps[i] << " has b/w " << thtBw[i] << "." << endl;
  }

  // Increase bandwidth allocations of other jobs
  for (size_t i = 0; i < NUM_OSTS; i++) {
    int numJobs = 0;
    double usedBw = 0.0;
    for (auto req : reqs[i]) {
      int ind = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      if (thtBw[ind] == true) {
        numJobs += 1;
        usedBw += minBw[ind];
      }
    }

    double alloc = (1.0 - usedBw) / ((int)reqs[i].size() - numJobs);
    // cout << i << " " << alloc << endl;
    for (auto req : reqs[i]) {
      int ind = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      if (thtBw[ind] == false) {
        minBw[ind] = min(minBw[ind], alloc);
      }
    }
  }

  // Determine effective storage system utilization and allocation of the bandwidth
  double totalUsedBw = 0.0;
  for (size_t i = 0; i < NUM_OSTS; i++) {
    double usedBw = 0.0;
    for (auto req : reqs[i]) {
      int ind = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      usedBw += minBw[ind];
    }
    totalUsedBw += usedBw;

    double xtraBw = 0.0;
    if ((ALLOC_XTRA_BW == true) && (ALLOC_X_BW_EQ == true)) {
      xtraBw = (1.0 - usedBw) / (double)reqs[i].size();
    }

    AppAllocs_t alloc;
    for (auto req : reqs[i]) {
      int ind = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      if ((ALLOC_XTRA_BW == true) && (ALLOC_X_BW_EQ == false)) {
        xtraBw = (1.0 - usedBw) * (minBw[ind] / usedBw);
      }
      alloc.push_back(make_tuple(req, minBw[ind] + xtraBw));
    }
    allocs.push_back(alloc);
  }

  runEffStorageSysUtil.push_back(totalUsedBw);

  delete [] appSize;
  delete [] thtBw;
  delete [] minBw;
}

void LnetMds::computeBwAllocationsESA(const size_t NUM_OSTS,
                                      const std::vector<std::vector<int>> &reqs,
                                      MapOstToAppAllocs_t &allocs)
{
  // Create a flat vector
  vector<int> flat;
  for (size_t i = 0; i < NUM_OSTS; i++) {
    for (auto req : reqs[i]) {
      flat.push_back(req);
    }
  }

  // Determine the number of applications doing I/O
  set<int> s = set<int>(flat.begin(), flat.end());
  vector<int> apps(s.begin(), s.end());
  const int numApps = (int)apps.size();

  unsigned int* appSize = NULL;
  appSize = new unsigned int[numApps];
  fill_n(appSize, numApps, 0);
  double meanSize = 0.0;

  // Determine the size of the applications
  for (size_t i = 0; i < NUM_OSTS; i++) {
    for (auto req : reqs[i]) {
      int ind = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      appSize[ind] += 1;
      meanSize += 1;
    }
  }

  meanSize = meanSize / (double)numApps;

  // Determine the minimum bandwidth of each app after fair share
  double* minBw = NULL;
  minBw = new double[numApps];
  fill_n(minBw, numApps, numeric_limits<double>::infinity());

  for (size_t i = 0; i < NUM_OSTS; i++) {
    double alloc = 1.0 / (double)reqs[i].size();
    for (auto req : reqs[i]) {
      int ind = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      minBw[ind] = min(minBw[ind], alloc);
    }
  }

  // Throttle some jobs' bandwidth
  bool* thtBw = NULL;
  thtBw = new bool[numApps];
  for (int i = 0; i < numApps; i++) {
    if (appSize[i] > meanSize) {
      minBw[i] = minBw[i] * (1.0 - ESA_B_THRESHOLD);
      thtBw[i] = true;
    } else {
      minBw[i] = numeric_limits<double>::infinity();
      thtBw[i] = false;
    }
    // cout << "App " << apps[i] << " has b/w " << thtBw[i] << "." << endl;
  }

  // Increase bandwidth allocations of other jobs
  for (size_t i = 0; i < NUM_OSTS; i++) {
    int numJobs = 0;
    double usedBw = 0.0;
    for (auto req : reqs[i]) {
      int ind = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      if (thtBw[ind] == true) {
        numJobs += 1;
        usedBw += minBw[ind];
      }
    }

    double alloc = (1.0 - usedBw) / ((int)reqs[i].size() - numJobs);
    // cout << i << " " << alloc << endl;
    for (auto req : reqs[i]) {
      int ind = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      if (thtBw[ind] == false) {
        minBw[ind] = min(minBw[ind], alloc);
      }
    }
  }

  // Determine effective storage system utilization and allocation of the bandwidth
  double totalUsedBw = 0.0;
  for (size_t i = 0; i < NUM_OSTS; i++) {
    double usedBw = 0.0;
    for (auto req : reqs[i]) {
      int ind = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      usedBw += minBw[ind];
    }
    totalUsedBw += usedBw;

    double xtraBw = 0.0;
    if ((ALLOC_XTRA_BW == true) && (ALLOC_X_BW_EQ == true)) {
      xtraBw = (1.0 - usedBw) / (double)reqs[i].size();
    }

    AppAllocs_t alloc;
    for (auto req : reqs[i]) {
      int ind = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      if ((ALLOC_XTRA_BW == true) && (ALLOC_X_BW_EQ == false)) {
        xtraBw = (1.0 - usedBw) * (minBw[ind] / usedBw);
      }
      alloc.push_back(make_tuple(req, minBw[ind] + xtraBw));
    }
    allocs.push_back(alloc);
  }

  runEffStorageSysUtil.push_back(totalUsedBw);

  delete [] appSize;
  delete [] thtBw;
  delete [] minBw;
}

void LnetMds::computeBwAllocationsTMF(const size_t NUM_OSTS,
                                      const std::vector<std::vector<int>> &reqs,
                                      MapOstToAppAllocs_t &allocs)
{
  // Create a flat vector
  vector<int> flat;
  for (size_t i = 0; i < NUM_OSTS; i++) {
    for (auto req : reqs[i]) {
      flat.push_back(req);
    }
  }

  // Determine the number of applications doing I/O
  set<int> s = set<int>(flat.begin(), flat.end());
  vector<int> apps(s.begin(), s.end());
  const int numApps = (int)apps.size();

  unsigned int* appFreq = NULL;
  appFreq = new unsigned int[numApps];
  double meanFreq = 0.0;

  // Update the frequencies of the applications
  for (int i = 0; i < numApps; i++) {
    bool found = false;
    for (auto &entry : freqDatabase) {
      if (entry.app == (unsigned int)apps[i]) {
        entry.num++;
        appFreq[i] = entry.num;
        meanFreq += entry.num;
        found = true;
        break;
      }
    }
    if (found == false) {
      freqDatabase.push_back(AppFreq_t());
      freqDatabase.back().app = (unsigned int)apps[i];
      freqDatabase.back().num = 1;
      appFreq[i] = 1;
      meanFreq += 1;
    }
  }

  meanFreq = meanFreq / (double)numApps;

  // Determine the minimum bandwidth of each app after fair share
  double* minBw = NULL;
  minBw = new double[numApps];
  fill_n(minBw, numApps, numeric_limits<double>::infinity());

  for (size_t i = 0; i < NUM_OSTS; i++) {
    double alloc = 1.0 / (double)reqs[i].size();
    for (auto req : reqs[i]) {
      int ind = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      minBw[ind] = min(minBw[ind], alloc);
    }
  }

  // Throttle some jobs' bandwidth
  bool* thtBw = NULL;
  thtBw = new bool[numApps];
  for (int i = 0; i < numApps; i++) {
    if (appFreq[i] > meanFreq) {
      minBw[i] = minBw[i] * (1.0 - TMF_B_THRESHOLD);
      thtBw[i] = true;
    } else {
      minBw[i] = numeric_limits<double>::infinity();
      thtBw[i] = false;
    }
    // cout << "App " << apps[i] << " has b/w " << thtBw[i] << "." << endl;
  }

  // Increase bandwidth allocations of other jobs
  for (size_t i = 0; i < NUM_OSTS; i++) {
    int numJobs = 0;
    double usedBw = 0.0;
    for (auto req : reqs[i]) {
      int ind = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      if (thtBw[ind] == true) {
        numJobs += 1;
        usedBw += minBw[ind];
      }
    }

    double alloc = (1.0 - usedBw) / ((int)reqs[i].size() - numJobs);
    // cout << i << " " << alloc << endl;
    for (auto req : reqs[i]) {
      int ind = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      if (thtBw[ind] == false) {
        minBw[ind] = min(minBw[ind], alloc);
      }
    }
  }

  // Determine effective storage system utilization and allocation of the bandwidth
  double totalUsedBw = 0.0;
  for (size_t i = 0; i < NUM_OSTS; i++) {
    double usedBw = 0.0;
    for (auto req : reqs[i]) {
      int ind = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      usedBw += minBw[ind];
    }
    totalUsedBw += usedBw;

    double xtraBw = 0.0;
    if ((ALLOC_XTRA_BW == true) && (ALLOC_X_BW_EQ == true)) {
      xtraBw = (1.0 - usedBw) / (double)reqs[i].size();
    }

    AppAllocs_t alloc;
    for (auto req : reqs[i]) {
      int ind = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      if ((ALLOC_XTRA_BW == true) && (ALLOC_X_BW_EQ == false)) {
        xtraBw = (1.0 - usedBw) * (minBw[ind] / usedBw);
      }
      alloc.push_back(make_tuple(req, minBw[ind] + xtraBw));
    }
    allocs.push_back(alloc);
  }

  runEffStorageSysUtil.push_back(totalUsedBw);

  delete [] appFreq;
  delete [] thtBw;
  delete [] minBw;
}


void LnetMds::computeBwAllocationsRND(const size_t NUM_OSTS,
                                      const std::vector<std::vector<int>> &reqs,
                                      MapOstToAppAllocs_t &allocs)
{
  // Create a flat vector
  vector<int> flat;
  for (size_t i = 0; i < NUM_OSTS; i++) {
    for (auto req : reqs[i]) {
      flat.push_back(req);
    }
  }

  // Determine the number of applications doing I/O
  set<int> s = set<int>(flat.begin(), flat.end());
  vector<int> apps(s.begin(), s.end());
  const int numApps = (int)apps.size();

  // Determine the minimum bandwidth of each app after fair share
  double* minBw = NULL;
  minBw = new double[numApps];
  fill_n(minBw, numApps, numeric_limits<double>::infinity());

  for (size_t i = 0; i < NUM_OSTS; i++) {
    double alloc = 1.0 / (double)reqs[i].size();
    for (auto req : reqs[i]) {
      int ind = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      minBw[ind] = min(minBw[ind], alloc);
    }
  }

  // Throttle some jobs' bandwidth
  bool* thtBw = NULL;
  thtBw = new bool[numApps];
  srand(numApps);
  for (int i = 0; i < numApps; i++) {
    if (rand() > (RAND_MAX / 2)) {
      minBw[i] = minBw[i] * (1.0 - TMF_B_THRESHOLD);
      thtBw[i] = true;
    } else {
      minBw[i] = numeric_limits<double>::infinity();
      thtBw[i] = false;
    }
    // cout << "App " << apps[i] << " has b/w " << thtBw[i] << "." << endl;
  }

  // Increase bandwidth allocations of other jobs
  for (size_t i = 0; i < NUM_OSTS; i++) {
    int numJobs = 0;
    double usedBw = 0.0;
    for (auto req : reqs[i]) {
      int ind = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      if (thtBw[ind] == true) {
        numJobs += 1;
        usedBw += minBw[ind];
      }
    }

    double alloc = (1.0 - usedBw) / ((int)reqs[i].size() - numJobs);
    // cout << i << " " << alloc << endl;
    for (auto req : reqs[i]) {
      int ind = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      if (thtBw[ind] == false) {
        minBw[ind] = min(minBw[ind], alloc);
      }
    }
  }

  // Determine effective storage system utilization and allocation of the bandwidth
  double totalUsedBw = 0.0;
  for (size_t i = 0; i < NUM_OSTS; i++) {
    double usedBw = 0.0;
    for (auto req : reqs[i]) {
      int ind = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      usedBw += minBw[ind];
    }
    totalUsedBw += usedBw;

    double xtraBw = 0.0;
    if ((ALLOC_XTRA_BW == true) && (ALLOC_X_BW_EQ == true)) {
      xtraBw = (1.0 - usedBw) / (double)reqs[i].size();
    }

    AppAllocs_t alloc;
    for (auto req : reqs[i]) {
      int ind = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      if ((ALLOC_XTRA_BW == true) && (ALLOC_X_BW_EQ == false)) {
        xtraBw = (1.0 - usedBw) * (minBw[ind] / usedBw);
      }
      alloc.push_back(make_tuple(req, minBw[ind] + xtraBw));
    }
    allocs.push_back(alloc);
  }

  runEffStorageSysUtil.push_back(totalUsedBw);

  delete [] thtBw;
  delete [] minBw;
}

void LnetMds::computeBwAllocationsMBW(const size_t NUM_OSTS,
                                      const std::vector<std::vector<int>> &reqs,
                                      MapOstToAppAllocs_t &allocs)
{
  // Create a flat vector
  vector<int> flat;
  for (size_t i = 0; i < NUM_OSTS; i++) {
    for (auto req : reqs[i]) {
      flat.push_back(req);
    }
  }

  // Determine the number of applications doing I/O
  set<int> s = set<int>(flat.begin(), flat.end());
  vector<int> apps(s.begin(), s.end());
  const int numApps = (int)apps.size();

  // Row Order matrix
  const bool cOrd = false;

  // Row Indices
  int *rInd = NULL;
  rInd = new int[NUM_OSTS * numApps];
  for (size_t i = 0; i < NUM_OSTS; i++) {
    for (int j = 0; j < numApps; j++) {
      rInd[(i * numApps) + j] = i;
    }
  }

  // Col Indices
  int* cInd = NULL;
  cInd = new int[NUM_OSTS * numApps];
  for (size_t i = 0; i < NUM_OSTS; i++) {
    for (int j = 0; j < numApps; j++) {
      cInd[(i * numApps) + j] = j;
    }
  }

  // Constraints matrix
  double* ele = NULL;
  ele = new double[NUM_OSTS * numApps];
  memset(ele, 0, (NUM_OSTS * numApps) * sizeof(double));
  for (size_t i = 0; i < NUM_OSTS; i++) {
    for (auto req : reqs[i]) {
      int app = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      ele[(i * numApps) + app] += 1;
    }
  }

  // Number of elements in the constraints matrix
  CoinBigIndex numels = NUM_OSTS * numApps;

  // Make a matrix of constraints
  const CoinPackedMatrix matrix(cOrd, rInd, cInd, ele, numels);

  // Lower allocation bound for each app
  double* collb = NULL;
  collb = new double[numApps];
  memset(collb, 0, numApps * sizeof(double));

  // Upper allocation bound for each app
  double* colub = NULL;
  colub = new double[numApps];
  fill_n(colub, numApps, 1);

  // The cooefficients of the minimum objective function
  double* obj = NULL;
  obj = new double[numApps];
  memset(obj, 0, numApps * sizeof(double));
  for (auto f : flat) {
    int app = (int)(find(apps.begin(), apps.end(), f) - apps.begin());
    obj[app] -= 1;
  }

  // The upper and lower bounds of each OST
  double rowlb[NUM_OSTS], rowub[NUM_OSTS];
  memset(rowlb, 0, NUM_OSTS * sizeof(double));
  fill_n(rowub, NUM_OSTS, 1);

  ClpSimplex model;
  model.loadProblem(matrix, collb, colub, obj, rowlb, rowub, NULL);
  model.primal();
  double* cVal = model.primalColumnSolution();

  // Determine allocation of the bandwidth
  for (size_t i = 0; i < NUM_OSTS; i++) {
    double usedBw = 0.0;
    for (auto req : reqs[i]) {
      int ind = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      usedBw += cVal[ind];
    }

    double xtraBw = 0.0;
    if ((ALLOC_XTRA_BW == true) && (ALLOC_X_BW_EQ == true)) {
      xtraBw = (1.0 - usedBw) / (double)reqs[i].size();
    }

    AppAllocs_t alloc;
    for (auto req : reqs[i]) {
      int ind = (int)(find(apps.begin(), apps.end(), req) - apps.begin());
      if ((ALLOC_XTRA_BW == true) && (ALLOC_X_BW_EQ == false)) {
        xtraBw = (1.0 - usedBw) * (cVal[ind] / usedBw);
      }
      alloc.push_back(make_tuple(req, cVal[ind] + xtraBw));
    }
    allocs.push_back(alloc);
  }

  // Determine effective storage system utilization	
  if (model.getObjValue() < 0) {
    runEffStorageSysUtil.push_back(-1 * model.getObjValue());
  }

  delete [] rInd;
  delete [] cInd;
  delete [] ele;
  delete [] collb;
  delete [] colub;
  delete [] obj;
}

void LnetMds::printStats()
{
#if 0
  for (auto entry : freqDatabase) {
     cout << "App " << entry.app << " appears " << entry.num << " times." << endl;
  }
#endif

  for (auto base : appDatabase) {
    cout << base.app_id << endl;
    cout << base.bw_issued << endl;
    cout << base.bw_redeemed << endl;
  }

  for (auto num : numCouponsIssued) {
    cout << num << endl;
  }

  for (auto val : valCouponsIssued) {
    cout << val << endl;
  }

  double avgEffStorageSysUtil = 0.0;
  for (auto bw : runEffStorageSysUtil) {
    avgEffStorageSysUtil += bw;
  }
  avgEffStorageSysUtil = 100.0 * avgEffStorageSysUtil / (double) runEffStorageSysUtil.size();
  avgEffStorageSysUtil /= (double) this->_osts.size();

  cout << "Effective System Util: " << avgEffStorageSysUtil << endl;
  cout << "Effective System B/w: " << this->getEffectiveSysBw() << endl;
}


#if STANDALONE
// Implement the policies
void getBwAllocs(Policy_t policy, vector<vector<int>> &reqs, MapOstToAppAllocs_t &allocs) {

	// Assert that all the OSTs are covered
	assert(reqs.size() == NUM_OSTS);

	// Perform POFS bandwidth allocation
  if (policy == POFS) {
    computeBwAllocationsPOFS(reqs, allocs);
  }

	// Perform BSIP bandwidth allocation	
  if (policy == BSIP) {
    computeBwAllocationsBSIP(reqs, allocs);
  }

	// Perform TSA bandwidth allocation
  if (policy == TSA) {
    computeBwAllocationsTSA(reqs, allocs);
  }

	// Perform ESA bandwidth allocation
  if (policy == ESA) {
    computeBwAllocationsESA(reqs, allocs);
  }

	// Perform TMF bandwidth allocation
  if (policy == TMF) {
    computeBwAllocationsTMF(reqs, allocs);
  }

	// Perform RND bandwidth allocation
  if (policy == RND) {
    computeBwAllocationsRND(reqs, allocs);
  }

	// Perform MBW bandwidth allocation
  if (policy == MBW) {
    computeBwAllocationsMBW(reqs, allocs);
  }

	// Perform GIFT bandwidth allocation
  if (policy == GIFT) {
    computeBwAllocationsGIFT(reqs, allocs);
  }
}

int main () {

	// OST 1 has has 5 apps doing I/O with integer IDs
	vector<int> ost1;
	ost1.push_back(2);
	ost1.push_back(6);
	ost1.push_back(1);
	ost1.push_back(9);
	ost1.push_back(3);

	// OST 2 has has 3 apps doing I/O with integer IDs
	vector<int> ost2;
	ost2.push_back(2);
	ost2.push_back(1);
	ost2.push_back(3);

	// Include empty vector for osts with no requests
	vector<int> ost3;

	vector<int> ost4;
	ost4.push_back(2);
	ost4.push_back(6);
	ost4.push_back(9);

	vector<int> ost5;
	ost5.push_back(1);
	ost5.push_back(9);

	// Send Format:
	// { ost1: {app1, app2, app3, ...}, ost2: {app2, app4, ...}, ...}
	vector<vector<int>> reqs1;
	reqs1.push_back(ost3);
	reqs1.push_back(ost3);
	reqs1.push_back(ost3);

	vector<vector<int>> reqs2;
	reqs2.push_back(ost4);
	reqs2.push_back(ost3);
	reqs2.push_back(ost5);

	vector<vector<int>> reqs3;
	reqs3.push_back(ost5);
	reqs3.push_back(ost2);
	reqs3.push_back(ost4);

	vector<vector<int>> reqs4;
	reqs4.push_back(ost1);
	reqs4.push_back(ost3);
	reqs4.push_back(ost2);

	vector<vector<int>> reqs5;
	reqs5.push_back(ost1);
	reqs5.push_back(ost4);
	reqs5.push_back(ost5);

	// Receive Format:
	// { {appID, b/w alloc}, {appID, b/w alloc}, ...}
	// b/w alloc is given as a frcation out of 1
	// multiply it to the max OST b/w to get the absolute number

	// Set policy
	Policy_t policy = GIFT;

	if (policy == GIFT) {
		numCouponsIssued.push_back(0);
		valCouponsIssued.push_back(0.0);
	}

	for (int i = 0; i < 5; i++) {	
		MapOstToAppAllocs_t allocs;
		switch(i) {
			case 0: getBwAllocs(policy, reqs1, allocs);
				break;
			case 1: getBwAllocs(policy, reqs2, allocs);
				break;
			case 2: getBwAllocs(policy, reqs3, allocs);
				break;
			case 3: getBwAllocs(policy, reqs4, allocs);
				break;
			case 4: getBwAllocs(policy, reqs5, allocs);
				break;
		}
		cout << "Interval " << i << endl;

		/* for (unsigned int j = 0; j < allocs.size(); j++) {
			cout << "OST " << j << endl;
			for (unsigned int i = 0; i < allocs[j].size(); i++) {
				cout << "App " << int(get<0>(allocs[j][i])) << " bw alloc = " << get<1>(allocs[j][i]) << endl;
			}
		} */
	}

	/* for (auto entry : freqDatabase) {
		cout << "App " << entry.app << " appears " << entry.num << " times." << endl;
	} */

	/*
	for (auto base : appDatabase) {
		cout << base.app_id << endl;
		cout << base.bw_issued << endl;
		cout << base.bw_redeemed << endl;
	}

	if (policy == GIFT) {
		for (auto num : numCouponsIssued) {
			 cout << num << endl;
		}

		for (auto val : valCouponsIssued) {
			 cout << val << endl;
		}
	}*/

	double avgEffStorageSysUtil = 0.0;
	for (auto bw : runEffStorageSysUtil) {
		 avgEffStorageSysUtil += bw;
	}
	avgEffStorageSysUtil = avgEffStorageSysUtil / (double)runEffStorageSysUtil.size() / NUM_OSTS * 100.0;

	cout << "Effective Storage System Utilization: " << avgEffStorageSysUtil << '%' << endl;

	return 0;
}
#endif
