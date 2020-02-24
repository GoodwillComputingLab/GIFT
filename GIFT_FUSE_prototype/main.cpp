#include <set>
#include <vector>
#include <cassert>
#include <iostream>
#include <algorithm>
#include "ClpSimplex.hpp"

#define NUM_OSTS 3
#define OST_IO_BW 300

#define RDMP_RATE_THRES 1.0
#define MAX_THRTL_THRES 0.1
#define SAMPLES_HORIZON 100

enum Policy_t {
  POFS,
  GIFT,
  MBW
};

using AppAlloc_t = std::tuple<int , double>;
using AppAllocs_t = std::vector<AppAlloc_t>;
using MapOstToAppAllocs_t = std::vector<AppAllocs_t>;

using Coupon_t = std::tuple<double, int>;
struct AppCoup_t {
  int app;
  std::vector<Coupon_t> coupons;
};
std::vector<AppCoup_t> coupDatabase;

using AppData_t = std::tuple<int, int, double, double, double, double, AppCoup_t>;
std::vector<AppData_t> rdmpDatabase;

std::vector<double> effectiveSysBw;

void getBwAllocs(Policy_t policy, std::vector<std::vector<int>> &reqs, MapOstToAppAllocs_t &allocs)
{
  // Assert that all the OSTs are covered
  assert (reqs.size() == NUM_OSTS);

  if (policy == POFS) {
    return;
  }

  // Create a flat vector
  std::vector<int> flat;
  for (size_t i = 0; i < NUM_OSTS; i++) {
    for (auto req : reqs[i]) {
      flat.push_back(req);
    }
    //flat.push_back(-1);
  }

  std::set<int> s = std::set<int>(flat.begin(), flat.end());
  std::vector<int> apps(s.begin(), s.end());
  const int numApps = apps.size();

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
      int app = find(apps.begin(), apps.end(), req) - apps.begin();
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
  double* fsbw = NULL;
  fsbw = new double[numApps];

  double* alcs = new double[numApps];
  std::fill_n(alcs, numApps, std::numeric_limits<double>::infinity());
  int* osts = new int[numApps];
  memset(osts, 0, numApps * sizeof(int));
  if (policy == GIFT) {
    std::vector<bool> apTh;// = {true, false, true, false, true};
    bool found = false;
    for (auto app : apps) {
      found = false;
      for (auto base : rdmpDatabase) {
        if (std::get<0>(base) == app) {
          if ((std::get<4>(base) + std::get<5>(base)) >= RDMP_RATE_THRES) {
            apTh.push_back(true);
          } else {
            apTh.push_back(false);
          }
          found = true;
          break;
        }
      }
      if (!found) {
        coupDatabase.push_back(AppCoup_t());
        coupDatabase.back().app = app;
        rdmpDatabase.push_back(std::make_tuple(app, 0, 0.0, 0.0, 0.0, std::numeric_limits<double>::infinity(), coupDatabase.back()));
        apTh.push_back(true);
      }
    }
    for (int i = 0; i < NUM_OSTS; i++) {
      double alc = 1.0 / double(reqs[i].size());
      for (auto req : reqs[i]) {
        int app = find(apps.begin(), apps.end(), req) - apps.begin();
        alcs[app] = std::min(alcs[app], alc);
        osts[app] += 1;
      }
    }
    double efbw[NUM_OSTS];
    memset(efbw, 0.0, NUM_OSTS * sizeof(double));
    for (int i = 0; i < NUM_OSTS; i++) {
      for (auto req : reqs[i]) {
        int app = find(apps.begin(), apps.end(), req) - apps.begin();
        efbw[i] += alcs[app];
      }
      //std::cout << efbw[i] << std::endl;
    }
    for (int i = 0; i < numApps; i++) {
      //std::cout << alcs[i] << std::endl;
      //std::cout << osts[i] << std::endl;
    }
    std::vector<double> bwRd;
    int index = 0;
    for (auto app : apps) {
      double bwRq = 0.0;
      for (int k = 0; k < rdmpDatabase.size(); k++) {
      	if (std::get<0>(rdmpDatabase[k]) == app) {
      	  bwRq = std::get<2>(rdmpDatabase[k]) - std::get<3>(rdmpDatabase[k]);
      	  break;
      	}
      }
      if (bwRq == 0.0) {
      	bwRd.push_back(0.0);
      	index += 1;
      	continue;
      }
      double bwAv = std::numeric_limits<double>::infinity();
      for (int i = 0; i < NUM_OSTS; i++) {
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
      	} else {
      	  bwAv = std::min(bwAv, efbw[i] / numReqs);
      	}
      }
      if (bwAv == 0.0) {
        bwRd.push_back(0.0);
        index += 1;
        continue;
      }
      double bwGv = std::min(bwRq, bwAv);
      for (int k = 0; k < rdmpDatabase.size(); k++) {
	if (std::get<0>(rdmpDatabase[k]) == app) {
	  double bwG = bwGv;
	  for (auto ind = std::get<6>(rdmpDatabase[k]).coupons.begin(); ind != std::get<6>(rdmpDatabase[k]).coupons.end();) {
	    if ((bwG < std::get<0>(*ind)) && (1 <= std::get<1>(*ind))) {
	      std::get<0>(*ind) -= bwG;
	      bwG -= bwG;
	      ++ind;
	    } else {
	      bwG -= std::get<0>(*ind);
	      ind = std::get<6>(rdmpDatabase[k]).coupons.erase(ind);
	    }
	    if (bwG <= 0.0) {
	      break;
	    }
	  }
	  std::get<3>(rdmpDatabase[k]) += bwGv;
	  std::get<4>(rdmpDatabase[k]) = std::get<3>(rdmpDatabase[k]) / std::get<2>(rdmpDatabase[k]);
	  break;
	}
      }
      bwRd.push_back(bwGv * osts[index]);
      index += 1;
    }

    std::fill_n(collb, numApps, std::numeric_limits<double>::infinity());
    std::fill_n(fsbw, numApps, std::numeric_limits<double>::infinity());

    for (size_t i = 0; i < NUM_OSTS; i++) {
      double alloc = 1.0 / double(reqs[i].size());
      for (auto req : reqs[i]) {
        int app = find(apps.begin(), apps.end(), req) - apps.begin();
        if (apTh[app]) {
          collb[app] = std::min(collb[app], (alloc * (1 - MAX_THRTL_THRES)) + bwRd[app]);
        } else {
          collb[app] = std::min(collb[app], alloc + bwRd[app]);
        }
        fsbw[app] = std::min(fsbw[app], alloc + bwRd[app]);
      }
    }
  } else if (policy == MBW) {
    memset(collb, 0, numApps * sizeof(double));
  }

  // Upper allocation bound for each app
  double* colub = NULL;
  colub = new double[numApps];
  std::fill_n(colub, numApps, 1);

  // The cooefficients of the minimum objective function
  double* obj = NULL;
  obj = new double[numApps];
  memset(obj, 0, numApps * sizeof(double));
  for (auto f : flat) {
    int app = find(apps.begin(), apps.end(), f) - apps.begin();
    obj[app] -= 1;
  }

  // The upper and lower bounds of each OST
  double rowlb[NUM_OSTS], rowub[NUM_OSTS];
  std::fill_n(rowub, NUM_OSTS, 1);

  ClpSimplex model;
  model.loadProblem(matrix, collb, colub, obj, rowlb, rowub, NULL);
  model.primal();
  double* cVal = model.primalColumnSolution();

  for (size_t j = 0; j < NUM_OSTS; j++) {
    AppAllocs_t alloc;
    for (int i = 0; i < numApps; i++) {
      alloc.push_back(std::make_tuple(apps[i], cVal[i]));
      if ((j == 0) && (policy == GIFT) && (cVal[i] <= fsbw[i])) {
        for (int k = 0; k < rdmpDatabase.size(); k++) {
          if (std::get<0>(rdmpDatabase[k]) == apps[i]) {
            std::get<1>(rdmpDatabase[k]) += 1;
            std::get<2>(rdmpDatabase[k]) += ((fsbw[i] - cVal[i]) * osts[i]);
            std::get<4>(rdmpDatabase[k])  = std::get<3>(rdmpDatabase[k]) / std::get<2>(rdmpDatabase[k]);
            std::get<5>(rdmpDatabase[k])  = std::sqrt((2 * std::log10(SAMPLES_HORIZON)) / std::get<1>(rdmpDatabase[k]));
            std::get<6>(rdmpDatabase[k]).coupons.push_back(std::make_tuple((fsbw[i] - cVal[i]), osts[i]));
            /* for (auto coup : std::get<6>(rdmpDatabase[k]).coupons) {
                 std::cout << "COUPON: " << std::get<0>(coup) << std::endl;
                 std::cout << "COUPON: " << std::get<1>(coup) << std::endl;
               }*/
            break;
          }
        }
      }
    }
    allocs.push_back(alloc);
  }

  if (model.getObjValue() < 0) {
    effectiveSysBw.push_back(model.getObjValue() * -OST_IO_BW);
  }

  delete [] alcs;
  delete [] osts;
  delete [] rInd;
  delete [] cInd;
  delete [] ele;
  delete [] collb;
  delete [] colub;
  delete [] obj;
}

int main ()
{

  // OST 1 has has 5 apps doing I/O with integer IDs
  std::vector<int> ost1;
  ost1.push_back(2);
  ost1.push_back(6);
  ost1.push_back(1);
  ost1.push_back(9);
  ost1.push_back(3);

  // OST 2 has has 3 apps doing I/O with integer IDs
  std::vector<int> ost2;
  ost2.push_back(2);
  ost2.push_back(1);
  ost2.push_back(3);

  // Include empty vector for osts with no requests
  std::vector<int> ost3;

  std::vector<int> ost4;
  ost4.push_back(2);
  ost4.push_back(6);
  ost4.push_back(9);

  std::vector<int> ost5;
  ost5.push_back(1);
  ost5.push_back(9);

  // Send Format:
  // { ost1: {app1, app2, app3, ...}, ost2: {app2, app4, ...}, ...}
  std::vector<std::vector<int>> reqs1;
  reqs1.push_back(ost3);
  reqs1.push_back(ost3);
  reqs1.push_back(ost3);

  std::vector<std::vector<int>> reqs2;
  reqs2.push_back(ost4);
  reqs2.push_back(ost3);
  reqs2.push_back(ost5);

  std::vector<std::vector<int>> reqs3;
  reqs3.push_back(ost5);
  reqs3.push_back(ost2);
  reqs3.push_back(ost4);

  std::vector<std::vector<int>> reqs4;
  reqs4.push_back(ost1);
  reqs4.push_back(ost3);
  reqs4.push_back(ost2);

  std::vector<std::vector<int>> reqs5;
  reqs5.push_back(ost1);
  reqs5.push_back(ost4);
  reqs5.push_back(ost5);

  // Receive Format:
  // { {appID, b/w alloc}, {appID, b/w alloc}, ...}
  // b/w alloc is given as a frcation out of 1
  // multiply it to the max OST b/w to get the absolute number

  // Set policy
  Policy_t policy = GIFT;

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
    /*std::cout << "Interval :" << i << std::endl;
    for (int i = 0; i < allocs[0].size(); i++) {
      std::cout << "App " << int(std::get<0>(allocs[0][i])) << " bandwidth allocation = " << std::get<1>(allocs[0][i]) << std::endl;
    }*/
  }

  /*for (int k = 0; k < rdmpDatabase.size(); k++) {
    std:: cout << std::get<0>(rdmpDatabase[k]) << std::endl;
    std:: cout << std::get<1>(rdmpDatabase[k]) << std::endl;
    std:: cout << std::get<2>(rdmpDatabase[k]) << std::endl;
    std:: cout << std::get<3>(rdmpDatabase[k]) << std::endl;
    std:: cout << std::get<4>(rdmpDatabase[k]) << std::endl;
    std:: cout << std::get<5>(rdmpDatabase[k]) << std::endl;
  }*/

  double effectiveSystemBandwidth = 0.0;
  for (auto bw : effectiveSysBw) {
    effectiveSystemBandwidth += bw;
  }
  effectiveSystemBandwidth = effectiveSystemBandwidth / effectiveSysBw.size();

  std::cout << "Effective System Bandwidth: " << effectiveSystemBandwidth << std::endl;

  return 0;
}
