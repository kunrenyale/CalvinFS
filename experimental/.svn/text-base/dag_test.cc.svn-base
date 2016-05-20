// Author: Alexander Thomson <thomson@cs.yale.edu>
//

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <atomic>

#include "common/utils.h"

#define WRITESETSIZE 10
#define READSETSIZE 10

DEFINE_int32(total_txns, 1000000, "total txns to add to dag");

struct Node {
  Node() {
    for (int i = 0; i < READSETSIZE; i++) {
      readset_[i] = rand() % 1000000;
    }
    for (int i = 0; i < WRITESETSIZE; i++) {
      writeset_[i] = rand() % 1000000;
    }
  }
  int readset_[READSETSIZE];
  int writeset_[WRITESETSIZE];
  Node* deps_[READSETSIZE];
};

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);

  Node* records[1000000];
  int cost[1000000];
  for (int i = 0; i < 1000000; i++) {
    records[i] = NULL;
	cost[i] = 0;
  }

  double start = GetTime();

  // Add nodes.
  for (int a = 0; a < FLAGS_total_txns; a++) {
    Node* node = new Node();
    // Set node dependencies for reads
    for (int i = 0; i < READSETSIZE; i++) {
      node->deps_[i] = records[node->readset_[i]];
    }
    // Update record ptrs to account for new writes.
    for (int i = 0; i < WRITESETSIZE; i++) {
      records[node->writeset_[i]] = node;
    }
  }

  double end = GetTime();
  LOG(ERROR) << "added " << FLAGS_total_txns << " dag nodes in "
             << (end - start) * 1000 << " ms";
  return 0;
}

