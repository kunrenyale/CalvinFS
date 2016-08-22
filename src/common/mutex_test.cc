// Author: Alexander Thomson <thomson@cs.yale.edu>
//

#include "common/mutex.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <atomic>

#include "common/utils.h"

using std::atomic;

DEFINE_bool(benchmark, false, "Run benchmarks instead of unit tests.");

void BenchmarkMutex() {
  Mutex m;
  double start = GetTime();
  for (int i = 0; i < 1000000; i++) {
    Lock l(&m);
  }
  LOG(ERROR) << "Mutex lock/unlock:        "
             << (GetTime() - start) * 1000 << " ns";
}

void BenchmarkMutexRW() {
  MutexRW m;
  double start = GetTime();
  for (int i = 0; i < 1000000; i++) {
    ReadLock l(&m);
  }
  LOG(ERROR) << "MutexRW readlock/unlock:  "
             << (GetTime() - start) * 1000 << " ns";

  start = GetTime();
  for (int i = 0; i < 1000000; i++) {
    WriteLock l(&m);
  }
  LOG(ERROR) << "MutexRW writelock/unlock: "
             << (GetTime() - start) * 1000 << " ns";
}

void BenchmarkIncrement() {
  atomic<int> x(0);
  double start = GetTime();
  for (int i = 0; i < 1000000; i++) {
    ++x;
  }
  LOG(ERROR) << "atomic<int>::++:       "
             << (GetTime() - start) * 1000 << " ns";
}

void BenchmarkCAS() {
  std::atomic_int x;
  x = 0;
  int y = 0;
  double start = GetTime();
  for (int i = 0; i < 1000000; i++) {
    x.compare_exchange_strong(y, i+1);
  }
  LOG(ERROR) << "atomic<uint64> CAS:       "
             << (GetTime() - start) * 1000 << " ns";

  start = GetTime();
  for (int i = 0; i < 1000000; i++) {
    x.compare_exchange_strong(y, i);
  }
  LOG(ERROR) << "atomic<uint64> CAS:       "
             << (GetTime() - start) * 1000 << " ns";
}

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  if (FLAGS_benchmark) {
    BenchmarkMutex();
    BenchmarkMutexRW();
    BenchmarkCAS();
    BenchmarkIncrement();
    return 0;
  }
  return RUN_ALL_TESTS();
}

