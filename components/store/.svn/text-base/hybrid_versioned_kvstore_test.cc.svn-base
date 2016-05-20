// Author: Kun Ren <kun@cs.yale.edu>
//

#include "components/store/hybrid_versioned_kvstore.h"

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <glog/logging.h>
#include <string>
#include "common/utils.h"
#include "components/store/versioned_kvstore.h"
#include "components/store/btreestore.h"
#include "components/store/leveldbstore.h"

class HybridVersionedKVStoreTest {
 public:
  HybridVersionedKVStoreTest() : store_(new HybridVersionedKVStore()) {}

  ~HybridVersionedKVStoreTest() {
    delete store_;
  }

  void Reset() {
    delete store_;
    store_ = new HybridVersionedKVStore();
  }

#define EXPECT_RECORD(key, value, version) do { \
  string result; \
  EXPECT_TRUE(store_->Exists(key, version)); \
  EXPECT_TRUE(store_->Get(key, version, &result)); \
  EXPECT_EQ(value, result); \
} while (false);

#define EXPECT_NO_RECORD(key, version) do { \
  string result; \
  EXPECT_FALSE(store_->Exists(key, version)); \
  EXPECT_FALSE(store_->Get(key, version, &result)) \
      << "found (" << key << ", " << result << ") at version " << version; \
} while (false);

  void PutGetDelete() {
    Reset();

    // Starts empty.
    EXPECT_NO_RECORD("alpha", 0);

    // Put records.
    store_->Put("alpha", "alice", 1);
    store_->Put("bravo", "bob", 2);

    EXPECT_NO_RECORD("alpha", 0);
    EXPECT_NO_RECORD("alpha", 1);
    EXPECT_RECORD("alpha", "alice", 2);

    // Delete at subsequent timestamp.
    store_->Delete("alpha", 3);

    // Check record timelines by reading at different versions.
    EXPECT_NO_RECORD("alpha", 0);
    EXPECT_NO_RECORD("alpha", 1);
    EXPECT_RECORD("alpha", "alice", 2);
    EXPECT_RECORD("alpha", "alice", 3);
    EXPECT_NO_RECORD("alpha", 4);

    EXPECT_NO_RECORD("bravo", 1);
    EXPECT_NO_RECORD("bravo", 2);
    EXPECT_RECORD("bravo", "bob", 3);
  }

  static const int kRecords = 100000;
  static const int kVersions = 10;

  void PutMany() {
    double start = GetTime();
    for (int v = 0; v < kVersions; v++) {
      for (int r = 0; r < kRecords; r++) {
        store_->Put(IntToString(r), IntToString(r+v), v);
      }
    }
    LOG(ERROR) << "PutMany(): " << GetTime() - start << " seconds";
  }
  void PutManyAgain() {
    double start = GetTime();
    for (int v = kVersions; v < 2*kVersions; v++) {
      for (int r = 0; r < kRecords; r++) {
        store_->Put(IntToString(r), IntToString(r+v), v);
      }
    }
    LOG(ERROR) << "PutManyAgain(): " << GetTime() - start << " seconds";
  }

  void GetMany() {
    for (int r = 0; r < kRecords; r++) {
      EXPECT_NO_RECORD(IntToString(r), 0);
    }
    double start = GetTime();
    for (int v = 0; v < kVersions; v++) {
      for (int r = 0; r < kRecords; r++) {
        EXPECT_RECORD(IntToString(r), IntToString(r+v), v+1);
      }
    }
    LOG(ERROR) << "GetMany(): " << GetTime() - start << " seconds";
  }

  void GetManyAgain() {
    for (int r = 0; r < kRecords; r++) {
      EXPECT_NO_RECORD(IntToString(r), 0);
    }
    double start = GetTime();
    for (int v = kVersions; v < 2*kVersions; v++) {
      for (int r = 0; r < kRecords; r++) {
        EXPECT_RECORD(IntToString(r), IntToString(r+v), v+1);
      }
    }
    LOG(ERROR) << "GetManyAgain(): " << GetTime() - start << " seconds";
  }

 private:
  // Store being tested.
  HybridVersionedKVStore* store_;
};

TEST(HybridVersionedKVStoreTest, BasicTest) {
  HybridVersionedKVStoreTest t;
  t.PutGetDelete();
  t.PutMany();
  t.GetMany();
  Spin(11);
  t.PutManyAgain();
  t.GetMany();
}

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
