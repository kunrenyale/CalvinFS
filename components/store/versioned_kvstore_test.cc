// Author: Alexander Thomson <thomson@cs.yale.edu>
//
// TODO(agt): Use gtest's value-parametrized (TEST_P) setup.
// TODO(agt): Test GetVersion.

#include "components/store/versioned_kvstore.h"

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <glog/logging.h>
#include <string>

#include "common/utils.h"
#include "components/store/btreestore.h"
#include "components/store/leveldbstore.h"

using std::pair;
using std::string;

DEFINE_bool(benchmark, false, "Run benchmarks instead of unit tests.");

// Testing some of the utility functions from versioned_kvstore.cc
unsigned char GetByte(uint64 x, uint32 k);
void SetByte(uint64* x, uint32 k, unsigned char c);
void AppendVersion(string* key, uint64 version, uint64 flags = 0);
uint64 ParseVersion(const Slice& versioned_key, uint64* flags = NULL);
Slice StripVersion(const Slice& versioned_key);

TEST(VersionTest, SetByteGetByte) {
  uint64 x = 0;
  SetByte(&x, 0, 5);
  EXPECT_EQ(static_cast<uint64>(0x5), x);
  SetByte(&x, 3, 5);
  EXPECT_EQ(static_cast<uint64>(0x5000005), x);
}

TEST(VersionTest, AppendParse) {
  string a("alpha");
  AppendVersion(&a, 1);
  EXPECT_EQ("alpha", StripVersion(a));
  EXPECT_EQ(static_cast<uint64>(1), ParseVersion(a));

  uint64 flags;
  EXPECT_EQ(static_cast<uint64>(1), ParseVersion(a, &flags));
  EXPECT_EQ(static_cast<uint64>(0), flags);
}

TEST(VersionTest, AppendParseWithFlags) {
  string a("alpha");
  AppendVersion(&a, 2, 1);
  EXPECT_EQ("alpha", StripVersion(a));
  EXPECT_EQ(static_cast<uint64>(2), ParseVersion(a));

  uint64 flags;
  EXPECT_EQ(static_cast<uint64>(2), ParseVersion(a, &flags));
  EXPECT_EQ(static_cast<uint64>(1), flags);
}

template <class KVStoreType>
class VersionedKVStoreTest {
 public:
  VersionedKVStoreTest() : store_(new VersionedKVStore(new KVStoreType())) {}

  ~VersionedKVStoreTest() {
    delete store_;
  }

  void Reset() {
    delete store_;
    store_ = new VersionedKVStore(new KVStoreType());
  }

#define EXPECT_RECORD(key,value,version) do { \
  string result; \
  EXPECT_TRUE(store_->Exists(key, version)); \
  EXPECT_TRUE(store_->Get(key, version, &result)); \
  EXPECT_EQ(value, result); \
} while (false);

#define EXPECT_NO_RECORD(key,version) do { \
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

  static const int kRecords = 4;
  static const int kVersions = 4;

  void PutMany() {
    for (int v = 0; v < kVersions; v++) {
      for (int r = 0; r < kRecords; r++) {
        store_->Put(IntToString(r), IntToString(r+v), v);
      }
    }
  }

  void GetMany() {
    for (int r = 0; r < kRecords; r++) {
      EXPECT_NO_RECORD(IntToString(r), 0);
    }
    for (int v = 0; v < kVersions; v++) {
      for (int r = 0; r < kRecords; r++) {
        EXPECT_RECORD(IntToString(r), IntToString(r+v), v+1);
      }
    }
  }

 private:
  // Store being tested.
  VersionedKVStore* store_;
};

TEST(VersionedKVStoreTest, BTreeStore) {
  VersionedKVStoreTest<BTreeStore> t;
  t.PutGetDelete();
  t.PutMany();
  t.GetMany();
}
TEST(VersionedKVStoreTest, LevelDBStore) {
  VersionedKVStoreTest<LevelDBStore> t;
  t.PutGetDelete();
  t.PutMany();
  t.GetMany();
}

int main(int argc, char **argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/*

////////////////////////////////////////////////////////////////////////////////

void Setup(uint64 size, vector<pair<string, string> >* elements) {
  elements->clear();
  for (uint64 i = 0; i < size; i++) {
    string key(17, 'x');
    snprintf(const_cast<char*>(key.data()), 17, "%016lX", i);
    key.resize(16);
    elements->push_back(make_pair(key, RandomBytesNoZeros(100)));
  }
}

void Shuffle(vector<pair<string, string> >* elements) {
  if (elements->size() <= 1) {
    return;
  }

  for (uint32 i = 0; i < elements->size(); i++) {
    int j, k;
    do {
      j = rand() % elements->size();
      k = rand() % elements->size();
    } while (j == k);
    pair<string, string> t = (*elements)[j];
    (*elements)[j] = (*elements)[k];
    (*elements)[k] = t;
  }
}

template <class Store>
void BM_SequentialFill(uint64 size) {
  // Setup.
  vector<pair<string, string> > elements;
  Setup(size, &elements);
  Store s;
  // Fill.
  double start = GetTime();
  for (uint64 i = 0; i < size; i++) {
    s.Put(elements[i].first, elements[i].second, i);
  }
  double end = GetTime();
  // Report.
  LOG(ERROR) << s.MVStoreType() << " sequential fill (" << size << "): "
             << 1000 * (end - start) << " ms";
}

template <class Store>
void BM_RandomFill(uint64 size) {
  // Setup.
  vector<pair<string, string> > elements;
  Setup(size, &elements);
  Shuffle(&elements);
  Store s;
  // Fill.
  double start = GetTime();
  for (uint64 i = 0; i < size; i++) {
    s.Put(elements[i].first, elements[i].second, i);
  }
  double end = GetTime();
  // Report.
  LOG(ERROR) << s.MVStoreType() << " random fill     (" << size << "): "
             << 1000 * (end - start) << " ms";
}

template <class Store>
void BM_SequentialRead(uint64 size) {
  // Setup.
  vector<pair<string, string> > elements;
  Setup(size, &elements);
  Store s;
  // Fill.
  for (uint64 i = 0; i < size; i++) {
    s.Put(elements[i].first, elements[i].second, i);
  }
  // Read.
  double start = GetTime();
  for (uint64 i = 0; i < size; i++) {
    string result;
    s.Get(elements[i].first, rand() % size + 1, &result);
  }
  double end = GetTime();
  // Report.
  LOG(ERROR) << s.MVStoreType() << " sequential read (" << size << "): "
             << 1000 * (end - start) << " ms";
}

template <class Store>
void BM_RandomRead(uint64 size) {
  // Setup.
  vector<pair<string, string> > elements;
  Setup(size, &elements);
  Store s;
  // Fill.
  for (uint64 i = 0; i < size; i++) {
    s.Put(elements[i].first, elements[i].second, i);
  }
  // Read.
  Shuffle(&elements);
  double start = GetTime();
  for (uint64 i = 0; i < size; i++) {
    string result;
    s.Get(elements[i].first, rand() % size + 1, &result);
  }
  double end = GetTime();
  // Report.
  LOG(ERROR) << s.MVStoreType() << " random read     (" << size << "): "
             << 1000 * (end - start) << " ms";
}

template <class Store>
void BM_AlternateReadWrite(uint64 size) {
  // Setup.
  vector<pair<string, string> > elements;
  Setup(size, &elements);

  // Shuffled copy for reading.
  vector<pair<string, string> > elements_copy(elements);
  Shuffle(&elements_copy);

  Store s;
  // Alternate writes and reads
  double start = GetTime();
  for (uint64 i = 0; i < size; i++) {
    s.Put(elements[i].first, elements[i].second, i);
    string result;
    s.Get(elements_copy[i].first, i, &result);
  }
  double end = GetTime();
  // Report.
  LOG(ERROR) << s.MVStoreType() << " write/read      (" << size << "): "
             << 1000 * (end - start) << " ms";
}

int main(int argc, char **argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  if (FLAGS_benchmark) {
    // Fill benchmarks.
    BM_SequentialFill<MVMapStore>(100000);
    BM_SequentialFill<MVBTreeStore>(100000);
    BM_SequentialFill<MVLevelDBStore>(100000);
    BM_RandomFill<MVMapStore>(100000);
    BM_RandomFill<MVBTreeStore>(100000);
    BM_RandomFill<MVLevelDBStore>(100000);

    // Read benchmarks.
    BM_SequentialRead<MVMapStore>(100000);
    BM_SequentialRead<MVBTreeStore>(100000);
    BM_SequentialRead<MVLevelDBStore>(100000);
    BM_RandomRead<MVMapStore>(100000);
    BM_RandomRead<MVBTreeStore>(100000);
    BM_RandomRead<MVLevelDBStore>(100000);

    // Alternating write/read benchmarks.
    BM_AlternateReadWrite<MVMapStore>(100000);
    BM_AlternateReadWrite<MVBTreeStore>(100000);
    BM_AlternateReadWrite<MVLevelDBStore>(100000);

    // TODO(agt): Multithreaded benchmarks?
    return 0;
  }
  return RUN_ALL_TESTS();
}

*/

