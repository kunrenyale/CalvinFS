// Author: Alexander Thomson <thomson@cs.yale.edu>
//
// TODO(agt): Move to btreestore_test.cc
// TODO(agt): Test Actions.

#include "components/store/kvstore.h"
#include "components/store/btreestore.h"
#include "components/store/leveldbstore.h"

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <glog/logging.h>
#include <map>
#include <string>

#include "common/utils.h"

using std::map;

template<class KVStoreType>
void TestInsertDelete() {
  KVStoreType s;
  string result;

  EXPECT_FALSE(s.Exists("a"));
  EXPECT_FALSE(s.Get("a", &result));

  s.Put("a", "alpha");
  EXPECT_TRUE(s.Exists("a"));
  EXPECT_TRUE(s.Get("a", &result));
  EXPECT_EQ("alpha", result);

  s.Put("b", "bravo");
  EXPECT_TRUE(s.Exists("a"));
  EXPECT_TRUE(s.Get("a", &result));
  EXPECT_EQ("alpha", result);
  EXPECT_TRUE(s.Exists("b"));
  EXPECT_TRUE(s.Get("b", &result));
  EXPECT_EQ("bravo", result);

  s.Delete("a");
  EXPECT_FALSE(s.Exists("a"));
  EXPECT_FALSE(s.Get("a", &result));
  EXPECT_TRUE(s.Exists("b"));
  EXPECT_TRUE(s.Get("b", &result));
  EXPECT_EQ("bravo", result);
}

TEST(BTreeStoreTest, EmptyIterator) {
  BTreeStore s;
  KVStore::Iterator* i = s.GetIterator();
  EXPECT_FALSE(i->Valid());
  i->Next();
  EXPECT_FALSE(i->Valid());

  delete i;

  // TODO(agt): ASSERT_DEATH tests on bad i->Key() and i->Value() calls.
}

template<class KVStoreType>
void TestIterator() {
  BTreeStore s;
  map<string, string> m;
  for (int i = 0; i < 1000; i++) {
    string k = RandomString(3 + rand() % 7);
    string v = RandomString(100);
    s.Put(k, v);
    m[k] = v;
  }

  KVStore::Iterator* i = s.GetIterator();
  EXPECT_FALSE(i->Valid());
  i->Next();
  for (auto j = m.begin(); j != m.end(); ++j) {
    EXPECT_TRUE(i->Valid());
    EXPECT_EQ(j->first, i->Key());
    EXPECT_EQ(j->second, i->Value());
    i->Next();
  }
  EXPECT_FALSE(i->Valid());

  delete i;

  // TODO(agt): Also test thread safety.
}

TEST(BTreeStoreTest, InsertDelete) {
  TestInsertDelete<BTreeStore>();
}
TEST(BTreeStoreTest, Iterator) {
  TestIterator<BTreeStore>();
}

TEST(LevelDBStoreTest, InsertDelete) {
  TestInsertDelete<LevelDBStore>();
}
TEST(LevelDBStoreTest, Iterator) {
  TestIterator<LevelDBStore>();
}

int main(int argc, char **argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

