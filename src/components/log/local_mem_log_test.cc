// Author: Alexander Thomson (thomson@cs.yale.edu)

#include "components/log/local_mem_log.h"

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <string>
#include "components/log/log.h"

TEST(LocalMemLogTest, AppendAndNext) {
  LocalMemLog log;
  Log::Reader* r = log.GetReader();

  // Reader is initially invalid.
  EXPECT_FALSE(r->Valid());

  // Reader should die with useful messages if we try to look at what version,
  // op, or encoded op it points to.
  ASSERT_DEATH({ r->Version(); }, "version called on invalid LogReader");
  ASSERT_DEATH({ r->Entry();   }, "entry called on invalid LogReader");
  ASSERT_DEATH({ r->Seek(0);   }, "seeking to invalid version 0");

  // Reader cannot advance when log is empty.
  EXPECT_FALSE(r->Next());

  // Append entry.
  // Reader is still invalid but can advance once, becoming valid.
  log.Append(1, "a");
  EXPECT_FALSE(r->Valid());
  EXPECT_TRUE(r->Next());
  EXPECT_TRUE(r->Valid());
  EXPECT_FALSE(r->Next());
  EXPECT_TRUE(r->Valid());

  // Check that r is positioned at 0.
  EXPECT_TRUE(r->Version() == 1);

  // Check that r exposes empty op (except received and committed
  // timestamps are 0).
  EXPECT_EQ("a", r->Entry());
}

TEST(LocalMemLogTest, AppendManyAndSeek) {
  LocalMemLog log;
  Log::Reader* r = log.GetReader();

  // Append 10k entries to force two resizes.
  for (uint64 i = 1; i <= 10000; i++) {
    log.Append(i, UInt64ToString(i));
  }

  // Seek to various entries.
  for (uint64 i = 1; i <= 10000; i++) {
    uint64 v = rand() % 10000 + 1;
    EXPECT_TRUE(r->Seek(v));
    EXPECT_EQ(v, r->Version());
    EXPECT_EQ(UInt64ToString(v), r->Entry());
  }

  // Append more, with holes.
  for (uint64 i = 10001; i <= 20000; i++) {
    if (i % 5 == 0) {
      log.Append(i, UInt64ToString(i));
    }
  }
  for (uint64 i = 1; i <= 10000; i++) {
    uint64 v = rand() % 20000 + 1;
    EXPECT_TRUE(r->Seek(v));
    if (v > 10000 && v % 5 != 0) {
      v += 5 - v % 5;
    }
    EXPECT_EQ(v, r->Version());
    EXPECT_EQ(UInt64ToString(v), r->Entry());
  }
}

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

