// Author: Alexander Thomson <thomson@cs.yale.edu>

#include "common/varint.h"

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <string>
#include <vector>

#include "common/types.h"

using std::string;
using std::vector;

TEST(VarintTest, AppendParse) {
  vector<uint64> unencoded;
  string s;
  for (int i = 0; i < 1000000; i++) {
    uint64 target = rand();
    unencoded.push_back(target);
    varint::Append64(&s, target);
  }

  const char* pos = s.data();
  for (int i = 0; i < 1000000; i++) {
    uint64 x;
    pos = varint::Parse64(pos, &x);
    EXPECT_EQ(unencoded[i], x);
  }
}

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

