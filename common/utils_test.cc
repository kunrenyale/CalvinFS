// Author: Alexander Thomson <thomson@cs.yale.edu>

#include "common/utils.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "common/vec.h"

using std::string;

DECLARE_bool(test_flag);
DEFINE_bool(test_flag, true, "Test flag!");

TEST(UtilsTest, GFlags) {
  EXPECT_TRUE(FLAGS_test_flag);
}

class TypeWithUnknownName {};
ADD_TYPE_NAME(uint32);
TEST(UtilsTest, TypeNames) {
  EXPECT_EQ("int32", TypeName<int32>());
  EXPECT_EQ("uint32", TypeName<uint32>());
  EXPECT_EQ("string", TypeName<string>());
  EXPECT_EQ("Slice", TypeName<Slice>());
}

TEST(UtilsTest, SplitString) {
  EXPECT_EQ(Vec<string>() || "", SplitString("", ' '));
  EXPECT_EQ(Vec<string>() || "ab", SplitString("ab", ' '));
  EXPECT_EQ(Vec<string>() | "a" || "b", SplitString("a b", ' '));
  EXPECT_EQ(Vec<string>() | "a" | "b" || "", SplitString("a b ", ' '));
  EXPECT_EQ(Vec<string>() | "" | "a" || "b", SplitString(" a b", ' '));
  EXPECT_EQ(Vec<string>() | "a" | "" || "b", SplitString("a  b", ' '));
}

TEST(UtilsTest, StringToInt) {
  EXPECT_EQ(10, StringToInt("10"));
  ASSERT_DEATH({ StringToInt(""); }, "invalid numeric string: ");
  ASSERT_DEATH({ StringToInt(" "); }, "invalid numeric string:  ");
  ASSERT_DEATH({ StringToInt("-"); }, "invalid numeric string: -");
  ASSERT_DEATH({ StringToInt("a"); }, "invalid numeric string: a");
}

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

