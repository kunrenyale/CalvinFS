// Author: Alexander Thomson <thomson@cs.yale.edu>

#include "common/vec.h"

#include <gtest/gtest.h>

TEST(VecTest, Vec) {
  // Create expected value.
  vector<int> expected;
  for (int i = 0; i < 5; i++)
    expected.push_back(i);

  // Create vector using Vec.
  EXPECT_EQ(expected,
            Vec<int>() | 0 | 1 | 2 | 3 || 4);

  // Append to expected value.
  expected.push_back(5);
  expected.push_back(6);

  // Create and append using Vec.
  EXPECT_EQ(expected,
            Vec<int>(Vec<int>() | 0 | 1 | 2 | 3 || 4) | 5 || 6);

  EXPECT_EQ(expected,
            Vec<int>(Vec<int>() | 0 | 1 | 2 | 3 | 4) | 5 || 6);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

