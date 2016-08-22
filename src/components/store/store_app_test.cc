// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// TODO(agt): Testing.

#include "components/store/store_app.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

