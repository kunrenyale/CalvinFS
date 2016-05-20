// @author Alexander Thomson (thomson@cs.yale.edu)
//
// TODO(alex): Write some tests.
// TODO(alex): Write some tests that span multiple physical machines.
// TODO(alex): Check for memory leaks?
// TODO(alex): Test error checking?

#include "machine/connection/connection_zmq.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

