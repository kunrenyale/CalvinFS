// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// TODO(alex): Test FromProto, ToString, ToFile, ToProto.

#include "machine/cluster_config.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "leveldb/env.h"

TEST(ClusterConfigTest, EmptyConfig) {
  ClusterConfig cc;
  EXPECT_EQ(0, cc.size());
  cc.FromString("");
  EXPECT_EQ(0, cc.size());
  cc.FromString("\n");
  EXPECT_EQ(0, cc.size());
}

TEST(ClusterConfigTest, BadConfigLine) {
  ClusterConfig cc;
  ASSERT_DEATH({ cc.FromString("o"); }, "bad config line: o");
  ASSERT_DEATH({ cc.FromString("1:localhost:1\no"); }, "bad config line: o");
}

TEST(ClusterConfigTest, BadMachineID) {
  ClusterConfig cc;
  ASSERT_DEATH({ cc.FromString("-1:localhost:1"); }, "bad machine id: -1");
  ASSERT_DEATH({ cc.FromString(":localhost:1"); }, "invalid numeric string: ");
  ASSERT_DEATH({ cc.FromString("BAD:localhost:1"); },
               "invalid numeric string: BAD");
}

TEST(ClusterConfigTest, EmptyHostname) {
  ClusterConfig cc;
  ASSERT_DEATH({ cc.FromString("1::1"); }, "empty hostname");
}

TEST(ClusterConfigTest, BadPort) {
  ClusterConfig cc;
  ASSERT_DEATH({ cc.FromString("1:localhost:-1"); }, "bad port: -1");
  ASSERT_DEATH({ cc.FromString("1:localhost:"); }, "invalid numeric string: ");
  ASSERT_DEATH({ cc.FromString("1:localhost:BAD"); },
               "invalid numeric string: BAD");
}

TEST(ClusterConfigTest, RepeatedMachineIDs) {
  ClusterConfig cc;
  ASSERT_DEATH({ cc.FromString("1:localhost:1001\n1:192.128.0.1:1002"); },
               "repeated machine id: 1");
}

TEST(ClusterConfigTest, RepeatedHostPort) {
  ClusterConfig cc;
  ASSERT_DEATH({ cc.FromString("1:localhost:1001\n2:localhost:1001"); },
               "repeated host/port pair: localhost:1001");
}

TEST(ClusterConfigTest, OneLineConfig) {
  ClusterConfig cc;
  cc.FromString("1:localhost:1001");
  EXPECT_EQ(1, cc.size());
  MachineInfo m;
  EXPECT_TRUE(cc.lookup_machine(1, &m));
  EXPECT_EQ((uint64)1, m.id());
  EXPECT_EQ("localhost", m.host());
  EXPECT_EQ(1001, m.port());
}

TEST(ClusterConfigTest, MultiLineConfig) {
  ClusterConfig cc;
  cc.FromString("1:localhost:1001\n2:192.128.0.1:1002");
  EXPECT_EQ(2, cc.size());
  MachineInfo m;
  EXPECT_TRUE(cc.lookup_machine(1, &m));
  EXPECT_EQ((uint64)1, m.id());
  EXPECT_EQ("localhost", m.host());
  EXPECT_EQ(1001, m.port());
  EXPECT_TRUE(cc.lookup_machine(2, &m));
  EXPECT_EQ((uint64)2, m.id());
  EXPECT_EQ("192.128.0.1", m.host());
  EXPECT_EQ(1002, m.port());
}

TEST(ClusterConfigTest, FromFile) {
  string filename("/tmp/testconfigfile-16102379510263512");
  // If the test configfile already exists, delete it.
  leveldb::Status s;
  if (leveldb::Env::Default()->FileExists(filename)) {
    s = leveldb::Env::Default()->DeleteFile(filename);
    CHECK(s.ok());
  }
  // Create test configfile.
  s = leveldb::WriteStringToFile(
        leveldb::Env::Default(),
        "1:localhost:1001\n2:192.128.0.1:1002",
        filename);
  CHECK(s.ok());

  // Check ClusterConfig
  ClusterConfig cc;
  cc.FromFile(filename);
  EXPECT_EQ(2, cc.size());
  MachineInfo m;
  EXPECT_TRUE(cc.lookup_machine(1, &m));
  EXPECT_EQ((uint64)1, m.id());
  EXPECT_EQ("localhost", m.host());
  EXPECT_EQ(1001, m.port());
  EXPECT_TRUE(cc.lookup_machine(2, &m));
  EXPECT_EQ((uint64)2, m.id());
  EXPECT_EQ("192.128.0.1", m.host());
  EXPECT_EQ(1002, m.port());

  // Delete test configfile.
  s = leveldb::Env::Default()->DeleteFile(filename);
  CHECK(s.ok());
}

TEST(ClusterConfigTest, FromFileInvalidFilename) {
  string filename("/tmp/testconfigfile-16102379510263512");
  // If the test configfile exists, delete it.
  leveldb::Status s;
  if (leveldb::Env::Default()->FileExists(filename)) {
    s = leveldb::Env::Default()->DeleteFile(filename);
    CHECK(s.ok());
  }

  // Check ClusterConfig
  ClusterConfig cc;
  ASSERT_DEATH({ cc.FromFile(filename); }, "failed to read file: " + filename);
}

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

