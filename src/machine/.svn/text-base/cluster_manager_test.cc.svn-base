// Author: Kun Ren <kun@cs.yale.edu>
//

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <map>
#include "machine/cluster_manager.h"
#include "machine/machine.h"
#include "common/utils.h"
#include "machine/app/app.h"

#include "leveldb/env.h"

// Replace this with your binary path and config path
DEFINE_string(calvin_path, "/home/ubuntu/calvin_codebase/calvin",
              "path to the main calvin directory");
DEFINE_string(binary, "calvin_server",
              "Calvin binary executable program");
DEFINE_string(ssh_key, "-i ~/Calvin_Key.pem",
              "For ssh authentication");
DEFINE_string(config, "/tmp/testconfigfile-16102379510263512",
              "conf file of Calvin cluster");
DEFINE_string(config_string, "1:50.18.147.54:10001\n2:54.241.237.202:10001",
              "conf string of Calvin cluster");

TEST(ClusterManagerTest, FailureCheck) {
  // If the test configfile already exists, delete it.
  leveldb::Status s;
  if (leveldb::Env::Default()->FileExists(FLAGS_config)) {
    s = leveldb::Env::Default()->DeleteFile(FLAGS_config);
    CHECK(s.ok());
  }
  // Create test configfile.
  s = leveldb::WriteStringToFile(
        leveldb::Env::Default(),
        "1:aaa:1001",
        FLAGS_config);

  ClusterManager manager(FLAGS_config, FLAGS_calvin_path, FLAGS_binary,
                         FLAGS_ssh_key);
  ASSERT_DEATH({manager.DeployCluster();}, "Fail to deploy calvin cluster!");
}

TEST(ClusterManagerTest, DeployAndKillTest) {
  // If the test configfile already exists, delete it.
  leveldb::Status s;
  if (leveldb::Env::Default()->FileExists(FLAGS_config)) {
    s = leveldb::Env::Default()->DeleteFile(FLAGS_config);
    CHECK(s.ok());
  }
  // Create test configfile.
  s = leveldb::WriteStringToFile(
        leveldb::Env::Default(),
        FLAGS_config_string,
        FLAGS_config);

  ClusterManager manager(FLAGS_config, FLAGS_calvin_path, FLAGS_binary,
                         FLAGS_ssh_key);
  manager.DeployCluster();
  usleep(1000*1000);
  manager.KillCluster();
  usleep(1000*1000);
}

TEST(ClusterManagerTest, DeployAndTearDownTest) {
  // If the test configfile already exists, delete it.
  leveldb::Status s;
  if (leveldb::Env::Default()->FileExists(FLAGS_config)) {
    s = leveldb::Env::Default()->DeleteFile(FLAGS_config);
    CHECK(s.ok());
  }
  // Create test configfile.
  s = leveldb::WriteStringToFile(
        leveldb::Env::Default(),
        FLAGS_config_string,
        FLAGS_config);

  ClusterManager manager(FLAGS_config, FLAGS_calvin_path, FLAGS_binary,
                         FLAGS_ssh_key);
  manager.DeployCluster();
  usleep(1000*2000);
  manager.TearDownCluster();
  usleep(1000*2000);
}

TEST(ClusterManagerTest, AddAppTest) {
  // If the test configfile already exists, delete it.
  leveldb::Status s;
  if (leveldb::Env::Default()->FileExists(FLAGS_config)) {
    s = leveldb::Env::Default()->DeleteFile(FLAGS_config);
    CHECK(s.ok());
  }
  // Create test configfile.
  s = leveldb::WriteStringToFile(
        leveldb::Env::Default(),
        FLAGS_config_string,
        FLAGS_config);

  ClusterManager manager(FLAGS_config, FLAGS_calvin_path, FLAGS_binary,
                         FLAGS_ssh_key);
  manager.DeployCluster();

  StartAppProto sap;
  ClusterConfig config = manager.GetConfig();
  for (map<uint64, MachineInfo>::const_iterator it =
       config.machines().begin();
       it != config.machines().end(); ++it) {
    uint64 machine_id = it->second.id();
    sap.add_participants(machine_id);
  }
  sap.set_app("TestAddApp");
  sap.set_app_name("TestAddAppName");
  manager.AddApp(sap);
  usleep(1000*1000);
  manager.KillCluster();
  usleep(1000*1000);
}

TEST(ClusterManagerTest, ListAppsTest) {
  // If the test configfile already exists, delete it.
  leveldb::Status s;
  if (leveldb::Env::Default()->FileExists(FLAGS_config)) {
    s = leveldb::Env::Default()->DeleteFile(FLAGS_config);
    CHECK(s.ok());
  }
  // Create test configfile.
  s = leveldb::WriteStringToFile(
        leveldb::Env::Default(),
        FLAGS_config_string,
        FLAGS_config);

  ClusterManager manager(FLAGS_config, FLAGS_calvin_path, FLAGS_binary,
                         FLAGS_ssh_key);
  manager.DeployCluster();

  StartAppProto sap;
  ClusterConfig config = manager.GetConfig();
  for (map<uint64, MachineInfo>::const_iterator it =
       config.machines().begin();
       it != config.machines().end(); ++it) {
    uint64 machine_id = it->second.id();
    sap.add_participants(machine_id);
  }
  sap.set_app("TestAddApp");
  sap.set_app_name("TestAddAppName");
  manager.AddApp(sap);
  usleep(1000*1000);
  manager.ListApps();
  usleep(1000*1000);
  manager.KillCluster();
  usleep(1000*1000);
}

TEST(ClusterManagerTest, ClusterstatusTest) {
  // If the test configfile already exists, delete it.
  leveldb::Status s;
  if (leveldb::Env::Default()->FileExists(FLAGS_config)) {
    s = leveldb::Env::Default()->DeleteFile(FLAGS_config);
    CHECK(s.ok());
  }
  // Create test configfile.
  s = leveldb::WriteStringToFile(
        leveldb::Env::Default(),
        FLAGS_config_string,
        FLAGS_config);

  ClusterManager manager(FLAGS_config, FLAGS_calvin_path, FLAGS_binary,
                         FLAGS_ssh_key);
  usleep(1000*1000);
  manager.ClusterStatus();
}

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
