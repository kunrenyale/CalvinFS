// Author: Kun Ren <kun@cs.yale.edu>
//

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "machine/cluster_manager.h"

DEFINE_string(command, "status", "cluster command");
DEFINE_string(config, "calvin.conf", "conf file of Calvin cluster");
DEFINE_string(calvin_path, "/home/ubuntu/CalvinFS",
              "path to the main calvin directory");
DEFINE_string(binary, "calvinfs_server", "Calvin binary executable program");
DEFINE_string(ssh_key1, "-i ~/Virginia.pem", "ssh_key for the first data center(Virginia)");
DEFINE_string(ssh_key2, "-i ~/Oregon.pem", "ssh_key for the second data center(Oregon)");
DEFINE_string(ssh_key3, "-i ~/Ireland.pem", "ssh_key for the third data center(Ireland)");
DEFINE_int32(experiment, 0, "the experiment that you want to run");
DEFINE_int32(clients, 20, "number of concurrent clients on each machine");
DEFINE_bool(valgrind, false, "Run binaries with valgrind?");

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  ClusterManager cm(FLAGS_config, FLAGS_calvin_path, FLAGS_binary,
                    FLAGS_ssh_key1, FLAGS_ssh_key2, FLAGS_ssh_key3);

  ClusterConfig config = cm.GetConfig();

  if (FLAGS_command == "update") {
    cm.Update();

  } else if (FLAGS_command == "put-config") {
    cm.PutConfig();

  } else if (FLAGS_command == "get-data") {
    cm.GetTempFiles("report.");

  } else if (FLAGS_command == "start") {
    cm.DeployCluster(GetTime() + 10, FLAGS_experiment, FLAGS_clients);

  } else if (FLAGS_command == "kill") {
    cm.KillCluster();

  } else if (FLAGS_command == "kill-partial") {
    cm.KillReplica(2);

  } else if (FLAGS_command == "status") {
    cm.ClusterStatus();

  } else {
    LOG(FATAL) << "unknown command: " << FLAGS_command;
  }
  return 0;
}

