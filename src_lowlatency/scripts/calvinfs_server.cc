// Author: Alexander Thomson (thomson@cs.yale.edu)
//

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "machine/cluster_config.h"
#include "machine/machine.h"
#include "components/log/log.h"
#include "components/log/log_app.h"
#include "components/log/paxos.h"
#include "components/store/store.h"
#include "components/store/store_app.h"
#include "components/scheduler/scheduler.h"
#include "components/scheduler/locking_scheduler.h"
#include "fs/batch.pb.h"
#include "fs/block_log.h"
#include "fs/block_store.h"
#include "fs/calvinfs.h"
#include "fs/calvinfs_client_app.h"
#include "fs/metadata_store.h"
#include "fs/metadata.pb.h"
#include "scripts/script_utils.h"

DEFINE_bool(calvin_version, false, "Print Calvin version information");
DEFINE_string(binary, "calvinfs_server", "Calvin binary executable program");
DEFINE_string(config, "calvin.conf", "conf file of Calvin cluster");
DEFINE_int32(machine_id, 0, "machine id");
DEFINE_double(time, 0, "start time");
DEFINE_int32(experiment, 0, "experiment that you want to run");

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  // Print Calvin version
  if (FLAGS_calvin_version) {
    // Check whether Calvin have been running
    if (is_process_exist((char *)FLAGS_binary.c_str()) == true) {
      return -2;
    } else {
      printf("Machine %d: (CalvinFS) 0.1 (c) Yale University 2016.\n",
             (int)FLAGS_machine_id);
      return 0;
    }
  }

  LOG(ERROR) << "Preparing to start CalvinFS node "
             << FLAGS_machine_id << "...";

  while (GetTime() < FLAGS_time) {
    usleep(100);
  }

  // Create machine.
  ClusterConfig cc;
  cc.FromFile(FLAGS_config);

  uint32 replicas = (cc.size() >= 3) ? 3 : 1;
  uint32 partitions = cc.size() / replicas;

  LOG(ERROR) << "Starting CalvinFS node " << FLAGS_machine_id
             << " (partition " << (FLAGS_machine_id % partitions)
             << "/" << partitions
             << ", replica " << (FLAGS_machine_id / partitions)
             << "/" << replicas << ")";

  Machine m(FLAGS_machine_id, cc);
  Spin(1);

  string fsconfig;
  MakeCalvinFSConfig(partitions, replicas).SerializeToString(&fsconfig);
  m.AppData()->Put("calvinfs-config", fsconfig);
  Spin(1);

  // Start paxos app (Currently each paxos group includes 3 machines).
  if (FLAGS_machine_id % partitions < 3) {
    StartAppProto sap;
    uint32 replica = FLAGS_machine_id / partitions; 
    for (uint32 i = replica * partitions; i < ((replica+1)*partitions) && (i < replica * partitions + 3); i++) {
      sap.add_participants(i);
    }

    sap.set_app("Paxos2App2");
    sap.set_app_name("paxos2");
    string args;
    sap.SerializeToString(&args);
    sap.set_app_args(args);
    m.AddApp(sap);
    LOG(ERROR) << "[" << FLAGS_machine_id << "] created Metalog (Paxos2)";
  }

  m.GlobalBarrier();
  Spin(1);

  // Start metadata store app.
  m.AddApp("MetadataStoreApp", "metadata");
  reinterpret_cast<MetadataStore*>(
      reinterpret_cast<StoreApp*>(m.GetApp("metadata"))->store())
          ->SetMachine(&m);
  LOG(ERROR) << "[" << FLAGS_machine_id << "] created MetadataStore";
  m.GlobalBarrier();
  Spin(1);

  // Start scheduler app.
  m.AddApp("LockingScheduler", "scheduler");
  Scheduler* scheduler_ = reinterpret_cast<Scheduler*>(m.GetApp("scheduler"));

  LOG(ERROR) << "[" << FLAGS_machine_id << "] created LockingScheduler";
  m.GlobalBarrier();
  Spin(1);

  // Bind scheduler to store.
  scheduler_->SetStore("metadata");

  LOG(ERROR) << "[" << FLAGS_machine_id << "] bound Scheduler to MetadataStore";
  m.GlobalBarrier();
  Spin(1);

  // Start block store app.
  m.AddApp("DistributedBlockStore", "blockstore");

  LOG(ERROR) << "[" << FLAGS_machine_id << "] created BlockStore";
  m.GlobalBarrier();
  Spin(1);

  // Start log app.
  m.AddApp("BlockLogApp", "blocklog");

  LOG(ERROR) << "[" << FLAGS_machine_id << "] created BlockLog";
  m.GlobalBarrier();
  Spin(1);

  // Connect log to scheduler.
  scheduler_->SetActionSource(
      reinterpret_cast<BlockLogApp*>(m.GetApp("blocklog"))->GetActionSource());

  LOG(ERROR) << "[" << FLAGS_machine_id << "] connected BlockLog to Scheduler";
  m.GlobalBarrier();
  Spin(1);

  // Start client app.
  m.AddApp("CalvinFSClientApp", "client");

  LOG(ERROR) << "[" << FLAGS_machine_id << "] created CalvinFSClientApp";
  reinterpret_cast<CalvinFSClientApp*>(m.GetApp("client"))
      ->set_start_time(FLAGS_time);
  reinterpret_cast<CalvinFSClientApp*>(m.GetApp("client"))
      ->set_experiment(FLAGS_experiment); 

  while (!m.Stopped()) {
    usleep(10000);
  }

  printf("Machine %d : Calvin server exit!\n", (int)FLAGS_machine_id);
  usleep(1000*1000);
}

