// Author: Kun Ren <kun@cs.yale.edu>
//

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "machine/cluster_config.h"
#include "machine/machine.h"
#include "scripts/script_utils.h"

DEFINE_bool(calvin_version, false, "Print Calvin version information");
DEFINE_string(binary, "calvin_server", "Calvin binary executable program");
DEFINE_string(config, "calvin.conf", "conf file of Calvin cluster");
DEFINE_int32(machine_id, 0, "machine id");

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  // Print Calvin version
  if (FLAGS_calvin_version) {
    // Check whether Calvin have been running
    if (is_process_exist((char *)FLAGS_binary.c_str()) == true) {
      return -2;
    } else {
      printf("Machine %d: (Calvin) 0.1 (c) Yale University 2013.\n",
             (int)FLAGS_machine_id);
      return 0;
    }
  }

  // Create machine.
  ClusterConfig cc;
  cc.FromFile(FLAGS_config);
  Machine m(FLAGS_machine_id, cc);

  while (!m.Stopped()) {
    usleep(10000);
  }

  printf("Machine %d : Calvin server exit!\n", (int)FLAGS_machine_id);
  usleep(1000*1000);
}

