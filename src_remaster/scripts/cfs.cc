// Author: Alexander Thomson (thomson@cs.yale.edu)
//

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>  // Bad alex.

#include "fs/metadata.pb.h"
#include "machine/external_connection.h"
#include "proto/header.pb.h"
#include "scripts/script_utils.h"

DEFINE_string(config, "calvin.conf", "conf file of Calvin cluster");
DEFINE_string(command, "", "fs command");
DEFINE_string(path, "", "path of file");
DEFINE_string(data, "", "data to append");

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  ClusterConfig cc;
  cc.FromFile(FLAGS_config);

  ExternalConnection connection(20002, cc);

  uint64 machine_id = rand() % cc.size();

  Header* header = new Header();
  header->set_from(kExternalMachineID);
  header->set_to(machine_id);
  header->set_type(Header::RPC);
  header->set_app("client");
  header->add_misc_string(FLAGS_path);
  header->set_external_host(HostName());
  header->set_external_port(20002);

  vector<pair<Header*, MessageBuffer*> > results(1);

  Header* h;
  MessageBuffer* m;
  if (FLAGS_command == "ls") {
    header->set_rpc("LS");
    connection.SendMessage(header, new MessageBuffer());
    connection.GetMessage(&h, &m);
    std::cout << *m;

  } else if (FLAGS_command == "cat") {
    header->set_rpc("READ_FILE");
    connection.SendMessage(header, new MessageBuffer());
    connection.GetMessage(&h, &m);
    std::cout << *m;

  } else if (FLAGS_command == "mkdir") {
    header->set_rpc("CREATE_FILE");
    header->add_misc_bool(true);  // DIR
    connection.SendMessage(header, new MessageBuffer());
    connection.GetMessage(&h, &m);
    std::cout << *m;

  } else if (FLAGS_command == "touch") {
    header->set_rpc("CREATE_FILE");
    header->add_misc_bool(false);  // DATA
    connection.SendMessage(header, new MessageBuffer());
    connection.GetMessage(&h, &m);
    std::cout << *m;

  } else if (FLAGS_command == "append") {
    header->set_rpc("APPEND");
    connection.SendMessage(header, new MessageBuffer(FLAGS_data));
    connection.GetMessage(&h, &m);
    std::cout << *m;

  } else {
    LOG(FATAL) << "unknown command: " << FLAGS_command;
  }

  return 0;
}

