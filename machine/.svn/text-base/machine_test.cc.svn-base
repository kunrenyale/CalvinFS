// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// TODO(agt): Write tests that span multiple machines.

#include "machine/app/app.h"
#include "machine/machine.h"
#include "machine/message_buffer.h"
#include "common/utils.h"

#include <glog/logging.h>
#include <google/profiler.h>
#include <gtest/gtest.h>

#include "proto/header.pb.h"

TEST(MachineTest, SendDataPtrMessageLocal) {
  Machine m(0, ClusterConfig::LocalCluster(1));
  MessageBuffer* result = NULL;
  Header* header = new Header();
  header->set_to(0);
  header->set_from(0);
  header->set_type(Header::DATA);
  header->set_data_ptr(reinterpret_cast<uint64>(&result));
  m.SendMessage(header, new MessageBuffer("foo"));
  // Local send of DATA message should happen synchronously, no need to wait.
  EXPECT_TRUE(result != NULL);
  EXPECT_EQ(MessageBuffer("foo"), *result);
}

TEST(MachineTest, SendDataChannelMessageLocal) {
  Machine m(0, ClusterConfig::LocalCluster(1));
  Header* header = new Header();
  header->set_to(0);
  header->set_from(0);
  header->set_type(Header::DATA);
  header->set_data_channel("my-channel");
  m.SendMessage(header, new MessageBuffer("foo"));
  // Local send of DATA message should happen synchronously, no need to wait.
  MessageBuffer* result = NULL;
  EXPECT_TRUE(m.DataChannel("my-channel")->Pop(&result));
  EXPECT_EQ(MessageBuffer("foo"), *result);
}

TEST(MachineTest, SendDataChannelMessageRemote) {
  Machine a(0, ClusterConfig::LocalCluster(2));
  Machine b(1, ClusterConfig::LocalCluster(2));
  Header* header = new Header();
  header->set_to(1);
  header->set_from(0);
  header->set_type(Header::DATA);
  header->set_data_channel("my-channel");
  a.SendMessage(header, new MessageBuffer("foo"));

  MessageBuffer* result = NULL;
  while (!b.DataChannel("my-channel")->Pop(&result)) {
    // Wait for message.
  }
  EXPECT_EQ(MessageBuffer("foo"), *result);
}

TEST(MachineTest, SendDataPtrMessageRemote) {
  Machine a(0, ClusterConfig::LocalCluster(2));
  Machine b(1, ClusterConfig::LocalCluster(2));
  MessageBuffer* result = NULL;
  Header* header = new Header();
  header->set_to(1);
  header->set_from(0);
  header->set_type(Header::DATA);
  header->set_data_ptr(reinterpret_cast<uint64>(&result));
  a.SendMessage(header, new MessageBuffer("foo"));

  // Wait for message.
  while (result == NULL) {
    Noop<MessageBuffer*>(result);
  }

  EXPECT_EQ(MessageBuffer("foo"), *result);
}

class SynchronousNoop : public App {
 public:
  virtual ~SynchronousNoop() {}
  virtual void HandleMessage(Header* header, MessageBuffer* message) {
    if (header->type() == Header::RPC && header->rpc() == "noop") {
      header->set_to(header->from());
      header->set_from(machine()->machine_id());
      header->set_type(Header::DATA);
      machine()->SendMessage(header, message);
    }
  }
};
REGISTER_APP(SynchronousNoop) {
  return new SynchronousNoop();
}

void LocalRPCBenchmark1() {
  Machine a(0, ClusterConfig::LocalCluster(1));

  StartAppProto sap;
  sap.add_participants(0);
  sap.set_app("SynchronousNoop");
  sap.set_app_name("Noop");
  a.AddApp(sap);
  Spin(1);

  double start = GetTime();
  for (int i = 0; i < 100000; i++) {
    MessageBuffer* result = NULL;
    Header* header = new Header();
    header->set_from(0);
    header->set_to(0);
    header->set_type(Header::RPC);
    header->set_app("Noop");
    header->set_rpc("noop");
    header->set_data_ptr(reinterpret_cast<uint64>(&result));
    a.SendMessage(header, new MessageBuffer());

    // Wait for response.
    SpinUntilNE<MessageBuffer*>(result, NULL);

    delete result;
  }
  LOG(ERROR) << "rpc overhead (one local machine):  "
             << (GetTime() - start) * 10000 << " ns";
}

void LocalRPCBenchmark2() {
  Machine a(0, ClusterConfig::LocalCluster(2));
  Machine b(1, ClusterConfig::LocalCluster(2));

  StartAppProto sap;
  sap.add_participants(0);
  sap.add_participants(1);
  sap.set_app("SynchronousNoop");
  sap.set_app_name("Noop");
  a.AddApp(sap);
  b.AddApp(sap);
  Spin(1);

  double start = GetTime();
  for (int i = 0; i < 1000; i++) {
    MessageBuffer* result = NULL;
    Header* header = new Header();
    header->set_from(0);
    header->set_to(1);
    header->set_type(Header::RPC);
    header->set_app("Noop");
    header->set_rpc("noop");
    header->set_data_ptr(reinterpret_cast<uint64>(&result));
    a.SendMessage(header, new MessageBuffer());

    // Wait for response.
    SpinUntilNE<MessageBuffer*>(result, NULL);

    delete result;
  }
  LOG(ERROR) << "rpc overhead (two local machines): "
             << (GetTime() - start) * 1000000 << " ns";
}

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  if (false) {  // TODO(agt): use gflags
    ProfilerStart("local1.prof");
    LocalRPCBenchmark1();
    ProfilerStop();
    ProfilerStart("local2.prof");
    LocalRPCBenchmark2();
    ProfilerStop();
  }
  return RUN_ALL_TESTS();
}

