// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// REIMP)LMENETNA LL OF THIS FOR MACHINE::AddApp

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "machine/app/app.h"
#include "machine/machine.h"
#include "machine/cluster_config.h"
#include "common/utils.h"

class TestApp : public App {
 public:
  explicit TestApp(int c) : counter_(c) {}
  virtual ~TestApp() {}
  virtual void HandleMessage(Header* header, MessageBuffer* message) {
    counter_++;
  }
  int counter() { return counter_; }

 private:
  int counter_;
};

void SendMessage(Machine* from, uint64 to, const string& appname) {
  Header* header = new Header();
  header->set_from(from->machine_id());
  header->set_to(to);
  header->set_type(Header::RPC);
  header->set_app(appname);
  from->SendMessage(header, new MessageBuffer());
}

REGISTER_APP(TestApp) {
  Scalar p;
  p.ParseFromString(ARG);
  CHECK(p.type() == Scalar::INT32);
  return new TestApp(p.int32_value());
}

TEST(AppStarterTest, OneMachine) {
  // Create machine.
  ClusterConfig cc;
  cc.FromString("0:localhost:10000");
  Machine m(0, cc);

  // Start app.
  StartAppProto sap;
  sap.add_participants(0);
  sap.set_app("TestApp");
  sap.set_app_name("TestAppName");
  ToScalar<int32>(10).SerializeToString(sap.mutable_app_args());
  m.AddApp(sap);
  Spin(0.1);
  EXPECT_EQ(10, reinterpret_cast<TestApp*>(m.GetApp("TestAppName"))->counter());
  SendMessage(&m, 0, "TestAppName");
  Spin(0.1);
  EXPECT_EQ(11, reinterpret_cast<TestApp*>(m.GetApp("TestAppName"))->counter());
}

TEST(AppStarterTest, MultipleMachines) {
  // Create machine.
  ClusterConfig cc;
  cc.FromString("0:localhost:10000\n1:localhost:10001\n2:localhost:10002");
  Machine a(0, cc);
  Machine b(1, cc);
  Machine c(2, cc);
  Spin(0.2);

  // Start app.
  StartAppProto sap;
  sap.add_participants(0);
  sap.add_participants(1);
  sap.set_app("TestApp");
  sap.set_app_name("TestAppName");
  ToScalar<int32>(10).SerializeToString(sap.mutable_app_args());
  a.AddApp(sap);
  b.AddApp(sap);
  c.AddApp(sap);
  Spin(0.1);
  EXPECT_TRUE(a.GetApp("TestAppName") != NULL);
  EXPECT_EQ(10, reinterpret_cast<TestApp*>(a.GetApp("TestAppName"))->counter());
  EXPECT_EQ(10, reinterpret_cast<TestApp*>(b.GetApp("TestAppName"))->counter());
  SendMessage(&a, 0, "TestAppName");
  SendMessage(&a, 1, "TestAppName");
  Spin(0.1);
  EXPECT_EQ(11, reinterpret_cast<TestApp*>(a.GetApp("TestAppName"))->counter());
  EXPECT_EQ(11, reinterpret_cast<TestApp*>(b.GetApp("TestAppName"))->counter());
}

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

