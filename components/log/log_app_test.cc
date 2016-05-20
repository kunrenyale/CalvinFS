// Author: Alexander Thomson (thomson@cs.yale.edu)
//

#include "components/log/log_app.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "common/utils.h"
#include "machine/cluster_config.h"
#include "machine/machine.h"
#include "proto/action.pb.h"

DEFINE_bool(benchmark, false, "Run benchmarks instead of tests.");

class LogAppTest {
 public:
  LogAppTest()
      : next_version_(1),
        a_(0, ClusterConfig::LocalCluster(2)),
        b_(1, ClusterConfig::LocalCluster(2)) {
    // Start app.
    StartAppProto sap;
    sap.add_participants(0);
    sap.set_app("LogApp");
    sap.set_app_name("LogApp");
    a_.AddApp(sap);
    Spin(0.1);
  }

  ~LogAppTest() {}

  void Append(const Slice& entry) {
    reinterpret_cast<LogApp*>(a_.GetApp("LogApp"))
        ->log_->Append(next_version_++, entry);
  }

  string Lookup(uint64 version) {
    Log::Reader* r =
        reinterpret_cast<LogApp*>(a_.GetApp("LogApp"))->log_->GetReader();
    r->Seek(version);
    string s =
      r->Valid()
      ? (UInt64ToString(r->Version()) + ":" + r->Entry().ToString())
      : "ERROR";
    delete r;
    return s;
  }

  template<typename T>
  RemoteLogSource<T>* GetRemoteSource() {
    return new RemoteLogSource<T>(&b_, 0, "LogApp");
  }

 private:
  uint64 next_version_;
  Machine a_;
  Machine b_;
};

TEST(LogAppTest, AppendOne) {
  LogAppTest t;
  t.Append("a");
  Spin(0.001);
  EXPECT_EQ("1:a", t.Lookup(1));
}

TEST(LogAppTest, RemoteRead) {
  LogAppTest t;
  Source<string*>* r = t.GetRemoteSource<string>();
  string* s;
  EXPECT_FALSE(r->Get(&s));
  t.Append("a");
  EXPECT_TRUE(r->Get(&s));
  EXPECT_EQ("a", *s);
  delete s;
  delete r;
}

TEST(LogAppTest, RemoteReadAction) {
  LogAppTest t;
  Source<Action*>* r = t.GetRemoteSource<Action>();
  string as;
  Action a;
  a.set_action_type(1);
  a.set_input("input");
  a.SerializeToString(&as);
  t.Append(as);
  Action* b;
  EXPECT_TRUE(r->Get(&b));
  EXPECT_EQ(a.input(), b->input());
  delete b;
  delete r;
}

TEST(LogAppTest, MoreRemoteReads) {
  LogAppTest t;
  Source<string*>* r = t.GetRemoteSource<string>();
  string* s;

  EXPECT_FALSE(r->Get(&s));
  for (uint64 i = 1; i <= 100; i++) {
    t.Append(UInt64ToString(i));
  }

  for (uint64 i = 1; i <= 100; i++) {
    EXPECT_TRUE(r->Get(&s));
    EXPECT_EQ(UInt64ToString(i), *s);
    delete s;
  }
  EXPECT_FALSE(r->Get(&s));

  for (uint64 i = 101; i <= 200; i++) {
    t.Append(UInt64ToString(i));
    EXPECT_TRUE(r->Get(&s));
    EXPECT_EQ(UInt64ToString(i), *s);
    delete s;
    EXPECT_FALSE(r->Get(&s));
  }
  delete r;
}

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  if (FLAGS_benchmark) {
    return 0;
  } else {
    return RUN_ALL_TESTS();
  }
}

