// Author: Alexander Thomson (thomson@cs.yale.edu)
//

#include "components/log/paxos.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "machine/cluster_config.h"
#include "machine/machine.h"
#include "common/utils.h"

DEFINE_bool(benchmark, false, "Run benchmarks instead of tests.");

DEFINE_uint64(size, 3, "Number of machines.");

class FakePaxosTest {
 public:
  FakePaxosTest() {
    // Create machines.
    for (uint64 i = 0; i < FLAGS_size; i++) {
      m_.push_back(new Machine(i, ClusterConfig::LocalCluster(FLAGS_size)));
    }
    Spin(0.1);

    // Start app.
    StartAppProto sap;
    for (uint64 i = 0; i < FLAGS_size; i++) {
      sap.add_participants(i);
    }
    sap.set_app("FakePaxosApp");
    sap.set_app_name("Paxos");
    ToScalar<uint64>(FLAGS_size).SerializeToString(sap.mutable_app_args());
    for (uint64 i = 0; i < FLAGS_size; i++) {
      m_[i]->AddApp(sap);
    }
    Spin(0.1);
  }

  ~FakePaxosTest() {
    for (uint32 i = 0; i < m_.size(); i++) {
      delete m_[i];
    }
  }

  void Append(uint64 i, const Slice& entry) {
    reinterpret_cast<FakePaxosApp*>(m_[i]->GetApp("Paxos"))->Append(entry);
  }

  string Lookup(uint64 i, uint64 version) {
    Log::Reader* r =
        reinterpret_cast<FakePaxosApp*>(m_[i]->GetApp("Paxos"))->GetReader();
    r->Seek(version);
    string s = 
      r->Valid()
      ? (UInt64ToString(r->Version()) + ":" + r->Entry().ToString())
      : "ERROR";
    delete r;
    return s;
  }

  template<typename T>
  RemoteLogSource<T>* GetRemoteSource(uint64 i, uint64 j) {
    return new RemoteLogSource<T>(m_[i], j, "Paxos");
  }

 private:  
  vector<Machine*> m_;
};

TEST(FakePaxosTest, AppendOne) {
  FakePaxosTest t;
  t.Append(1, "a");
  Spin(0.001);
  EXPECT_EQ("1:a", t.Lookup(0, 1));
  EXPECT_EQ("1:a", t.Lookup(1, 1));
  EXPECT_EQ("1:a", t.Lookup(2, 1));
}

TEST(FakePaxosTest, AppendMany) {
  FakePaxosTest t;
  for (int i = 1; i <= 1000; i++) {
    t.Append(rand() % FLAGS_size, IntToString(i));
  }
  Spin(0.1);
  for (int i = 1; i <= 1000; i++) {
    for (uint32 j = 0; j < FLAGS_size; j++) {
      EXPECT_EQ(IntToString(i) + ":" + IntToString(i), t.Lookup(j, i));
    }
  }
}

TEST(FakePaxosTest, RemoteRead) {
  FakePaxosTest t;
  Source<string*>* r = t.GetRemoteSource<string>(2, 1);
  string* s;
  EXPECT_FALSE(r->Get(&s));
  t.Append(1, "a");
  while (!r->Get(&s)) {}
  EXPECT_EQ("a", *s);
  delete s;
  delete r;
}

TEST(FakePaxosTest, MoreRemoteReads) {
  FakePaxosTest t;
  Source<string*>* r = t.GetRemoteSource<string>(0, 1);
  string* s;

  EXPECT_FALSE(r->Get(&s));
  for (uint64 i = 1; i <= 100; i++) {
    t.Append(rand() % FLAGS_size, UInt64ToString(i));
  }

  for (uint64 i = 1; i <= 100; i++) {
    while (!r->Get(&s)) {}
    EXPECT_EQ(UInt64ToString(i), *s);
    delete s;
  }
  EXPECT_FALSE(r->Get(&s));

  for (uint64 i = 101; i <= 200; i++) {
    t.Append(rand() % FLAGS_size, UInt64ToString(i));
    while (!r->Get(&s)) {}
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

