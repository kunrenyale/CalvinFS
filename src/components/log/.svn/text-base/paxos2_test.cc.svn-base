// Author: Alexander Thomson (thomson@cs.yale.edu)
//

#include "components/log/paxos2.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "machine/cluster_config.h"
#include "machine/machine.h"
#include "common/utils.h"

DEFINE_bool(benchmark, false, "Run benchmarks instead of tests.");

DEFINE_uint64(size, 3, "Number of machines.");

class Paxos2Test {
 public:
  Paxos2Test() {
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
    sap.set_app("Paxos2App");
    sap.set_app_name("paxos2");
    ToScalar<uint64>(FLAGS_size).SerializeToString(sap.mutable_app_args());
    for (uint64 i = 0; i < FLAGS_size; i++) {
      m_[i]->AddApp(sap);
    }
    Spin(0.1);
  }

  ~Paxos2Test() {
    for (uint32 i = 0; i < m_.size(); i++) {
      delete m_[i];
    }
  }

  void Append(uint64 i, uint64 entry, int count) {
    reinterpret_cast<Paxos2App*>(m_[i]->GetApp("paxos2"))->Append(entry, count);
  }

  string Lookup(uint64 i, uint64 version) {
    Log::Reader* r =
        reinterpret_cast<Paxos2App*>(m_[i]->GetApp("paxos2"))->GetReader();
    r->Seek(version);
    if (r->Valid()) {
      string s = r->Entry().ToString();
      delete r;
      PairSequence p;
      p.ParseFromString(s);
      s.clear();
      uint64 offset = p.misc();
      for (int i = 0; i < p.pairs_size(); i++) {
        s.append(UInt64ToString(p.pairs(i).first()) + ":" +
                 UInt64ToString(offset) + ",");
        offset += p.pairs(i).second();
      }
      s.resize(s.size() - 1);
      return s;
    } else {
      delete r;
      return "ERROR";
    }
  }

  template<typename T>
  RemoteLogSource<T>* GetRemoteSource(uint64 i, uint64 j) {
    return new RemoteLogSource<T>(m_[i], j, "paxos2");
  }

 private:
  vector<Machine*> m_;
};

TEST(Paxos2Test, DoesItWork) {
  Paxos2Test t;
  t.Append(0, 101, 4);
  t.Append(0, 102, 3);
  Spin(1);
  EXPECT_EQ("101:1,102:5", t.Lookup(0, 1));
  EXPECT_EQ("101:1,102:5", t.Lookup(1, 1));
  EXPECT_EQ("101:1,102:5", t.Lookup(2, 1));
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

