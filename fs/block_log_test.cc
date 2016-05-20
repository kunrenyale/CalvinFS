// Author: Alexander Thomson <thomson@cs.yale.edu>
//

#include "fs/block_log.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "common/utils.h"
#include "fs/calvinfs.h"

DEFINE_bool(benchmark, false, "Run benchmarks instead of unit tests.");

class BlockLogTest {
 public:
  BlockLogTest(int n, int r) : config_(MakeCalvinFSConfig(n, r)) {
    string fsconfig;
    MakeCalvinFSConfig(n, r).SerializeToString(&fsconfig);

    // Create machines; Start blockstore app.
    for (int i = 0; i < n*r; i++) {
      m_.push_back(new Machine(i, ClusterConfig::LocalCluster(n*r)));
      m_[i]->AppData()->Put("calvinfs-config", fsconfig);
      m_[i]->AddApp("DistributedBlockStore", "blockstore");
    }
    // Start Paxos app.
    StartAppProto sap;
    if (r == 1) {
      for (int i = 0; i < 3; i++) {
        sap.add_participants(i);
      }
    } else {
      for (int i = 0; i < r; i++) {
        sap.add_participants(i * n);
      }
    }
    sap.set_app("Paxos2App2");
    sap.set_app_name("paxos2");
    string args;
    sap.SerializeToString(&args);
    sap.set_app_args(args);
    for (int i = 0; i < sap.participants_size(); i++) {
      m_[sap.participants(i)]->AddApp(sap);
    }

    // Start BlockLogApp.
    for (int i = 0; i < n*r; i++) {
      m_[i]->AddApp("BlockLogApp", "blocklog");
      bl_.push_back(reinterpret_cast<BlockLogApp*>(m_[i]->GetApp("blocklog")));
      actions_.push_back(bl_[i]->GetActionSource());
    }
  }

  ~BlockLogTest() {
    for (uint32 i = 0; i < m_.size(); i++) {
      delete m_[i];
    }
  }

  CalvinFSConfigMap config_;
  vector<Machine*> m_;
  vector<BlockLogApp*> bl_;
  vector<Source<Action*>*> actions_;
};

TEST(BlockLogTest, ThreePartitionsOneReplica) {
  BlockLogTest t(3, 1);

  // Run a while without actually logging anything.
  Spin(1);

  // Log something.
  Action a;
  a.set_action_type(1);
  a.set_input("input");

  // Make sure that "a" to machine 1 (for replica 0).
  EXPECT_TRUE(
      t.config_.LookupMetadataShard(t.config_.HashFileName("a"), 0) == 1);
  a.add_readset("a");

  string s;
  a.SerializeToString(&s);
  t.bl_[1]->Append(s);

  // Wait a bit more for something bad to happen.
  Spin(1);

  // See if it got appended.
  Action* b = NULL;
  EXPECT_FALSE(t.actions_[0]->Get(&b));
  EXPECT_FALSE(t.actions_[2]->Get(&b));
  EXPECT_TRUE(t.actions_[1]->Get(&b));
  a.set_version(1);
  EXPECT_EQ(a.DebugString(), b->DebugString());
  delete b;
}

TEST(BlockLogTest, FivePartitionsThreeReplicas) {
  BlockLogTest t(5, 3);

  // Run a while without actually logging anything.
  Spin(1);

  // Log something.
  Action a;
  a.set_action_type(1);
  a.set_input("input");
  a.add_readset("a");

  // Look up what shards will see the action.
  set<int> recipients;
  for (int r = 0; r < 3; r++) {
    recipients.insert(
        t.config_.LookupMetadataShard(t.config_.HashFileName("a"), r));
  }

  string s;
  a.SerializeToString(&s);
  t.bl_[1]->Append(s);

  // Wait a bit more for something bad to happen.
  Spin(1);

  // See if it got appended.
  Action* b = NULL;
  for (int i = 0; i < 15; i++) {
    if (recipients.count(i) != 0) {
      EXPECT_TRUE(t.actions_[i]->Get(&b));
      a.set_version(1);
      EXPECT_EQ(a.DebugString(), b->DebugString());
      delete b;
    } else {
      EXPECT_FALSE(t.actions_[i]->Get(&b));
    }
  }
}

struct LogEntry {
  Action* action;
  set<uint64> partitions;
};

TEST(BlockLogTest, ManyAppends) {
  int count = 100;
  int partitions = 3;
  int replicas = 3;
  int machines = partitions * replicas;
  BlockLogTest t(partitions, replicas);

  // Create list of actions to append (and note what metadata shards they will
  // be forwarded to).
  vector<LogEntry> entries;
  entries.resize(count);
  for (int i = 0; i < count; i++) {
    entries[i].action = new Action();
    entries[i].action->set_action_type(i);
    entries[i].action->set_input(IntToString(i));
    entries[i].action->add_readset("r" + IntToString(i));
    entries[i].action->add_writeset("w" + IntToString(i));
    uint64 rmds = t.config_.HashFileName(entries[i].action->readset(0));
    uint64 wmds = t.config_.HashFileName(entries[i].action->writeset(0));
    for (int r = 0; r < replicas; r++) {
      entries[i].partitions.insert(t.config_.LookupMetadataShard(rmds, r));
      entries[i].partitions.insert(t.config_.LookupMetadataShard(wmds, r));
    }
  }

  // Add actions.
  for (int i = 0; i < count; i++) {
    t.bl_[rand() % machines]->Append(entries[i].action);
  }

  // Read log from each machine's perspective.
  for (int m = 0; m < machines; m++) {
    set<int> expected_actions;
    for (int i = 0; i < count; i++) {
      if (entries[i].partitions.count(m) != 0) {
        expected_actions.insert(i);
      }
    }
    for (uint32 i = 0; i < expected_actions.size(); i++) {
      Action* b = NULL;
      while (!t.actions_[m]->Get(&b)) {
        // Wait for next action to appear if necessary.
      }
      EXPECT_TRUE(expected_actions.count(StringToInt(b->input())));
      delete b;
    }
    // No more should appear.
    Action* b = NULL;
    EXPECT_FALSE(t.actions_[m]->Get(&b));
  }
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

