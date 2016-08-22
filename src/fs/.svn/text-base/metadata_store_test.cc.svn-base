// Author: Alexander Thomson <thomson@cs.yale.edu>
//

#include "fs/metadata_store.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <set>

#include "components/store/kvstore.h"
#include "components/store/versioned_kvstore.h"
#include "components/store/btreestore.h"
#include "components/store/store_app.h"
#include "fs/calvinfs.h"
#include "machine/cluster_config.h"
#include "machine/machine.h"

using std::set;

DEFINE_bool(benchmark, false, "Run benchmarks instead of unit tests.");

////////////////////////////////////////////////////////////////////////////////
// Local tests:

TEST(MetadataStoreTest, CheckRootDir) {
  VersionedKVStore* base = new VersionedKVStore(new BTreeStore());
  MetadataStore md(base);

  // Check that root directory exists in base.
  string s;
  EXPECT_TRUE(base->Exists("", 1));
  EXPECT_TRUE(base->Get("", 1, &s));
  MetadataEntry e;
  e.ParseFromString(s);
  EXPECT_TRUE(e.permissions().owner() == 0);
  EXPECT_TRUE(e.permissions().group() == 0);
  EXPECT_EQ("777", e.permissions().mode());
  EXPECT_EQ(DIR, e.type());
  EXPECT_EQ(0, e.dir_contents_size());

  // Read root directory using a LOOKUP action.
  Action a;
  a.set_action_type(MetadataAction::LOOKUP);
  a.set_version(1);
  MetadataAction::LookupInput in;
  in.set_path("");
  in.mutable_permissions();
  in.SerializeToString(a.mutable_input());

  md.GetRWSets(&a);
  md.Run(&a);

  EXPECT_TRUE(a.has_output());
  MetadataAction::LookupOutput out;
  out.ParseFromString(a.output());
  EXPECT_TRUE(out.success());
  EXPECT_EQ(DIR, out.entry().type());
  EXPECT_EQ(0, out.entry().dir_contents_size());

  // TODO(agt): Check that root dir cannot be deleted.
}

TEST(MetadataStoreTest, CreateAppendResizeRead) {
  VersionedKVStore* base = new VersionedKVStore(new BTreeStore());
  MetadataStore md(base);

  // mkdir /foo
  Action a;
  a.set_action_type(MetadataAction::CREATE_FILE);
  a.set_version(1);
  MetadataAction::CreateFileInput ci;
  ci.set_path("/foo");
  ci.mutable_permissions();
  ci.set_type(DIR);
  ci.SerializeToString(a.mutable_input());

  md.GetRWSets(&a);
  md.Run(&a);

  EXPECT_TRUE(a.has_output());
  MetadataAction::CreateFileOutput co;
  co.ParseFromString(a.output());
  EXPECT_TRUE(co.success());

  // entry /foo exists in base store
  EXPECT_TRUE(base->Exists("", 3));
  EXPECT_TRUE(base->Exists("/foo", 2));
  EXPECT_FALSE(base->Exists("/foo/bar", 2));
  EXPECT_FALSE(base->Exists("/bar", 2));

  // lookup /foo
  a.Clear();
  a.set_action_type(MetadataAction::LOOKUP);
  a.set_version(2);
  MetadataAction::LookupInput li;
  li.set_path("/foo");
  li.mutable_permissions();
  li.SerializeToString(a.mutable_input());

  md.GetRWSets(&a);
  md.Run(&a);

  EXPECT_TRUE(a.has_output());
  MetadataAction::LookupOutput lo;
  lo.ParseFromString(a.output());
  EXPECT_TRUE(lo.success());
  EXPECT_EQ(DIR, lo.entry().type());
  EXPECT_EQ(0, lo.entry().dir_contents_size());

  // touch /foo/bar
  a.Clear();
  a.set_action_type(MetadataAction::CREATE_FILE);
  a.set_version(2);
  ci.set_path("/foo/bar");
  ci.set_type(DATA);
  ci.SerializeToString(a.mutable_input());

  md.GetRWSets(&a);
  md.Run(&a);

  EXPECT_TRUE(a.has_output());
  co.Clear();
  co.ParseFromString(a.output());
  EXPECT_TRUE(co.success());

  // entries /foo and /foo/bar exist in base store
  EXPECT_TRUE(base->Exists("", 3));
  EXPECT_TRUE(base->Exists("/foo", 3));
  EXPECT_TRUE(base->Exists("/foo/bar", 3));
  EXPECT_FALSE(base->Exists("/foo/ba", 3));
  EXPECT_FALSE(base->Exists("/foo/bar/", 3));
  EXPECT_FALSE(base->Exists("/foo/baz", 3));
  EXPECT_FALSE(base->Exists("/bar", 3));

  // lookup /foo again
  a.Clear();
  a.set_action_type(MetadataAction::LOOKUP);
  a.set_version(3);
  li.Clear();
  li.set_path("/foo");
  li.mutable_permissions();
  li.SerializeToString(a.mutable_input());

  md.GetRWSets(&a);
  md.Run(&a);

  EXPECT_TRUE(a.has_output());
  lo.Clear();
  lo.ParseFromString(a.output());
  EXPECT_TRUE(lo.success());
  EXPECT_EQ(DIR, lo.entry().type());
  EXPECT_EQ(1, lo.entry().dir_contents_size());
  EXPECT_EQ("bar", lo.entry().dir_contents(0));

  // lookup /foo/ba (should fail)
  a.Clear();
  a.set_action_type(MetadataAction::LOOKUP);
  a.set_version(3);
  li.Clear();
  li.set_path("/foo/ba");
  li.mutable_permissions();
  li.SerializeToString(a.mutable_input());

  md.GetRWSets(&a);
  md.Run(&a);

  EXPECT_TRUE(a.has_output());
  lo.Clear();
  lo.ParseFromString(a.output());
  EXPECT_FALSE(lo.success());

  // lookup /foo/bar
  a.Clear();
  a.set_action_type(MetadataAction::LOOKUP);
  a.set_version(3);
  li.Clear();
  li.set_path("/foo/bar");
  li.mutable_permissions();
  li.SerializeToString(a.mutable_input());

  md.GetRWSets(&a);
  md.Run(&a);

  EXPECT_TRUE(a.has_output());
  lo.Clear();
  lo.ParseFromString(a.output());
  EXPECT_TRUE(lo.success());
  EXPECT_EQ(DATA, lo.entry().type());
  EXPECT_EQ(0, lo.entry().file_parts_size());

  // append to /foo/bar
  a.Clear();
  a.set_action_type(MetadataAction::APPEND);
  a.set_version(3);
  MetadataAction::AppendInput ai;
  ai.set_path("/foo/bar");
  ai.mutable_permissions();
  ai.add_data()->set_length(10);
  ai.add_data()->set_length(20);
  ai.mutable_data(1)->set_block_id(30);
  ai.mutable_data(1)->set_block_offset(40);
  ai.add_data()->set_length(50);
  ai.SerializeToString(a.mutable_input());

  md.GetRWSets(&a);
  md.Run(&a);

  EXPECT_TRUE(a.has_output());
  MetadataAction::AppendOutput ao;
  ao.ParseFromString(a.output());
  EXPECT_TRUE(ao.success());

  // lookup /foo/bar again
  a.Clear();
  a.set_action_type(MetadataAction::LOOKUP);
  a.set_version(4);
  li.Clear();
  li.set_path("/foo/bar");
  li.mutable_permissions();
  li.SerializeToString(a.mutable_input());

  md.GetRWSets(&a);
  md.Run(&a);

  EXPECT_TRUE(a.has_output());
  lo.Clear();
  lo.ParseFromString(a.output());
  EXPECT_TRUE(lo.success());
  EXPECT_EQ(DATA, lo.entry().type());
  EXPECT_EQ(3, lo.entry().file_parts_size());
  EXPECT_TRUE(lo.entry().file_parts(0).length() == 10);
  EXPECT_FALSE(lo.entry().file_parts(0).has_block_id());
  EXPECT_FALSE(lo.entry().file_parts(0).has_block_offset());
  EXPECT_TRUE(lo.entry().file_parts(1).length() == 20);
  EXPECT_TRUE(lo.entry().file_parts(1).block_id() == 30);
  EXPECT_TRUE(lo.entry().file_parts(1).block_offset() == 40);
  EXPECT_TRUE(lo.entry().file_parts(2).length() == 50);
  EXPECT_FALSE(lo.entry().file_parts(2).has_block_id());
  EXPECT_FALSE(lo.entry().file_parts(2).has_block_offset());

  // resize (truncate) /foo/bar
  a.Clear();
  a.set_action_type(MetadataAction::RESIZE);
  a.set_version(4);
  MetadataAction::ResizeInput ri;
  ri.set_path("/foo/bar");
  ri.mutable_permissions();
  ri.set_size(25);
  ri.SerializeToString(a.mutable_input());

  md.GetRWSets(&a);
  md.Run(&a);

  EXPECT_TRUE(a.has_output());
  MetadataAction::ResizeOutput ro;
  ro.ParseFromString(a.output());
  EXPECT_TRUE(ro.success());

  // lookup /foo/bar again
  a.Clear();
  a.set_action_type(MetadataAction::LOOKUP);
  a.set_version(5);
  li.Clear();
  li.set_path("/foo/bar");
  li.mutable_permissions();
  li.SerializeToString(a.mutable_input());

  md.GetRWSets(&a);
  md.Run(&a);

  EXPECT_TRUE(a.has_output());
  lo.Clear();
  lo.ParseFromString(a.output());
  EXPECT_TRUE(lo.success());
  EXPECT_EQ(DATA, lo.entry().type());
  EXPECT_EQ(2, lo.entry().file_parts_size());
  EXPECT_TRUE(lo.entry().file_parts(0).length() == 10);
  EXPECT_FALSE(lo.entry().file_parts(0).has_block_id());
  EXPECT_FALSE(lo.entry().file_parts(0).has_block_offset());
  EXPECT_TRUE(lo.entry().file_parts(1).length() == 15);
  EXPECT_TRUE(lo.entry().file_parts(1).block_id() == 30);
  EXPECT_TRUE(lo.entry().file_parts(1).block_offset() == 40);

  // resize (extend) /foo/bar
  a.Clear();
  a.set_action_type(MetadataAction::RESIZE);
  a.set_version(5);
  ri.Clear();
  ri.set_path("/foo/bar");
  ri.mutable_permissions();
  ri.set_size(40);
  ri.SerializeToString(a.mutable_input());

  md.GetRWSets(&a);
  md.Run(&a);

  EXPECT_TRUE(a.has_output());
  ro.Clear();
  ro.ParseFromString(a.output());
  EXPECT_TRUE(ro.success());

  // lookup /foo/bar again
  a.Clear();
  a.set_action_type(MetadataAction::LOOKUP);
  a.set_version(6);
  li.Clear();
  li.set_path("/foo/bar");
  li.mutable_permissions();
  li.SerializeToString(a.mutable_input());

  md.GetRWSets(&a);
  md.Run(&a);

  EXPECT_TRUE(a.has_output());
  lo.Clear();
  lo.ParseFromString(a.output());
  EXPECT_TRUE(lo.success());
  EXPECT_EQ(DATA, lo.entry().type());
  EXPECT_EQ(3, lo.entry().file_parts_size());
  EXPECT_TRUE(lo.entry().file_parts(0).length() == 10);
  EXPECT_FALSE(lo.entry().file_parts(0).has_block_id());
  EXPECT_FALSE(lo.entry().file_parts(0).has_block_offset());
  EXPECT_TRUE(lo.entry().file_parts(1).length() == 15);
  EXPECT_TRUE(lo.entry().file_parts(1).block_id() == 30);
  EXPECT_TRUE(lo.entry().file_parts(1).block_offset() == 40);
  EXPECT_TRUE(lo.entry().file_parts(2).length() == 15);
  EXPECT_FALSE(lo.entry().file_parts(2).has_block_id());
  EXPECT_FALSE(lo.entry().file_parts(2).has_block_offset());
}

////////////////////////////////////////////////////////////////////////////////
// DISTRIBUTED TESTS

class MetadataStoreTest {
 public:
  MetadataStoreTest(int n, int r) : config_(MakeCalvinFSConfig(n, r)) {
    string fsconfig;
    MakeCalvinFSConfig(n, r).SerializeToString(&fsconfig);

    // Create machines; Start blockstore app.
    for (int i = 0; i < n*r; i++) {
      m_.push_back(new Machine(i, ClusterConfig::LocalCluster(n*r)));
      m_[i]->AppData()->Put("calvinfs-config", fsconfig);
      m_[i]->AddApp("MetadataStoreApp", "mds");
      mds_.push_back(reinterpret_cast<StoreApp*>(m_[i]->GetApp("mds")));
      reinterpret_cast<MetadataStore*>(mds_[i]->store())->SetMachine(m_[i]);
    }
  }

  ~MetadataStoreTest() {
    for (uint32 i = 0; i < m_.size(); i++) {
      delete m_[i];
    }
  }

  // Returns total number of participants.
  int RunAction(Action* action) {
    // Compute readers and writers.
    set<uint64> participants;
    for (int i = 0; i < action->readset_size(); i++) {
      uint64 mds = config_.HashFileName(action->readset(i));
      for (uint32 r = 0; r < config_.config().metadata_replication_factor(); r++) {
        participants.insert(config_.LookupMetadataShard(mds, r));
      }
    }
    for (int i = 0; i < action->writeset_size(); i++) {
      uint64 mds = config_.HashFileName(action->writeset(i));
      for (uint32 r = 0; r < config_.config().metadata_replication_factor(); r++) {
        participants.insert(config_.LookupMetadataShard(mds, r));
      }
    }

    AtomicQueue<Action*> done;
    map<uint64, Action*> actions;
    for (auto it = participants.begin(); it != participants.end(); ++it) {
      actions[*it] = new Action(*action);
      Header* header = new Header();
      header->set_from(*it);
      header->set_to(*it);
      header->set_type(Header::RPC);
      header->set_app("mds");
      header->set_rpc("RUNLOCAL");
      header->add_misc_int(reinterpret_cast<uint64>(actions[*it]));
      header->add_misc_int(reinterpret_cast<uint64>(&done));
      m_[*it]->SendMessage(header, new MessageBuffer());
    }
    for (uint32 i = 0; i < participants.size(); i++) {
      Action* a = NULL;
      while (!done.Pop(&a)) {
        usleep(10);
      }
      if (i == 0) {
        action->Clear();
        action->CopyFrom(*a);
      } else {
        CHECK_EQ(action->DebugString(), a->DebugString());
      }
      delete a;
    }

    return participants.size();
  }

  CalvinFSConfigMap config_;
  vector<Machine*> m_;
  vector<StoreApp*> mds_;
};

TEST(MetadataStoreTest, OneMachine) {
  MetadataStoreTest t(1, 1);
  
  // mkdir /foo
  Action a;
  a.set_action_type(MetadataAction::CREATE_FILE);
  a.set_version(1);
  MetadataAction::CreateFileInput ci;
  ci.set_path("/foo");
  ci.mutable_permissions();
  ci.set_type(DIR);
  ci.SerializeToString(a.mutable_input());

  t.mds_[0]->GetRWSets(&a);
  EXPECT_EQ(1, t.RunAction(&a));

  EXPECT_TRUE(a.has_output());
  MetadataAction::CreateFileOutput co;
  co.ParseFromString(a.output());
  EXPECT_TRUE(co.success());
}

TEST(MetadataStoreTest, TwoMachines) {
  MetadataStoreTest t(2, 1);

  // mkdir /foo
  Action a;
  a.set_action_type(MetadataAction::CREATE_FILE);
  a.set_version(1);
  MetadataAction::CreateFileInput ci;
  ci.set_path("/foo");
  ci.mutable_permissions();
  ci.set_type(DIR);
  ci.SerializeToString(a.mutable_input());

  t.mds_[0]->GetRWSets(&a);
  EXPECT_EQ(2, t.RunAction(&a));

  EXPECT_TRUE(a.has_output());
  MetadataAction::CreateFileOutput co;
  co.ParseFromString(a.output());
  EXPECT_TRUE(co.success());
}

TEST(MetadataStoreTest, TwoPartitionsTwoReplicas) {
  MetadataStoreTest t(2, 2);

  // mkdir /foo
  Action a;
  a.set_action_type(MetadataAction::CREATE_FILE);
  a.set_version(1);
  MetadataAction::CreateFileInput ci;
  ci.set_path("/foo");
  ci.mutable_permissions();
  ci.set_type(DIR);
  ci.SerializeToString(a.mutable_input());

  t.mds_[0]->GetRWSets(&a);
  EXPECT_EQ(4, t.RunAction(&a));

  EXPECT_TRUE(a.has_output());
  MetadataAction::CreateFileOutput co;
  co.ParseFromString(a.output());
  EXPECT_TRUE(co.success());
}

TEST(MetadataStoreTest, MoreActions) {
  MetadataStoreTest t(3, 3);

  // mkdir /foo
  Action a;
  a.set_action_type(MetadataAction::CREATE_FILE);
  a.set_version(1);
  MetadataAction::CreateFileInput ci;
  ci.set_path("/foo");
  ci.mutable_permissions();
  ci.set_type(DIR);
  ci.SerializeToString(a.mutable_input());

  t.mds_[0]->GetRWSets(&a);
  EXPECT_EQ(6, t.RunAction(&a));

  EXPECT_TRUE(a.has_output());
  MetadataAction::CreateFileOutput co;
  co.ParseFromString(a.output());
  EXPECT_TRUE(co.success());

  // lookup /foo
  a.Clear();
  a.set_action_type(MetadataAction::LOOKUP);
  a.set_version(2);
  MetadataAction::LookupInput li;
  li.set_path("/foo");
  li.mutable_permissions();
  li.SerializeToString(a.mutable_input());

  t.mds_[0]->GetRWSets(&a);
  EXPECT_EQ(3, t.RunAction(&a));

  EXPECT_TRUE(a.has_output());
  MetadataAction::LookupOutput lo;
  lo.ParseFromString(a.output());
  EXPECT_TRUE(lo.success());
  EXPECT_EQ(DIR, lo.entry().type());
  EXPECT_EQ(0, lo.entry().dir_contents_size());

  // touch /foo/bar
  a.Clear();
  a.set_action_type(MetadataAction::CREATE_FILE);
  a.set_version(2);
  ci.set_path("/foo/bar");
  ci.set_type(DATA);
  ci.SerializeToString(a.mutable_input());

  t.mds_[0]->GetRWSets(&a);
  EXPECT_EQ(6, t.RunAction(&a));

  EXPECT_TRUE(a.has_output());
  co.Clear();
  co.ParseFromString(a.output());
  EXPECT_TRUE(co.success());

  // lookup /foo again
  a.Clear();
  a.set_action_type(MetadataAction::LOOKUP);
  a.set_version(3);
  li.Clear();
  li.set_path("/foo");
  li.mutable_permissions();
  li.SerializeToString(a.mutable_input());

  t.mds_[0]->GetRWSets(&a);
  EXPECT_EQ(3, t.RunAction(&a));

  EXPECT_TRUE(a.has_output());
  lo.Clear();
  lo.ParseFromString(a.output());
  EXPECT_TRUE(lo.success());
  EXPECT_EQ(DIR, lo.entry().type());
  EXPECT_EQ(1, lo.entry().dir_contents_size());
  EXPECT_EQ("bar", lo.entry().dir_contents(0));

  // lookup /foo/ba (should fail)
  a.Clear();
  a.set_action_type(MetadataAction::LOOKUP);
  a.set_version(3);
  li.Clear();
  li.set_path("/foo/ba");
  li.mutable_permissions();
  li.SerializeToString(a.mutable_input());

  t.mds_[0]->GetRWSets(&a);
  EXPECT_EQ(3, t.RunAction(&a));

  EXPECT_TRUE(a.has_output());
  lo.Clear();
  lo.ParseFromString(a.output());
  EXPECT_FALSE(lo.success());

  // lookup /foo/bar
  a.Clear();
  a.set_action_type(MetadataAction::LOOKUP);
  a.set_version(3);
  li.Clear();
  li.set_path("/foo/bar");
  li.mutable_permissions();
  li.SerializeToString(a.mutable_input());

  t.mds_[0]->GetRWSets(&a);
  EXPECT_EQ(3, t.RunAction(&a));

  EXPECT_TRUE(a.has_output());
  lo.Clear();
  lo.ParseFromString(a.output());
  EXPECT_TRUE(lo.success());
  EXPECT_EQ(DATA, lo.entry().type());
  EXPECT_EQ(0, lo.entry().file_parts_size());

  // append to /foo/bar
  a.Clear();
  a.set_action_type(MetadataAction::APPEND);
  a.set_version(3);
  MetadataAction::AppendInput ai;
  ai.set_path("/foo/bar");
  ai.mutable_permissions();
  ai.add_data()->set_length(10);
  ai.add_data()->set_length(20);
  ai.mutable_data(1)->set_block_id(30);
  ai.mutable_data(1)->set_block_offset(40);
  ai.add_data()->set_length(50);
  ai.SerializeToString(a.mutable_input());

  t.mds_[0]->GetRWSets(&a);
  EXPECT_EQ(3, t.RunAction(&a));

  EXPECT_TRUE(a.has_output());
  MetadataAction::AppendOutput ao;
  ao.ParseFromString(a.output());
  EXPECT_TRUE(ao.success());

  // lookup /foo/bar again
  a.Clear();
  a.set_action_type(MetadataAction::LOOKUP);
  a.set_version(4);
  li.Clear();
  li.set_path("/foo/bar");
  li.mutable_permissions();
  li.SerializeToString(a.mutable_input());

  t.mds_[0]->GetRWSets(&a);
  EXPECT_EQ(3, t.RunAction(&a));

  EXPECT_TRUE(a.has_output());
  lo.Clear();
  lo.ParseFromString(a.output());
  EXPECT_TRUE(lo.success());
  EXPECT_EQ(DATA, lo.entry().type());
  EXPECT_EQ(3, lo.entry().file_parts_size());
  EXPECT_TRUE(lo.entry().file_parts(0).length() == 10);
  EXPECT_FALSE(lo.entry().file_parts(0).has_block_id());
  EXPECT_FALSE(lo.entry().file_parts(0).has_block_offset());
  EXPECT_TRUE(lo.entry().file_parts(1).length() == 20);
  EXPECT_TRUE(lo.entry().file_parts(1).block_id() == 30);
  EXPECT_TRUE(lo.entry().file_parts(1).block_offset() == 40);
  EXPECT_TRUE(lo.entry().file_parts(2).length() == 50);
  EXPECT_FALSE(lo.entry().file_parts(2).has_block_id());
  EXPECT_FALSE(lo.entry().file_parts(2).has_block_offset());

  // resize (truncate) /foo/bar
  a.Clear();
  a.set_action_type(MetadataAction::RESIZE);
  a.set_version(4);
  MetadataAction::ResizeInput ri;
  ri.set_path("/foo/bar");
  ri.mutable_permissions();
  ri.set_size(25);
  ri.SerializeToString(a.mutable_input());

  t.mds_[0]->GetRWSets(&a);
  EXPECT_EQ(3, t.RunAction(&a));

  EXPECT_TRUE(a.has_output());
  MetadataAction::ResizeOutput ro;
  ro.ParseFromString(a.output());
  EXPECT_TRUE(ro.success());

  // lookup /foo/bar again
  a.Clear();
  a.set_action_type(MetadataAction::LOOKUP);
  a.set_version(5);
  li.Clear();
  li.set_path("/foo/bar");
  li.mutable_permissions();
  li.SerializeToString(a.mutable_input());

  t.mds_[0]->GetRWSets(&a);
  EXPECT_EQ(3, t.RunAction(&a));

  EXPECT_TRUE(a.has_output());
  lo.Clear();
  lo.ParseFromString(a.output());
  EXPECT_TRUE(lo.success());
  EXPECT_EQ(DATA, lo.entry().type());
  EXPECT_EQ(2, lo.entry().file_parts_size());
  EXPECT_TRUE(lo.entry().file_parts(0).length() == 10);
  EXPECT_FALSE(lo.entry().file_parts(0).has_block_id());
  EXPECT_FALSE(lo.entry().file_parts(0).has_block_offset());
  EXPECT_TRUE(lo.entry().file_parts(1).length() == 15);
  EXPECT_TRUE(lo.entry().file_parts(1).block_id() == 30);
  EXPECT_TRUE(lo.entry().file_parts(1).block_offset() == 40);

  // resize (extend) /foo/bar
  a.Clear();
  a.set_action_type(MetadataAction::RESIZE);
  a.set_version(5);
  ri.Clear();
  ri.set_path("/foo/bar");
  ri.mutable_permissions();
  ri.set_size(40);
  ri.SerializeToString(a.mutable_input());

  t.mds_[0]->GetRWSets(&a);
  EXPECT_EQ(3, t.RunAction(&a));

  EXPECT_TRUE(a.has_output());
  ro.Clear();
  ro.ParseFromString(a.output());
  EXPECT_TRUE(ro.success());

  // lookup /foo/bar again
  a.Clear();
  a.set_action_type(MetadataAction::LOOKUP);
  a.set_version(6);
  li.Clear();
  li.set_path("/foo/bar");
  li.mutable_permissions();
  li.SerializeToString(a.mutable_input());

  t.mds_[0]->GetRWSets(&a);
  EXPECT_EQ(3, t.RunAction(&a));

  EXPECT_TRUE(a.has_output());
  lo.Clear();
  lo.ParseFromString(a.output());
  EXPECT_TRUE(lo.success());
  EXPECT_EQ(DATA, lo.entry().type());
  EXPECT_EQ(3, lo.entry().file_parts_size());
  EXPECT_TRUE(lo.entry().file_parts(0).length() == 10);
  EXPECT_FALSE(lo.entry().file_parts(0).has_block_id());
  EXPECT_FALSE(lo.entry().file_parts(0).has_block_offset());
  EXPECT_TRUE(lo.entry().file_parts(1).length() == 15);
  EXPECT_TRUE(lo.entry().file_parts(1).block_id() == 30);
  EXPECT_TRUE(lo.entry().file_parts(1).block_offset() == 40);
  EXPECT_TRUE(lo.entry().file_parts(2).length() == 15);
  EXPECT_FALSE(lo.entry().file_parts(2).has_block_id());
  EXPECT_FALSE(lo.entry().file_parts(2).has_block_offset());
}

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  if (FLAGS_benchmark) {
    return 0;
  }
  return RUN_ALL_TESTS();
}

