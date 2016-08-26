// Author: Alexander Thomson <thomson@cs.yale.edu>
//

#include "fs/block_store.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "common/utils.h"
#include "fs/calvinfs.h"

DEFINE_bool(benchmark, false, "Run benchmarks instead of unit tests.");
DEFINE_uint64(count, 1000, "Benchmark size");

TEST(BlockStoreTest, StartsEmpty) {
  LocalFileBlockStore bs;
  string s;

  for (uint64 i = 0; i < 10000; i++) {
    EXPECT_FALSE(bs.Exists(i));
    EXPECT_FALSE(bs.Get(10, &s));
  }
}

TEST(BlockStoreTest, PutGet) {
  LocalFileBlockStore bs;
  string s;

  bs.Put(10, "asdf");
  EXPECT_FALSE(bs.Exists(0));
  EXPECT_FALSE(bs.Exists(1));
  EXPECT_FALSE(bs.Exists(11));
  EXPECT_FALSE(bs.Exists(100));
  EXPECT_TRUE(bs.Exists(10));
  EXPECT_TRUE(bs.Get(10, &s));
  EXPECT_EQ("asdf", s);
}

TEST(BlockStoreTest, Overwrite) {
  LocalFileBlockStore bs;
  string s;

  bs.Put(10, "asdf");
  bs.Put(10, "blargh");
  EXPECT_TRUE(bs.Exists(10));
  EXPECT_TRUE(bs.Get(10, &s));
  EXPECT_EQ("blargh", s);
}

TEST(DistributedBlockStore, OneMachine) {
  Machine m;
  string fsconfig;
  MakeCalvinFSConfig().SerializeToString(&fsconfig);
  m.AppData()->Put("calvinfs-config", fsconfig);
  m.AddApp("DistributedBlockStore", "blockstore");

  DistributedBlockStoreApp* dbs =
      reinterpret_cast<DistributedBlockStoreApp*>(m.GetApp("blockstore"));
  dbs->Put(10, "asdf");
  dbs->Put(10, "blargh");
  EXPECT_TRUE(dbs->Exists(10));
  string s;
  EXPECT_TRUE(dbs->Get(10, &s));
  EXPECT_EQ("blargh", s);
}

TEST(DistributedBlockStore, ThreeMachines) {
  string fsconfig;
  MakeCalvinFSConfig(3).SerializeToString(&fsconfig);

  vector<Machine*> m;
  for (int i = 0; i < 3; i++) {
    m.push_back(new Machine(i, ClusterConfig::LocalCluster(3)));
    m[i]->AppData()->Put("calvinfs-config", fsconfig);
    m[i]->AddApp("DistributedBlockStore", "blockstore");
  }

  vector<DistributedBlockStoreApp*> dbs;
  for (int i = 0; i < 3; i++) {
    dbs.push_back(
      reinterpret_cast<DistributedBlockStoreApp*>(m[i]->GetApp("blockstore")));
  }

  dbs[0]->Put(10, "asdf");
  string s;
  EXPECT_TRUE(dbs[0]->Exists(10));
  EXPECT_TRUE(dbs[1]->Exists(10));
  EXPECT_TRUE(dbs[2]->Exists(10));
  EXPECT_TRUE(dbs[0]->Get(10, &s));
  EXPECT_EQ("asdf", s);
  EXPECT_TRUE(dbs[1]->Get(10, &s));
  EXPECT_EQ("asdf", s);
  EXPECT_TRUE(dbs[2]->Get(10, &s));
  EXPECT_EQ("asdf", s);

  dbs[1]->Put(10, "blargh");
  EXPECT_TRUE(dbs[0]->Exists(10));
  EXPECT_TRUE(dbs[1]->Exists(10));
  EXPECT_TRUE(dbs[2]->Exists(10));
  EXPECT_TRUE(dbs[0]->Get(10, &s));
  EXPECT_EQ("blargh", s);
  EXPECT_TRUE(dbs[1]->Get(10, &s));
  EXPECT_EQ("blargh", s);
  EXPECT_TRUE(dbs[2]->Get(10, &s));
  EXPECT_EQ("blargh", s);

  for (uint32 i = 0; i < m.size(); i++) {
    delete m[i];
  }
}

TEST(DistributedBlockStore, ThreeMachinesMoreBlocks) {
  string fsconfig;
  MakeCalvinFSConfig(3).SerializeToString(&fsconfig);

  vector<Machine*> m;
  for (int i = 0; i < 3; i++) {
    m.push_back(new Machine(i, ClusterConfig::LocalCluster(3)));
    m[i]->AppData()->Put("calvinfs-config", fsconfig);
    m[i]->AddApp("DistributedBlockStore", "blockstore");
  }

  vector<DistributedBlockStoreApp*> dbs;
  for (int i = 0; i < 3; i++) {
    dbs.push_back(
      reinterpret_cast<DistributedBlockStoreApp*>(m[i]->GetApp("blockstore")));
  }

  // Record contents.
  vector<string> blocks(1, "");

  for (uint64 i = 1; i <= 10000; i++) {
    // Write a random string of random length.
    blocks.push_back(RandomString(rand() % 2048));

    dbs[rand() % 3]->Put(i, blocks[i]);
  }

  for (uint64 i = 1; i <= 10000; i++) {
    string s;
    EXPECT_TRUE(dbs[rand() % 3]->Exists(i));
    EXPECT_TRUE(dbs[rand() % 3]->Get(i, &s));
    EXPECT_EQ(blocks[i], s);
  }
  EXPECT_FALSE(dbs[0]->Exists(10001));

  for (uint32 i = 0; i < m.size(); i++) {
    delete m[i];
  }
}

void Benchmark() {
  string data;
  for (int i = 0; i < 1024; i++) {
    data.append(1, '0' + rand() % 10);
  }

  LocalFileBlockStore bs;
  double start = GetTime();
  for (uint64 i = 0; i < FLAGS_count; i++) {
    bs.Put(i, data);
    data[rand() % 1024] = '0' + rand() % 10;
  }
  double stop = GetTime();

  LOG(ERROR) << "Put " << FLAGS_count << ": " << (stop - start) * 1000 << " ms";

  start = GetTime();
  for (uint64 i = 0; i < FLAGS_count; i++) {
    bs.Get(i, &data);
  }
  stop = GetTime();

  LOG(ERROR) << "Get " << FLAGS_count << ": " << (stop - start) * 1000 << " ms";
}

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  if (FLAGS_benchmark) {
    Benchmark();
    return 0;
  } else {
    return RUN_ALL_TESTS();
  }
}

