// Author: Alexander Thomson <thomson@cs.yale.edu>
//         Kun  Ren <kun.ren@yale.edu>
//

#include "fs/block_store.h"

#include <glog/logging.h>
#include <leveldb/db.h>
#include <leveldb/env.h>
#include <leveldb/slice.h>
#include <string>
#include <vector>
#include "common/types.h"
#include "common/utils.h"
#include "machine/app/app.h"
#include "fs/calvinfs.h"

using leveldb::Env;
using leveldb::Slice;
using leveldb::ReadFileToString;
using leveldb::WriteStringToFile;
using std::string;
using std::vector;

REGISTER_APP(BlockStore) {
  return new BlockStoreApp(new HybridBlockStore());
}

REGISTER_APP(DistributedBlockStore) {
  return new DistributedBlockStoreApp(new HybridBlockStore());
}

/////////////////////////     LocalFileBlockStore     /////////////////////////

static const uint32 kDirCount = 1000;

void EncodeBlockID(uint64 block_id, string* path) {
  uint32 dir =
      FNVHash(UInt64ToString(block_id)) % kDirCount;
  path->resize(path->size() + 22);
  snprintf(const_cast<char*>(path->data()) + path->size() - 22, 22,
           "/%03u/%016lX", dir, block_id);
  path->resize(path->size() - 1);  // Remove trailing null character.
}

LocalFileBlockStore::LocalFileBlockStore() {
  leveldb::Env::Default()->GetTestDirectory(&path_prefix_);
  path_prefix_.append("/localfile-blocks");
  vector<string> children;
  if (!leveldb::Env::Default()->GetChildren(path_prefix_, &children).ok()) {
    CHECK(leveldb::Env::Default()->CreateDir(path_prefix_).ok());
  }
  CHECK(leveldb::Env::Default()->GetChildren(path_prefix_, &children).ok());
  if (!children.empty()) {
    CHECK_EQ(0, system((string("rm -fr ") + path_prefix_ + "/*").c_str()));
  }
  for (uint32 i = 0; i < kDirCount; i++) {
    string dir(path_prefix_);
    dir.resize(dir.size() + 5);
    snprintf(const_cast<char*>(dir.data()) + dir.size() - 5, 5, "/%03u", i);
    dir.resize(dir.size() - 1);  // Remove trailing null character.
    CHECK(leveldb::Env::Default()->CreateDir(dir).ok());
  }
}

bool LocalFileBlockStore::Exists(uint64 block_id) {
  string path(path_prefix_);
  EncodeBlockID(block_id, &path);
  return Env::Default()->FileExists(path);
}

void LocalFileBlockStore::Put(uint64 block_id, const Slice& data) {
  string path(path_prefix_);
  EncodeBlockID(block_id, &path);
  leveldb::Status s = WriteStringToFile(Env::Default(), data, path);
  CHECK(s.ok());
}

bool LocalFileBlockStore::Get(uint64 block_id, string* data) {
  string path(path_prefix_);
  EncodeBlockID(block_id, &path);
  leveldb::Status s = ReadFileToString(Env::Default(), path, data);
  return s.ok();
}

//////////////////////////     LevelDBBlockStore     //////////////////////////

LevelDBBlockStore::LevelDBBlockStore() {
  string path;
  leveldb::Env::Default()->GetTestDirectory(&path);
  path.append("/leveldb-blocks-");
  static atomic<int> guid(0);
  path.append(IntToString(guid++));

  // Destroy any previous instantiation of the database.
  leveldb::DestroyDB(path, leveldb::Options());

  // Instantiate a new database.
  leveldb::Options options;
  options.create_if_missing = true;
  options.error_if_exists = true;

  if (!leveldb::DB::Open(options, path, &blocks_).ok()) {
    LOG(ERROR) << "Error opening LevelDB database.";
  }
}

LevelDBBlockStore::~LevelDBBlockStore() {
  delete blocks_;
}

bool LevelDBBlockStore::Exists(uint64 block_id) {
  string s;
  return Get(block_id, &s);
}

void LevelDBBlockStore::Put(uint64 block_id, const Slice& data) {
  CHECK(blocks_->Put(
      leveldb::WriteOptions(),
      UInt64ToString(block_id),
      data).ok());
}

bool LevelDBBlockStore::Get(uint64 block_id, string* data) {
  return blocks_->Get(
      leveldb::ReadOptions(),
      UInt64ToString(block_id),
      data).ok();
}

///////////////////////////     HybridBlockStore     ///////////////////////////

bool HybridBlockStore::Exists(uint64 block_id) {
  if (block_id % 2 == 0) {
    return small_blocks_.Exists(block_id);
  } else {
    return large_blocks_.Exists(block_id);
  }
}

void HybridBlockStore::Put(uint64 block_id, const Slice& data) {
  if (block_id % 2 == 0) {
    small_blocks_.Put(block_id, data);
  } else {
    large_blocks_.Put(block_id, data);
  }
}

bool HybridBlockStore::Get(uint64 block_id, string* data) {
  if (block_id % 2 == 0) {
    return small_blocks_.Get(block_id, data);
  } else {
    return large_blocks_.Get(block_id, data);
  }
}

////////////////////////////     BlockStoreApp     ////////////////////////////

BlockStoreApp::~BlockStoreApp() {
  delete blocks_;
}

bool BlockStoreApp::Exists(uint64 block_id) {
  return blocks_->Exists(block_id);
}

void BlockStoreApp::Put(uint64 block_id, const Slice& data) {
  blocks_->Put(block_id, data);
}

bool BlockStoreApp::Get(uint64 block_id, string* data) {
  return blocks_->Get(block_id, data);
}

void BlockStoreApp::HandleMessage(Header* header, MessageBuffer* message) {
  LOG(FATAL) << "RPC request sent to BlockStoreApp";
}

///////////////////////     DistributedBlockStoreApp     ///////////////////////

DistributedBlockStoreApp::DistributedBlockStoreApp(BlockStore* blocks) {
  blocks_ = blocks;
}

bool DistributedBlockStoreApp::Exists(uint64 block_id) {
  if (IsLocal(block_id)) {
    return blocks_->Exists(block_id);
  }

  // Nonlocal file. Send request to the that owns it (for this replica).
  Header* header = new Header();
  header->set_from(machine()->machine_id());
  header->set_to(
      config_->LookupBlucket(config_->HashBlockID(block_id), replica_));
  header->set_type(Header::RPC);
  header->set_app(name());
  header->set_rpc("EXISTS");
  header->add_misc_int(block_id);
  MessageBuffer* m = NULL;
  header->set_data_ptr(reinterpret_cast<uint64>(&m));
  machine()->SendMessage(header, new MessageBuffer());

  // Wait for response.
  SpinUntilNE<MessageBuffer*>(m, NULL);

  bool result = !m->empty();
  delete m;
  return result;
}

void DistributedBlockStoreApp::Put(uint64 block_id, const Slice& data) {
  atomic<int>* acks = new atomic<int>(0);
  for (uint32 i = 0; i < config_->config().block_replication_factor(); i++) {
    Header* header = new Header();
    header->set_from(machine()->machine_id());
    header->set_to(config_->LookupBlucket(config_->HashBlockID(block_id), i));
    header->set_type(Header::RPC);
    header->set_app(name());
    header->set_rpc("PUT");
    header->add_misc_int(block_id);
    header->set_ack_counter(reinterpret_cast<uint64>(acks));
    machine()->SendMessage(header, new MessageBuffer(data));
  }

  // Only need to wait for ack from the local data center.
  while (acks->load() < 1) {
    usleep(10);
  }
}

bool DistributedBlockStoreApp::Get(uint64 block_id, string* data) {
  if (IsLocal(block_id)) {
    return blocks_->Get(block_id, data);
  }

  // Nonlocal file. Send request to the that owns it (for this replica).
  Header* header = new Header();
  header->set_from(machine()->machine_id());
  header->set_to(
      config_->LookupBlucket(config_->HashBlockID(block_id), replica_));
  header->set_type(Header::RPC);
  header->set_app(name());
  header->set_rpc("GET");
  header->add_misc_int(block_id);
  MessageBuffer* m = NULL;
  header->set_data_ptr(reinterpret_cast<uint64>(&m));
  machine()->SendMessage(header, new MessageBuffer());

  // Wait for response.
  SpinUntilNE<MessageBuffer*>(m, NULL);

  bool found = !m->empty();
  if (found) {
    data->assign((*m)[0].data(), (*m)[0].size());
  }
  delete m;
  return found;
}

void DistributedBlockStoreApp::HandleMessage(
    Header* header,
    MessageBuffer* message) {

  // Get request's block id.
  uint64 block_id = header->misc_int(0);
  CHECK(IsLocal(block_id)) << "RPC request for non-local block";

  if (header->rpc() == "EXISTS") {
    CHECK(message->empty());
    if (blocks_->Exists(block_id)) {
      message->Append(new string("e"));
    }
    machine()->SendReplyMessage(header, message);

  } else if (header->rpc() == "GET") {
    CHECK(message->empty());
    string* data = new string();
    if (blocks_->Get(block_id, data)) {
      message->Append(data);
    } else {
      delete data;
    }
    machine()->SendReplyMessage(header, message);

  } else if (header->rpc() == "PUT") {
    blocks_->Put(block_id, (*message)[0]);
    message->clear();
    machine()->SendReplyMessage(header, message);

  } else {
    LOG(FATAL) << "unrecognized RPC type: " << header->rpc();
  }
}

void DistributedBlockStoreApp::Start() {
  config_ = new CalvinFSConfigMap(machine());
  replica_ = config_->LookupReplica(machine()->machine_id());
}

bool DistributedBlockStoreApp::IsLocal(uint64 block_id) {
  return config_->LookupMetadataShard(config_->HashBlockID(block_id), replica_)
         == machine()->machine_id();
}

