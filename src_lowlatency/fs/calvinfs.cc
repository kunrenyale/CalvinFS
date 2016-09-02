// Author: Alexander Thomson <thomson@cs.yale.edu>
//

#include "fs/calvinfs.h"

#include <map>
#include "common/utils.h"
#include "components/log/log_app.h"
#include "components/scheduler/scheduler.h"
#include "components/scheduler/serial_scheduler.h"
#include "components/store/btreestore.h"
#include "components/store/kvstore.h"
#include "components/store/versioned_kvstore.h"
#include "fs/block_store.h"
#include "fs/calvinfs_config.pb.h"
#include "fs/metadata.pb.h"
#include "fs/metadata_store.h"
#include "machine/cluster_config.h"
#include "machine/machine.h"

using std::map;
using std::pair;
using std::make_pair;

CalvinFSConfigMap::CalvinFSConfigMap(const CalvinFSConfig& config) {
  Init(config);
}

CalvinFSConfigMap::CalvinFSConfigMap(Machine* machine) {
  string s;
  CHECK(machine->AppData()->Lookup("calvinfs-config", &s)) << "no config found";
  CalvinFSConfig config;
  config.ParseFromString(s);
  Init(config);
}

uint64 CalvinFSConfigMap::LookupReplica(uint64 machine_id) {
  CHECK(replicas_.count(machine_id) != 0) << "unknown machine: " << machine_id;
  return replicas_[machine_id];
}

uint64 CalvinFSConfigMap::HashBlockID(uint64 block_id) {
  return FNVHash(UInt64ToString(33 * block_id)) % config_.blucket_count();
}

uint64 CalvinFSConfigMap::HashFileName(const Slice& filename) {
  return FNVHash(filename) % config_.metadata_shard_count();
}

uint64 CalvinFSConfigMap::LookupBlucket(uint64 id, uint64 replica) {
  auto it = bluckets_.find(make_pair(id, replica));
  if (it == bluckets_.end()) {
    LOG(FATAL) << "nonexistant blucket (" << id << ", " << replica << ")";
  }
  return it->second;
}

uint64 CalvinFSConfigMap::LookupMetadataShard(uint64 id, uint64 replica) {
  auto it = metadata_shards_.find(make_pair(id, replica));
  if (it == metadata_shards_.end()) {
    LOG(FATAL) << "nonexistant md shard (" << id << ", " << replica << ")";
  }
  return it->second;
}

uint64 CalvinFSConfigMap::GetPartitionsPerReplica () {
  return config_.metadata_shard_count();
}

void CalvinFSConfigMap::Init(const CalvinFSConfig& config) {
  config_.CopyFrom(config);

  for (int i = 0; i < config.replicas_size(); i++) {
    replicas_[config.replicas(i).machine()] = config.replicas(i).replica();
  }

  for (int i = 0; i < config.bluckets_size(); i++) {
    bluckets_[make_pair(config.bluckets(i).id(),
                        config.bluckets(i).replica())] =
        config.bluckets(i).machine();
  }

  for (int i = 0; i < config.metadata_shards_size(); i++) {
    metadata_shards_[make_pair(config.metadata_shards(i).id(),
                               config.metadata_shards(i).replica())] =
        config.metadata_shards(i).machine();
  }

  // Init replica_schema_
  replica_schema_["/a"] = 0;
  replica_schema_["/b"] = 0;
  replica_schema_["/c"] = 0; 
}

uint32 CalvinFSConfigMap::LookupReplicaByDir(string dir) {
  CHECK(replica_schema_.count(dir) > 0);
  return replica_schema_[dir];
}

////////////////////////////////////////////////////////////////////////////////

CalvinFSConfig MakeCalvinFSConfig() {
  CalvinFSConfig c;
  c.set_block_replication_factor(1);
  c.set_metadata_replication_factor(1);
  c.set_blucket_count(1);
  c.set_metadata_shard_count(1);

  c.add_replicas()->set_machine(0);
  c.mutable_replicas(0)->set_replica(0);
  c.add_bluckets()->set_id(0);
  c.add_metadata_shards()->set_id(0);
  return c;
}

CalvinFSConfig MakeCalvinFSConfig(int n) {
  CalvinFSConfig c;
  c.set_block_replication_factor(1);
  c.set_metadata_replication_factor(1);
  c.set_blucket_count(n);
  c.set_metadata_shard_count(n);

  for (int i = 0; i < n; i++) {
    c.add_replicas()->set_machine(i);
    c.mutable_replicas(i)->set_replica(0);
    c.add_bluckets()->set_id(i);
    c.mutable_bluckets(i)->set_machine(i);
    c.add_metadata_shards()->set_id(i);
    c.mutable_metadata_shards(i)->set_machine(i);
  }
  return c;
}

CalvinFSConfig MakeCalvinFSConfig(int n, int r) {
  CalvinFSConfig c;
  c.set_block_replication_factor(r);
  c.set_metadata_replication_factor(r);
  c.set_blucket_count(n);
  c.set_metadata_shard_count(n);

  for (int i = 0; i < r; i++) {
    for (int j = 0; j < n; j++) {
      int m = i*n+j;  // machine id
      c.add_replicas()->set_machine(m);
      c.mutable_replicas(m)->set_replica(i);
      c.add_bluckets()->set_id(j);
      c.mutable_bluckets(m)->set_replica(i);
      c.mutable_bluckets(m)->set_machine(m);
      c.add_metadata_shards()->set_id(j);
      c.mutable_metadata_shards(m)->set_replica(i);
      c.mutable_metadata_shards(m)->set_machine(m);
    }
  }
  return c;
}

////////////////////////////////////////////////////////////////////////////////

LocalCalvinFS::LocalCalvinFS()
    : machine_(new Machine(0, ClusterConfig::LocalCluster(1))),
      blocks_(new HybridBlockStore()) {
  // Save basic config info.
  string fsconfig;
  MakeCalvinFSConfig().SerializeToString(&fsconfig);
  machine_->AppData()->Put("calvinfs-config", fsconfig);

  // Start metadata store app.
  StartAppProto sap;
  sap.add_participants(0);
  sap.set_app("MetadataStoreApp");
  sap.set_app_name("metadata");
  machine_->AddApp(sap);
  metadata_ = reinterpret_cast<StoreApp*>(machine_->GetApp("metadata"));
  reinterpret_cast<MetadataStore*>(metadata_->store())->SetMachine(machine_);

  // Start scheduler app.
  sap.set_app("SerialScheduler");
  sap.set_app_name("scheduler");
  machine_->AddApp(sap);
  scheduler_ = reinterpret_cast<Scheduler*>(machine_->GetApp("scheduler"));

  // Bind scheduler to store.
  scheduler_->SetStore("metadata");

  // Start log app.
  sap.set_app("LogApp");
  sap.set_app_name("log");
  machine_->AddApp(sap);
  log_ = reinterpret_cast<LogApp*>(machine_->GetApp("log"));

  // Connect log to scheduler.
  source_ = new RemoteLogSource<Action>(machine_, 0, "log");
  scheduler_->SetActionSource(source_);

  // Get results queue.
  results_ = machine_->DataChannel("action-results");
}

LocalCalvinFS::~LocalCalvinFS() {
  delete blocks_;
  delete machine_;
  delete source_;
}

// NOTE: This is a snapshot (not linearizable) read.
//
// TODO(agt): Avoid rereading blocks that appear multiple times in a file?
Status LocalCalvinFS::ReadFileToString(const string& path, string* data) {
  data->clear();

  // Lookup MetadataEntry.
  Action a;
  a.set_version(scheduler_->SafeVersion());
  a.set_action_type(MetadataAction::LOOKUP);
  MetadataAction::LookupInput in;
  in.set_path(path);
  in.SerializeToString(a.mutable_input());
  metadata_->GetRWSets(&a);
  metadata_->Run(&a);
  MetadataAction::LookupOutput out;
  out.ParseFromString(a.output());
  if (!out.success()) {
    return Status::Error("metadata lookup error");
  }
  if (out.entry().type() != DATA) {
    return Status::Error("wrong file type");
  }

  // Get blocks.
  for (int i = 0; i < out.entry().file_parts_size(); i++) {
    if (out.entry().file_parts(i).block_id() == 0) {
      // Implicit all-zero block!
      data->append(out.entry().file_parts(i).length(), '\0');
    } else {
      // Block from block store.
      string block;
      if (!blocks_->Get(out.entry().file_parts(i).block_id(), &block)) {
        return Status::Error("block lookup error");
      }
      data->append(
          block,
          out.entry().file_parts(i).block_offset(),
          out.entry().file_parts(i).length());
    }
  }

  return Status::OK();
}

Status LocalCalvinFS::CreateDirectory(const string& path) {
  Action a;
  a.set_client_machine(0);
  a.set_client_channel("action-results");
  a.set_action_type(MetadataAction::CREATE_FILE);
  MetadataAction::CreateFileInput in;
  in.set_path(path);
  in.set_type(DIR);
  in.SerializeToString(a.mutable_input());
  metadata_->GetRWSets(&a);
  string as;
  a.SerializeToString(&as);
  log_->Append(as);

  MessageBuffer* m = NULL;
  while (!results_->Pop(&m)) {
    // Wait for action to complete and be sent back.
  }

  a.ParseFromArray((*m)[0].data(), (*m)[0].size());
  delete m;

  // Success?
  MetadataAction::CreateFileOutput out;
  out.ParseFromString(a.output());
  return out.success() ? Status::OK() : Status::Error("failed to create dir");
}

Status LocalCalvinFS::CreateFile(const string& path) {
  Action a;
  a.set_client_machine(0);
  a.set_client_channel("action-results");
  a.set_action_type(MetadataAction::CREATE_FILE);
  MetadataAction::CreateFileInput in;
  in.set_path(path);
  in.set_type(DATA);
  in.SerializeToString(a.mutable_input());
  metadata_->GetRWSets(&a);
  string as;
  a.SerializeToString(&as);
  log_->Append(as);

  MessageBuffer* m = NULL;
  while (!results_->Pop(&m)) {
    // Wait for action to complete and be sent back.
  }

  a.ParseFromArray((*m)[0].data(), (*m)[0].size());
  delete m;


  // Success?
  MetadataAction::CreateFileOutput out;
  out.ParseFromString(a.output());
  return out.success() ? Status::OK() : Status::Error("failed to create file");
}

// TODO(agt): Break up large writes into multiple blocks?
Status LocalCalvinFS::AppendStringToFile(const string& data, const string& path) {
  // Write data block.
  uint64 block_id = machine_->GetGUID() * 2 + (data.size() > 1024 ? 1 : 0);
  blocks_->Put(block_id, data);

  // Update metadata.
  Action a;
  a.set_client_machine(0);
  a.set_client_channel("action-results");
  a.set_action_type(MetadataAction::APPEND);
  MetadataAction::AppendInput in;
  in.set_path(path);
  in.add_data();
  in.mutable_data(0)->set_length(data.size());
  in.mutable_data(0)->set_block_id(block_id);
  in.SerializeToString(a.mutable_input());
  metadata_->GetRWSets(&a);
  string as;
  a.SerializeToString(&as);
  log_->Append(as);

  MessageBuffer* m = NULL;
  while (!results_->Pop(&m)) {
    // Wait for action to complete and be sent back.
  }

  a.ParseFromArray((*m)[0].data(), (*m)[0].size());
  delete m;

  // Success?
  MetadataAction::AppendOutput out;
  out.ParseFromString(a.output());
  return out.success() ? Status::OK() : Status::Error("append failed");
}

// NOTE: Snapshot (not linearizable) read.
Status LocalCalvinFS::LS(const string& path, vector<string>* contents) {
  contents->clear();

  // Lookup MetadataEntry.
  Action a;
  a.set_version(scheduler_->SafeVersion());
  a.set_action_type(MetadataAction::LOOKUP);
  MetadataAction::LookupInput in;
  in.set_path(path);
  in.SerializeToString(a.mutable_input());
  metadata_->GetRWSets(&a);
  metadata_->Run(&a);

  MetadataAction::LookupOutput out;
  out.ParseFromString(a.output());
  if (!out.success()) {
    return Status::Error("metadata lookup error");
  }
  if (out.entry().type() != DIR) {
    return Status::Error("wrong file type");
  }

  // Read dir contents.
  for (int i = 0; i < out.entry().dir_contents_size(); i++) {
    contents->push_back(out.entry().dir_contents(i));
  }

  return Status::OK();
}

Status LocalCalvinFS::Remove(const string& path) {
  return Status::Error("not implemented");
}

Status LocalCalvinFS::Copy(const string& from_path, const string& to_path) {
  return Status::Error("not implemented");
}

Status LocalCalvinFS::WriteStringToFile(const string& data, const string& path) {
  return Status::Error("not implemented");
}

