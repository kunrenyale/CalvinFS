// Author: Kun  Ren (kun.ren@yale.edu)
//

#include "fs/metadata_store.h"

#include <glog/logging.h>
#include <map>
#include <set>
#include <string>
#include "btree/btree_map.h"
#include "common/utils.h"
#include "components/store/store_app.h"
#include "components/store/versioned_kvstore.pb.h"
#include "components/store/btreestore.h"
#include "machine/app/app.h"
#include "machine/machine.h"
#include "machine/message_buffer.h"
#include "fs/calvinfs.h"
#include "fs/metadata.pb.h"
#include "machine/app/app.h"
#include "proto/action.pb.h"

using std::map;
using std::set;
using std::string;
using std::pair;
using std::make_pair;

REGISTER_APP(MetadataStoreApp) {
  return new StoreApp(new MetadataStore(new BTreeStore()));
}


string ParentDir(const string& path) {
  // Root dir is a special case.
  if (path.empty()) {
    LOG(FATAL) << "root dir has no parent";
  }
  std::size_t offset = path.rfind('/');
  CHECK_NE(string::npos, offset);     // at least 1 slash required
  CHECK_NE(path.size() - 1, offset);  // filename cannot be empty
  return string(path, 0, offset);
}

string FileName(const string& path) {
  // Root dir is a special case.
  if (path.empty()) {
    return path;
  }
  uint32 offset = path.rfind('/');
  CHECK_NE(string::npos, offset);     // at least 1 slash required
  CHECK_NE(path.size() - 1, offset);  // filename cannot be empty
  return string(path, offset + 1);
}


///////////////////////        ExecutionContext        ////////////////////////
//
// TODO(agt): The implementation below is a LOCAL execution context.
//            Extend this to be a DISTRIBUTED one.
// TODO(agt): Generalize and move to components/store/store.{h,cc}.
//
class ExecutionContext {
 public:
  // Constructor performs all reads.
  ExecutionContext(KVStore* store, Action* action)
      : store_(store), version_(action->version()), aborted_(false) {
    for (int i = 0; i < action->readset_size(); i++) {
      if (!store_->Get(action->readset(i), &reads_[action->readset(i)])) {
        reads_.erase(action->readset(i));
      }
    }

    if (action->readset_size() > 0) {
      reader_ = true;
    } else {
      reader_ = false;
    }

    if (action->writeset_size() > 0) {
      writer_ = true;
    } else {
      writer_ = false;
    }
  }

  // Destructor installs all writes.
  ~ExecutionContext() {
    if (!aborted_) {
      for (auto it = writes_.begin(); it != writes_.end(); ++it) {
        store_->Put(it->first, it->second);
      }
      for (auto it = deletions_.begin(); it != deletions_.end(); ++it) {
        store_->Delete(*it);
      }
    }
  }

  bool EntryExists(const string& path) {
    return reads_.count(path) != 0;
  }

  bool GetEntry(const string& path, MetadataEntry* entry) {
    entry->Clear();
    if (reads_.count(path) != 0) {
      entry->ParseFromString(reads_[path]);
      return true;
    }
    return false;
  }

  void PutEntry(const string& path, const MetadataEntry& entry) {
    deletions_.erase(path);
    entry.SerializeToString(&writes_[path]);
    if (reads_.count(path) != 0) {
      entry.SerializeToString(&reads_[path]);
    }
  }

  void DeleteEntry(const string& path) {
    reads_.erase(path);
    writes_.erase(path);
    deletions_.insert(path);
  }

  bool Abort() {
    return aborted_;
  }

  bool IsWriter() {
    if (writer_)
      return true;
    else
      return false;
  }

 protected:
  ExecutionContext() {}
  KVStore* store_;
  uint64 version_;
  bool aborted_;
  map<string, string> reads_;
  map<string, string> writes_;
  set<string> deletions_;

  // True iff any reads are at this partition.
  bool reader_;

  // True iff any writes are at this partition.
  bool writer_;

};

////////////////////      DistributedExecutionContext      /////////////////////
//
//  Generalize and move to components/store/store.{h,cc}?

class DistributedExecutionContext : public ExecutionContext {
 public:
  // Constructor performs all reads.
  DistributedExecutionContext(
      Machine* machine,
      CalvinFSConfigMap* config,
      KVStore* store,
      Action* action)
        : machine_(machine), config_(config) {
    // Initialize parent class variables.
    store_ = store;
    version_ = action->version();
    aborted_ = false;
    origin_ = action->origin();

    data_channel_version = action->distinct_id();

    // Look up what replica we're at.
    replica_ = config_->LookupReplica(machine_->machine_id());

    machine_id_ = machine_->machine_id();

LOG(ERROR) << "Machine: "<<machine_id_<< "  DistributedExecutionContext received a txn:: data_channel_version:"<<data_channel_version;
   
    // Handle remaster action
    if (action->remaster() == true) {
      writer_ = true;

      // Forward_remaster: all entries that need to remaster, replica_id=>keys
      map<uint32, set<string>> forward_remaster;
      set<string> local_keys;
      bool remote_remaster = false;

      for (int i = 0; i < action->remastered_keys_size(); i++) {
        // Local read.
        if (!store_->Get(action->remastered_keys(i), &reads_[action->remastered_keys(i)])) {
          reads_.erase(action->remastered_keys(i));
        }

        MetadataEntry entry;
        GetEntry(action->remastered_keys(i), &entry);
        if (entry.master() != action->remaster_from()) {
          remote_remaster = true;
          forward_remaster[entry.master()].insert(action->remastered_keys(i));
        } else {
          local_keys.insert(action->remastered_keys(i));
        }     
      }


      if (remote_remaster == true) {
        // Send the remaster actions(generate a new action) to the involved replicas;
        Action* remaster_action = new Action();
        remaster_action->CopyFrom(*action);

        for (auto it = forward_remaster.begin(); it != forward_remaster.end(); it++) {
          uint32 remote_replica = it->first;
          set<string> remote_keys = it->second;
         
          // Just ignore this action because the previous remaster action already did so
          if (remote_replica == action->remaster_to()) {
            continue;          
          }
          
          // ignore the keys in this remaster action, and generate a new remaster action
          remaster_action->set_remaster_from(remote_replica);
          remaster_action->clear_remastered_keys();
          remaster_action->clear_distinct_id();
          remaster_action->set_distinct_id(machine_->GetGUID());
LOG(ERROR) << "Machine: "<<machine_id_<< "  DistributedExecutionContext received a remaster txn(will forward another remaster):: data_channel_version:"<<data_channel_version<<"  remaster id:"<<remaster_action->distinct_id();          
          for (auto it = remote_keys.begin(); it != remote_keys.end(); it++) {
            remaster_action->add_remastered_keys(*it);
          }

          Header* header = new Header();
          header->set_from(machine_id_);
          header->set_to(config_->LookupMetadataShard(config_->GetMdsFromMachine(machine_id_), remote_replica));
          header->set_type(Header::RPC);
          header->set_app("blocklog");
          header->set_rpc("APPEND");
          string* block = new string();
          remaster_action->SerializeToString(block);
          machine_->SendMessage(header, new MessageBuffer(Slice(*block)));
        }
        
        // If no local keys need to be remastered, then we can simply abort this action
        if (local_keys.size() == 0) {
          aborted_ = true;
LOG(ERROR) << "Machine: "<<machine_id_<< "  DistributedExecutionContext received a remaster txn(will abort):: data_channel_version:"<<data_channel_version; 
          return;
        }
      }
LOG(ERROR) << "Machine: "<<machine_id_<< "  DistributedExecutionContext received a remaster txn(will run):: data_channel_version:"<<data_channel_version;           
    } else {
    
      set<uint64> remote_nonmin_machines;
      // Figure out what machines are readers (and perform local reads).
      reader_ = false;
      set<uint64> remote_readers;

      KeyMasterEntries local_entries;

      for (int i = 0; i < action->readset_size(); i++) {
        uint64 mds = config_->HashFileName(action->readset(i));
        uint64 machine = config_->LookupMetadataShard(mds, replica_);
        if ((machine == machine_id_)) {
          // Local read.
          if (!store_->Get(action->readset(i), &reads_[action->readset(i)])) {
            reads_.erase(action->readset(i));
          }
          reader_ = true;
          
          MetadataEntry entry;
          GetEntry(action->readset(i), &entry);

          KeyMasterEntry* e = local_entries.add_entries();
          e->set_key(action->readset(i));
          e->set_master(entry.master());

        } else {
          remote_readers.insert(machine);
          remote_nonmin_machines.insert(machine);
        }
      }

      // Figure out what machines are writers.
      writer_ = false;
      set<uint64> remote_writers;
    
      for (int i = 0; i < action->writeset_size(); i++) {
        uint64 mds = config_->HashFileName(action->writeset(i));
        uint64 machine = config_->LookupMetadataShard(mds, replica_);
        if ((machine == machine_id_)) {
          writer_ = true;
        } else {
          remote_writers.insert(machine);
          remote_nonmin_machines.insert(machine);
        }
      }
      
      uint64 min_machine_id;
      if (reader_ == false) {
        min_machine_id = *(remote_readers.begin());
LOG(ERROR) << "Machine: "<<machine_id_<< "  DistributedExecutionContext received a txn(reader_ == true), distinct id: "<<action->distinct_id();  
      }  else {
        min_machine_id = machine_id_; 
        if (remote_readers.size() > 0 && *(remote_readers.begin()) < min_machine_id) {
          min_machine_id = *(remote_readers.begin());
        }
      }

      if (min_machine_id != machine_id_) {
        // Send local key/master to the min_machine_id
        Header* header = new Header();
        header->set_from(machine_id_);
        header->set_to(min_machine_id);
        header->set_type(Header::DATA);
        header->set_data_channel("action-check" + UInt64ToString(data_channel_version));
        MessageBuffer* m = new MessageBuffer(local_entries);
        m->Append(ToScalar<uint64>(machine_id_));
        machine_->SendMessage(header, m);
LOG(ERROR) << "Machine: "<<machine_id_<< "  not min_machine, wait for receiving the decision:: data_channel_version:"<<data_channel_version;   
        // Wait for the final decision
        AtomicQueue<MessageBuffer*>* channel = machine_->DataChannel("action-check-ack" + UInt64ToString(data_channel_version));
        // Get results.
        while (!channel->Pop(&m)) {
          usleep(10);
        }

        Scalar s;
        s.ParseFromArray((*m)[0].data(), (*m)[0].size());
        bool abort_decision = FromScalar<bool>(s);

        if (abort_decision == true) {
          aborted_ = true;
LOG(ERROR) << "Machine: "<<machine_id_<< "  not min_machine, got the decision(will abort):: data_channel_version:"<<data_channel_version;   
          return;
        }
LOG(ERROR) << "Machine: "<<machine_id_<< "  not min_machine, got the decision(will execute):: data_channel_version:"<<data_channel_version; 
      } else {
        // min_machine_id
        action->clear_keys_origins();
        set<uint32> involved_replicas;

        // machine_replicas: machine_id=>all involved replicas on that machine
        map<uint64, set<uint32>> machine_replicas;

        // Collect local keys/masters
        for (int j = 0; j < local_entries.entries_size(); j++) {
          action->add_keys_origins()->CopyFrom(local_entries.entries(j));
          uint32 key_replica = local_entries.entries(j).master();
          machine_replicas[machine_id_].insert(key_replica);
          involved_replicas.insert(key_replica);
          if (key_replica != origin_) {
            aborted_ = true;
          }
        }
LOG(ERROR) << "Machine: "<<machine_id_<< "  min_machine, wait for receiving the data from all other machines:: data_channel_version:"<<data_channel_version;  
        // Wait to receive remote keys/masters
        AtomicQueue<MessageBuffer*>* channel = machine_->DataChannel("action-check" + UInt64ToString(data_channel_version));
        for (uint32 i = 0; i < remote_nonmin_machines.size(); i++) {
          MessageBuffer* m = NULL;
          // Get results.
          while (!channel->Pop(&m)) {
            usleep(10);
          }

          KeyMasterEntries remote_entries;
          remote_entries.ParseFromArray((*m)[0].data(), (*m)[0].size());
          Scalar s;
          s.ParseFromArray((*m)[1].data(), (*m)[1].size());
          uint64 remote_machine_id = FromScalar<uint64>(s);
LOG(ERROR) << "Machine: "<<machine_id_<< "  min_machine, got data from one remote machine:: data_channel_version:"<<data_channel_version<<" from machine:"<<remote_machine_id; 
          for (int j = 0; j < remote_entries.entries_size(); j++) {
            action->add_keys_origins()->CopyFrom(remote_entries.entries(j));
            uint32 key_replica = remote_entries.entries(j).master();
            machine_replicas[remote_machine_id].insert(key_replica);
            involved_replicas.insert(key_replica);
            if (key_replica != origin_) {
              aborted_ = true;
            }
          }      
        }

        // Send the final decision to all involved machines
        for (auto it = remote_nonmin_machines.begin(); it != remote_nonmin_machines.end(); ++it) {
LOG(ERROR) << "Machine: "<<machine_id_<< "  min_machine, send decision to machine:"<<(*it)<<" :: data_channel_version:"<<data_channel_version;
          Header* header = new Header();
          header->set_from(machine_id_);
          header->set_to(*it);
          header->set_type(Header::DATA);
          header->set_data_channel("action-check-ack" + UInt64ToString(data_channel_version));
          MessageBuffer* m = new MessageBuffer();
          m->Append(ToScalar<bool>(aborted_));
          machine_->SendMessage(header, m); 
        }
LOG(ERROR) << "Machine: "<<machine_id_<< "  min_machine, got all the data from all other machines:: data_channel_version:"<<data_channel_version<<" *** abort decision is:"<<aborted_;
        // Send the action to the new replica
        if (aborted_ == true && replica_ == origin_) {
LOG(ERROR) << "Machine: "<<machine_id_<< "  DistributedExecutionContext received a txn:: data_channel_version:"<<data_channel_version<<"-- abort this action, will forward this action";
          if (involved_replicas.size() == 1) {
            action->set_single_replica(true);
          } else {
            action->set_single_replica(false);
          }

          uint32 lowest_replica = *(involved_replicas.begin());

          uint32 machine_sent = config_->LookupMetadataShard(config_->GetMdsFromMachine(min_machine_id), lowest_replica);
          Header* header = new Header();
          header->set_from(machine_id_);
          header->set_to(machine_sent);
          header->set_type(Header::RPC);
          header->set_app("blocklog");
          header->set_rpc("APPEND");
          string* block = new string();
          action->SerializeToString(block);
          machine_->SendMessage(header, new MessageBuffer(Slice(*block)));
LOG(ERROR) << "Machine: "<<machine_id_<< "  DistributedExecutionContext received a txn:: data_channel_version:"<<data_channel_version<<"-- abort this action, and forward this action to"<<machine_sent;
  
          return;
        }
LOG(ERROR) << "Machine: "<<machine_id_<< "  DistributedExecutionContext received a txn:: data_channel_version:"<<data_channel_version<<"-- will continue running this action. min machine is:"<<min_machine_id;   
      }
      
    
      // If any reads were performed locally, broadcast them to writers.
      if (reader_) {
        MapProto local_reads;
        for (auto it = reads_.begin(); it != reads_.end(); ++it) {
          MapProto::Entry* e = local_reads.add_entries();
          e->set_key(it->first);
          e->set_value(it->second);
        }
        for (auto it = remote_writers.begin(); it != remote_writers.end(); ++it) {
          Header* header = new Header();
          header->set_from(machine_id_);
          header->set_to(*it);
          header->set_type(Header::DATA);
          header->set_data_channel("action-" + UInt64ToString(data_channel_version));
          machine_->SendMessage(header, new MessageBuffer(local_reads));
        }
      }

      // If any writes will be performed locally, wait for all remote reads.
      if (writer_) {
        // Get channel.
        AtomicQueue<MessageBuffer*>* channel =
          machine_->DataChannel("action-" + UInt64ToString(data_channel_version));
        for (uint32 i = 0; i < remote_readers.size(); i++) {
          MessageBuffer* m = NULL;
          // Get results.
          while (!channel->Pop(&m)) {
            usleep(10);
          }
          MapProto remote_read;
          remote_read.ParseFromArray((*m)[0].data(), (*m)[0].size());
          for (int j = 0; j < remote_read.entries_size(); j++) {
            CHECK(reads_.count(remote_read.entries(j).key()) == 0);
            reads_[remote_read.entries(j).key()] = remote_read.entries(j).value();
          }
        }
        // Close channel.
        machine_->CloseDataChannel("action-" + UInt64ToString(data_channel_version));
      }
LOG(ERROR) << "Machine: "<<machine_id_<< "  DistributedExecutionContext received a txn:: data_channel_version:"<<data_channel_version<<"-- finish DistributedExecutionContext. min machine is:"<<min_machine_id;  
    }
  }

  // Destructor installs all LOCAL writes.
  ~DistributedExecutionContext() {
    if (!aborted_) {
      for (auto it = writes_.begin(); it != writes_.end(); ++it) {
        uint64 mds = config_->HashFileName(it->first);
        uint64 machine = config_->LookupMetadataShard(mds, replica_);
        if (machine == machine_id_) {
          store_->Put(it->first, it->second);
        }
      }
      for (auto it = deletions_.begin(); it != deletions_.end(); ++it) {
        uint64 mds = config_->HashFileName(*it);
        uint64 machine = config_->LookupMetadataShard(mds, replica_);
        if (machine == machine_id_) {
          store_->Delete(*it);
        }
      }
    }
  }

 private:
  // Local machine.
  Machine* machine_;

  uint64 machine_id_;

  // Deployment configuration.
  CalvinFSConfigMap* config_;

  // Local replica id.
  uint64 replica_;

  uint64 data_channel_version;

  uint32 origin_;
};

///////////////////////          MetadataStore          ///////////////////////

MetadataStore::MetadataStore(KVStore* store)
    : store_(store), machine_(NULL), config_(NULL) {
}

MetadataStore::~MetadataStore() {
  delete store_;
}

void MetadataStore::SetMachine(Machine* m) {
  machine_ = m;
  config_ = new CalvinFSConfigMap(machine_);

  machine_id_ = machine_->machine_id();

  replica_ = config_->LookupReplica(machine_id_);

  machines_per_replica_ = config_->GetPartitionsPerReplica();

  // Initialize by inserting an entry for the root directory "/" (actual
  // representation is "" since trailing slashes are always removed).
  if (IsLocal("")) {
    MetadataEntry entry;
    entry.mutable_permissions();
    entry.set_type(DIR);
    string serialized_entry;
    entry.SerializeToString(&serialized_entry);
    store_->Put("", serialized_entry);
  }
}

int RandomSize() {
  return 1 + rand() % 2047;
}

uint32 MetadataStore::LookupReplicaByDir(string dir) {
  return config_->LookupReplicaByDir(dir);
}


uint64 MetadataStore::GetMachineForReplica(Action* action) {
  set<uint32> replica_involved;
  uint32 to_expect = 0;
  string channel_name = "get-replica-" + UInt64ToString(action->distinct_id());
  set<uint64> machines_involved;
  map<uint64, set<string>> remote_keys;


LOG(ERROR) << "Machine: "<<machine_id_<<":^^^^^^^^ MetadataStore::GetMachineForReplica(begin)^^^^^^  distinct id is:"<<action->distinct_id();

  // Only need to check the readset(Note: read-write keys are also in the readset)
  for (int i = 0; i < action->readset_size(); i++) {
    uint64 mds = config_->HashFileName(action->readset(i));
    uint64 remote_machine_id = config_->LookupMetadataShard(mds, replica_);
    machines_involved.insert(mds);

    if (remote_machine_id == machine_id_) {
      // Get the mastership of the local records directly
      uint32 replica = GetLocalKeyMastership(action->readset(i));
      replica_involved.insert(replica);
      
      KeyMasterEntry map_entry;
      map_entry.set_key(action->readset(i));
      map_entry.set_master(replica);
      action->add_keys_origins()->CopyFrom(map_entry);
LOG(ERROR) << "Machine: "<<machine_id_<<":^^^^^^^^ MetadataStore::GetMachineForReplica(begin)^^^^^^  distinct id is:"<<action->distinct_id()<<" --key is local:"<<action->readset(i);
    } else {
      remote_keys[remote_machine_id].insert(action->readset(i));
    }
  }

  for(auto it = remote_keys.begin(); it != remote_keys.end();it++) {
    // Send message to remote machines to get the mastership of remote records
    uint64 remote_machineid = it->first;
    set<string> keys = it->second;

    Header* header = new Header();
    header->set_from(machine_id_);
    header->set_to(remote_machineid);
    header->set_type(Header::RPC);
    header->set_app("metadata");
    header->set_rpc("GETMASTER");
    header->add_misc_string(channel_name);
    header->add_misc_int(keys.size());
    for (auto it2 = keys.begin(); it2 != keys.end(); it2++) {
      header->add_misc_string(*it2);
    }

    machine_->SendMessage(header, new MessageBuffer());
    to_expect++;
  }

  // now that all RPCs have been sent, wait for responses
  AtomicQueue<MessageBuffer*>* channel = machine_->DataChannel(channel_name);
  while (to_expect > 0) {
    MessageBuffer* m = NULL;
    while (!channel->Pop(&m)) {
      // Wait for action to complete and be sent back.
      usleep(100);
    }
    
    KeyMasterEntries remote_entries;
    remote_entries.ParseFromArray((*m)[0].data(), (*m)[0].size());

    for (int j = 0; j < remote_entries.entries_size(); j++) {
      action->add_keys_origins()->CopyFrom(remote_entries.entries(j));
      uint32 key_replica = remote_entries.entries(j).master();
      replica_involved.insert(key_replica);
    } 


    to_expect--;
  }

LOG(ERROR) << "Machine: "<<machine_id_<<":^^^^^^^^ MetadataStore::GetMachineForReplica(finish getting all master)^^^^^^  distinct id is:"<<action->distinct_id();

  CHECK(replica_involved.size() >= 1);

  uint32 lowest_replica = *(replica_involved.begin());

  if (replica_involved.size() == 1) {
    action->set_single_replica(true);

    if (lowest_replica == replica_) {
      // For single local replica action, we can randomly send it to one machine of that replica
      return lowest_replica * machines_per_replica_ + rand() % machines_per_replica_; 
    } else {
      // For single remote replica action, we should forward to the first involved machine of that replica
      return config_->LookupMetadataShard(*(machines_involved.begin()), lowest_replica);
    }
  } else {
    action->set_single_replica(false);

LOG(ERROR) << "Machine: "<<machine_id_<<":^^^^^^^^ MetadataStore::GetMachineForReplica(multi-replica action)^^^^^^  distinct id is:"<<action->distinct_id()<<"  will forward to machine:"<<config_->LookupMetadataShard(*(machines_involved.begin()), lowest_replica);

    return config_->LookupMetadataShard(*(machines_involved.begin()), lowest_replica);
  }

}

bool MetadataStore::CheckLocalMastership(Action* action, set<string>& keys) {
  bool can_execute_now = true;
  for (int i = 0; i < action->readset_size(); i++) {
    if (IsLocal(action->readset(i))) {
      uint32 replica = GetLocalKeyMastership(action->readset(i));
      if (replica != action->origin()) {
        keys.insert(action->readset(i));
        can_execute_now = false;
      }
    }
  }

  return can_execute_now;
}

uint64 MetadataStore::GetHeadMachine(uint64 machine_id) {

  return (machine_id/machines_per_replica_) * machines_per_replica_;
}

uint32 MetadataStore::LocalReplica() {
  return replica_;
}

uint32 MetadataStore::GetLocalKeyMastership(string key) {
   string value;
   store_->Get(key, &value);
   MetadataEntry entry;
   entry.ParseFromString(value);

  return entry.master();
}

void MetadataStore::Init() {
  int asize = machine_->config().size();
  int bsize = 1000;
  int csize = 500;

  double start = GetTime();

  // Update root dir.
  if (IsLocal("")) {
    MetadataEntry entry;
    entry.mutable_permissions();
    entry.set_type(DIR);
    for (int i = 0; i < 1000; i++) {
      entry.add_dir_contents("a" + IntToString(i));
    }
    entry.set_master(0);
    string serialized_entry;
    entry.SerializeToString(&serialized_entry);
    store_->Put("", serialized_entry);
  }

  // Add dirs.
  for (int i = 0; i < asize; i++) {
    string dir("/a" + IntToString(i));
    if (IsLocal(dir)) {
      MetadataEntry entry;
      entry.mutable_permissions();
      entry.set_type(DIR);
      for (int j = 0; j < bsize; j++) {
        entry.add_dir_contents("b" + IntToString(j));
      }

      entry.set_master(LookupReplicaByDir(dir));
      string serialized_entry;
      entry.SerializeToString(&serialized_entry);
      store_->Put(dir, serialized_entry);
    }
    // Add subdirs.
    for (int j = 0; j < bsize; j++) {
      string subdir(dir + "/b" + IntToString(j));
      if (IsLocal(subdir)) {
        MetadataEntry entry;
        entry.mutable_permissions();
        entry.set_type(DIR);
        for (int k = 0; k < csize; k++) {
          entry.add_dir_contents("c" + IntToString(k));
        }

        entry.set_master(LookupReplicaByDir(subdir));
        string serialized_entry;
        entry.SerializeToString(&serialized_entry);
        store_->Put(subdir, serialized_entry);
      }
      // Add files.
      for (int k = 0; k < csize; k++) {
        string file(subdir + "/c" + IntToString(k));
        if (IsLocal(file)) {
          MetadataEntry entry;
          entry.mutable_permissions();
          entry.set_type(DATA);
          FilePart* fp = entry.add_file_parts();
          fp->set_length(RandomSize());
          fp->set_block_id(0);
          fp->set_block_offset(0);

          entry.set_master(LookupReplicaByDir(file));
          string serialized_entry;
          entry.SerializeToString(&serialized_entry);
          store_->Put(file, serialized_entry);
        }
      }
      if (j % 100 == 0) {
        LOG(ERROR) << "[" << machine_->machine_id() << "] "
                   << "MDS::Init() progress: " << (i * bsize + j) / 100 + 1
                   << "/" << asize * bsize / 100;
      }
    }
  }
  LOG(ERROR) << "[" << machine_->machine_id() << "] "
             << "MDS::Init() complete. Elapsed time: "
             << GetTime() - start << " seconds";
}

void MetadataStore::InitSmall() {
  int asize = machine_->config().size();
  int bsize = 1000;

  double start = GetTime();

  // Update root dir.
  if (IsLocal("")) {
    MetadataEntry entry;
    entry.mutable_permissions();
    entry.set_type(DIR);
    for (int i = 0; i < 1000; i++) {
      entry.add_dir_contents("a" + IntToString(i));
    }

    entry.set_master(0);
    string serialized_entry;
    entry.SerializeToString(&serialized_entry);
    store_->Put("", serialized_entry);
  }

  // Add dirs.
  for (int i = 0; i < asize; i++) {
    string dir("/a" + IntToString(i));
    if (IsLocal(dir)) {
      MetadataEntry entry;
      entry.mutable_permissions();
      entry.set_type(DIR);
      for (int j = 0; j < bsize; j++) {
        entry.add_dir_contents("b" + IntToString(j));
      }

      entry.set_master(LookupReplicaByDir(dir));
      string serialized_entry;
      entry.SerializeToString(&serialized_entry);
      store_->Put(dir, serialized_entry);
    }
    // Add subdirs.
    for (int j = 0; j < bsize; j++) {
      string subdir(dir + "/b" + IntToString(j));
      if (IsLocal(subdir)) {
        MetadataEntry entry;
        entry.mutable_permissions();
        entry.set_type(DIR);
        entry.add_dir_contents("c");

        entry.set_master(LookupReplicaByDir(subdir));
        string serialized_entry;
        entry.SerializeToString(&serialized_entry);
        store_->Put(subdir, serialized_entry);
      }
      // Add files.
      string file(subdir + "/c");
      if (IsLocal(file)) {
        MetadataEntry entry;
        entry.mutable_permissions();
        entry.set_type(DATA);

        entry.set_master(LookupReplicaByDir(file));
        string serialized_entry;
        entry.SerializeToString(&serialized_entry);
        store_->Put(file, serialized_entry);
      }
    }
  }
  LOG(ERROR) << "[" << machine_->machine_id() << "] "
             << "MDS::InitSmall() complete. Elapsed time: "
             << GetTime() - start << " seconds";
}

bool MetadataStore::IsLocal(const string& path) {
  return machine_id_ ==
         config_->LookupMetadataShard(
            config_->HashFileName(path),
            replica_);
}

void MetadataStore::GetRWSets(Action* action) {
  action->clear_readset();
  action->clear_writeset();

  MetadataAction::Type type =
      static_cast<MetadataAction::Type>(action->action_type());

  if (type == MetadataAction::CREATE_FILE) {
    MetadataAction::CreateFileInput in;
    in.ParseFromString(action->input());
    action->add_readset(in.path());
    action->add_writeset(in.path());
    action->add_readset(ParentDir(in.path()));
    action->add_writeset(ParentDir(in.path()));

  } else if (type == MetadataAction::ERASE) {
    MetadataAction::EraseInput in;
    in.ParseFromString(action->input());
    action->add_readset(in.path());
    action->add_writeset(in.path());
    action->add_readset(ParentDir(in.path()));
    action->add_writeset(ParentDir(in.path()));

  } else if (type == MetadataAction::COPY) {
    MetadataAction::CopyInput in;
    in.ParseFromString(action->input());
    action->add_readset(in.from_path());
    action->add_writeset(in.to_path());
    action->add_readset(ParentDir(in.to_path()));
    action->add_writeset(ParentDir(in.to_path()));

  } else if (type == MetadataAction::RENAME) {
    MetadataAction::RenameInput in;
    in.ParseFromString(action->input());
    action->add_readset(in.from_path());
    action->add_writeset(in.from_path());
    action->add_readset(ParentDir(in.from_path()));
    action->add_writeset(ParentDir(in.from_path()));
    action->add_writeset(in.to_path());
    action->add_readset(ParentDir(in.to_path()));
    action->add_writeset(ParentDir(in.to_path()));

  } else if (type == MetadataAction::LOOKUP) {
    MetadataAction::LookupInput in;
    in.ParseFromString(action->input());
    action->add_readset(in.path());

  } else if (type == MetadataAction::RESIZE) {
    MetadataAction::ResizeInput in;
    in.ParseFromString(action->input());
    action->add_readset(in.path());
    action->add_writeset(in.path());

  } else if (type == MetadataAction::WRITE) {
    MetadataAction::WriteInput in;
    in.ParseFromString(action->input());
    action->add_readset(in.path());
    action->add_writeset(in.path());

  } else if (type == MetadataAction::APPEND) {
    MetadataAction::AppendInput in;
    in.ParseFromString(action->input());
    action->add_readset(in.path());
    action->add_writeset(in.path());

  } else if (type == MetadataAction::CHANGE_PERMISSIONS) {
    MetadataAction::ChangePermissionsInput in;
    in.ParseFromString(action->input());
    action->add_readset(in.path());
    action->add_writeset(in.path());

  } else {
    LOG(FATAL) << "invalid action type";
  }
}

void MetadataStore::Run(Action* action) {
  // Prepare by performing all reads.
  ExecutionContext* context;
  if (machine_ == NULL) {
    context = new ExecutionContext(store_, action);
  } else {
    context = new DistributedExecutionContext(machine_, config_, store_, action);
  }

  if (context->Abort() == true) {
LOG(ERROR) << "Machine: "<<machine_->machine_id()<< " MetadataStore::Run(&&& Abort &&&)， action:"<<action->distinct_id();
    action->clear_client_machine();
    delete context;
    return;
  } else if (!context->IsWriter()) {
    delete context;
    return;
  }

  MetadataAction::Type type =
      static_cast<MetadataAction::Type>(action->action_type());

  if (action->remaster() == true) {
    Remaster_Internal(context, action);
LOG(ERROR) << "Machine: "<<machine_->machine_id()<< " MetadataStore::Run(*** finish Remaster_Internal)， action:"<<action->distinct_id();
  } else {

  if (type == MetadataAction::CREATE_FILE) {
    MetadataAction::CreateFileInput in;
    MetadataAction::CreateFileOutput out;
    in.ParseFromString(action->input());
    CreateFile_Internal(context, in, &out);
    out.SerializeToString(action->mutable_output());

  } else if (type == MetadataAction::ERASE) {
    MetadataAction::EraseInput in;
    MetadataAction::EraseOutput out;
    in.ParseFromString(action->input());
    Erase_Internal(context, in, &out);
    out.SerializeToString(action->mutable_output());

  } else if (type == MetadataAction::COPY) {
    MetadataAction::CopyInput in;
    MetadataAction::CopyOutput out;
    in.ParseFromString(action->input());
    Copy_Internal(context, in, &out);
    out.SerializeToString(action->mutable_output());

  } else if (type == MetadataAction::RENAME) {
LOG(ERROR) << "Machine: "<<machine_->machine_id()<< " MetadataStore::Run(*** begin RENAME)， action:"<<action->distinct_id();
    MetadataAction::RenameInput in;
    MetadataAction::RenameOutput out;
    in.ParseFromString(action->input());
    Rename_Internal(context, in, &out);
    out.SerializeToString(action->mutable_output());
LOG(ERROR) << "Machine: "<<machine_->machine_id()<< " MetadataStore::Run(*** finish RENAME)， action:"<<action->distinct_id();
  } else if (type == MetadataAction::LOOKUP) {
    MetadataAction::LookupInput in;
    MetadataAction::LookupOutput out;
    in.ParseFromString(action->input());
    Lookup_Internal(context, in, &out);
    out.SerializeToString(action->mutable_output());

  } else if (type == MetadataAction::RESIZE) {
    MetadataAction::ResizeInput in;
    MetadataAction::ResizeOutput out;
    in.ParseFromString(action->input());
    Resize_Internal(context, in, &out);
    out.SerializeToString(action->mutable_output());

  } else if (type == MetadataAction::WRITE) {
    MetadataAction::WriteInput in;
    MetadataAction::WriteOutput out;
    in.ParseFromString(action->input());
    Write_Internal(context, in, &out);
    out.SerializeToString(action->mutable_output());

  } else if (type == MetadataAction::APPEND) {
    MetadataAction::AppendInput in;
    MetadataAction::AppendOutput out;
    in.ParseFromString(action->input());
    Append_Internal(context, in, &out);
    out.SerializeToString(action->mutable_output());

  } else if (type == MetadataAction::CHANGE_PERMISSIONS) {
    MetadataAction::ChangePermissionsInput in;
    MetadataAction::ChangePermissionsOutput out;
    in.ParseFromString(action->input());
    ChangePermissions_Internal(context, in, &out);
    out.SerializeToString(action->mutable_output());

  } else {
    LOG(FATAL) << "invalid action type";
  }

  }

  delete context;
}

void MetadataStore::Remaster_Internal(ExecutionContext* context, Action* action) {
  MetadataEntry entry;
  uint32 origin_master = action->remaster_to();
  uint32 origin_from = action->remaster_from();
  vector<string> remastered_keys;

  for (int i = 0; i < action->remastered_keys_size(); i++) {
    if (!context->GetEntry(action->remastered_keys(i), &entry)) {
      // Entry doesn't exist!
      LOG(ERROR) <<"Entry doesn't exist!, should not happen!";
      return;
    }
    
    if (entry.master() == origin_from) {
      remastered_keys.push_back(action->remastered_keys(i));
      entry.set_master(origin_master);
      context->PutEntry(action->remastered_keys(i), entry);
    }

  }

LOG(ERROR) << "Machine: "<<machine_id_<<":^^^^^^^^ MetadataStore::Remaster_Internal^^^^^^  distinct id is:"<<action->distinct_id();

  if (origin_master == replica_ || action->remaster_from() == replica_) {
    // Need to send message to confirm the completation of the remaster operation
    uint32 keys_size = remastered_keys.size();
    uint64 machine_sent = machine_->machine_id();

    Header* header = new Header();
    header->set_from(machine_->machine_id());
    header->set_to(machine_sent);
    header->set_type(Header::RPC);
    header->set_app("blocklog");
    header->set_rpc("COMPLETED_REMASTER");

    MessageBuffer* m = new MessageBuffer();

    bool remaster_to;
    if (origin_master == replica_) {
      remaster_to = true;
    } else {
      remaster_to = false;
    }
    
    m->Append(ToScalar<bool>(remaster_to));
    m->Append(ToScalar<uint32>(keys_size));
    
    for (uint32 i = 0; i < keys_size; i++) {
      m->Append(ToScalar<string>(remastered_keys[i]));
    }
    machine_->SendMessage(header, m);
LOG(ERROR) << "Machine: "<<machine_id_<<":^^^^^^^^ MetadataStore::Remaster_Internal^^^^^^(send COMPLETED_REMASTER)  distinct id is:"<<action->distinct_id();
  }
}

void MetadataStore::CreateFile_Internal(
    ExecutionContext* context,
    const MetadataAction::CreateFileInput& in,
    MetadataAction::CreateFileOutput* out) {
  // Don't fuck with the root dir.
  if (in.path() == "") {
    out->set_success(false);
    out->add_errors(MetadataAction::PermissionDenied);
    return;
  }

  // Look up parent dir.
  string parent_path = ParentDir(in.path());
  MetadataEntry parent_entry;
  if (!context->GetEntry(parent_path, &parent_entry)) {
    // Parent doesn't exist!
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }

  // TODO(agt): Check permissions.

  // If file already exists, fail.
  // TODO(agt): Look up file directly instead of looking through parent dir?
  string filename = FileName(in.path());
  for (int i = 0; i < parent_entry.dir_contents_size(); i++) {
    if (parent_entry.dir_contents(i) == filename) {
      out->set_success(false);
      out->add_errors(MetadataAction::FileAlreadyExists);
      return;
    }
  }

  // Update parent.
  // TODO(agt): Keep dir_contents sorted?
  parent_entry.add_dir_contents(filename);
  context->PutEntry(parent_path, parent_entry);

  // Add entry.
  MetadataEntry entry;
  entry.mutable_permissions()->CopyFrom(in.permissions());
  entry.set_type(in.type());
  entry.set_master(replica_);
  context->PutEntry(in.path(), entry);
}

void MetadataStore::Erase_Internal(
    ExecutionContext* context,
    const MetadataAction::EraseInput& in,
    MetadataAction::EraseOutput* out) {
  // Don't fuck with the root dir.
  if (in.path() == "") {
    out->set_success(false);
    out->add_errors(MetadataAction::PermissionDenied);
    return;
  }

  // Look up parent dir.
  string parent_path = ParentDir(in.path());
  MetadataEntry parent_entry;
  if (!context->GetEntry(parent_path, &parent_entry)) {
    // Parent doesn't exist!
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }

  // Look up target file.
  MetadataEntry entry;
  if (!context->GetEntry(in.path(), &entry)) {
    // File doesn't exist!
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }
  if (entry.type() == DIR && entry.dir_contents_size() != 0) {
    // Trying to delete a non-empty directory!
    out->set_success(false);
    out->add_errors(MetadataAction::DirectoryNotEmpty);
    return;
  }

  // TODO(agt): Check permissions.

  // Delete target file entry.
  context->DeleteEntry(in.path());

  // Find file and remove it from parent directory.
  string filename = FileName(in.path());
  for (int i = 0; i < parent_entry.dir_contents_size(); i++) {
    if (parent_entry.dir_contents(i) == filename) {
      // Remove reference to target file entry from dir contents.
      parent_entry.mutable_dir_contents()
          ->SwapElements(i, parent_entry.dir_contents_size() - 1);
      parent_entry.mutable_dir_contents()->RemoveLast();

      // Write updated parent entry.
      context->PutEntry(parent_path, parent_entry);
      return;
    }
  }
}

void MetadataStore::Copy_Internal(
    ExecutionContext* context,
    const MetadataAction::CopyInput& in,
    MetadataAction::CopyOutput* out) {

  // Currently only support Copy: (non-recursive: only succeeds for DATA files and EMPTY directory)
  MetadataEntry from_entry;
  if (!context->GetEntry(in.from_path(), &from_entry)) {
    // File doesn't exist!
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }

  string parent_to_path = ParentDir(in.to_path());
  MetadataEntry parent_to_entry;
  if (!context->GetEntry(parent_to_path, &parent_to_entry)) {
    // File doesn't exist!
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }

  // If file already exists, fail.
  string filename = FileName(in.to_path());
  for (int i = 0; i < parent_to_entry.dir_contents_size(); i++) {
    if (parent_to_entry.dir_contents(i) == filename) {
      out->set_success(false);
      out->add_errors(MetadataAction::FileAlreadyExists);
      return;
    }
  }

  // Update parent
  parent_to_entry.add_dir_contents(filename);
  context->PutEntry(parent_to_path, parent_to_entry);
  
  // Add entry
  MetadataEntry to_entry;
  to_entry.CopyFrom(from_entry);
  to_entry.set_master(replica_);
  context->PutEntry(in.to_path(), to_entry);
}

void MetadataStore::Rename_Internal(
    ExecutionContext* context,
    const MetadataAction::RenameInput& in,
    MetadataAction::RenameOutput* out) {
  // Currently only support Copy: (non-recursive: only succeeds for DATA files and EMPTY directory)

  MetadataEntry from_entry;
  if (!context->GetEntry(in.from_path(), &from_entry)) {
    // File doesn't exist!
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }

  string parent_from_path = ParentDir(in.from_path());
  MetadataEntry parent_from_entry;
  if (!context->GetEntry(parent_from_path, &parent_from_entry)) {
    // File doesn't exist!
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }

  string parent_to_path = ParentDir(in.to_path());
  MetadataEntry parent_to_entry;
  if (!context->GetEntry(parent_to_path, &parent_to_entry)) {
    // File doesn't exist!
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }

  // If file already exists, fail.
  string to_filename = FileName(in.to_path());
  for (int i = 0; i < parent_to_entry.dir_contents_size(); i++) {
    if (parent_to_entry.dir_contents(i) == to_filename) {
      out->set_success(false);
      out->add_errors(MetadataAction::FileAlreadyExists);
      return;
    }
  }

  // Update to_parent (add new dir content)
  parent_to_entry.add_dir_contents(to_filename);
  context->PutEntry(parent_to_path, parent_to_entry);

  // Add to_entry
  MetadataEntry to_entry;
  to_entry.CopyFrom(from_entry);
  to_entry.set_master(replica_);
  context->PutEntry(in.to_path(), to_entry);

  // Update from_parent(Find file and remove it from parent directory.)
  string from_filename = FileName(in.from_path());
  for (int i = 0; i < parent_from_entry.dir_contents_size(); i++) {
    if (parent_from_entry.dir_contents(i) == from_filename) {

      // Remove reference to target file entry from dir contents.
      parent_from_entry.mutable_dir_contents()->SwapElements(i, parent_from_entry.dir_contents_size() - 1);

      parent_from_entry.mutable_dir_contents()->RemoveLast();

      // Write updated parent entry.
      context->PutEntry(parent_from_path, parent_from_entry);
      break;
    }
  }
 
  // Erase the from_entry
  context->DeleteEntry(in.from_path());

}

void MetadataStore::Lookup_Internal(
    ExecutionContext* context,
    const MetadataAction::LookupInput& in,
    MetadataAction::LookupOutput* out) {
  // Look up existing entry.
  MetadataEntry entry;
  if (!context->GetEntry(in.path(), &entry)) {
    // File doesn't exist!
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }

  // TODO(agt): Check permissions.

  // Return entry.
  out->mutable_entry()->CopyFrom(entry);
}

void MetadataStore::Resize_Internal(
    ExecutionContext* context,
    const MetadataAction::ResizeInput& in,
    MetadataAction::ResizeOutput* out) {
  // Look up existing entry.
  MetadataEntry entry;
  if (!context->GetEntry(in.path(), &entry)) {
    // File doesn't exist!
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }

  // Only resize DATA files.
  if (entry.type() != DATA) {
    out->set_success(false);
    out->add_errors(MetadataAction::WrongFileType);
    return;
  }

  // TODO(agt): Check permissions.

  // If we're resizing to size 0, just clear all file part.
  if (in.size() == 0) {
    entry.clear_file_parts();
    return;
  }

  // Truncate/remove entries that go past the target size.
  uint64 total = 0;
  for (int i = 0; i < entry.file_parts_size(); i++) {
    total += entry.file_parts(i).length();
    if (total >= in.size()) {
      // Discard all following file parts.
      entry.mutable_file_parts()->DeleteSubrange(
          i + 1,
          entry.file_parts_size() - i - 1);

      // Truncate current file part by (total - in.size()) bytes.
      entry.mutable_file_parts(i)->set_length(
          entry.file_parts(i).length() - (total - in.size()));
      break;
    }
  }

  // If resize operation INCREASES file size, append a new default file part.
  if (total < in.size()) {
    entry.add_file_parts()->set_length(in.size() - total);
  }

  // Write out new version of entry.
  context->PutEntry(in.path(), entry);
}

void MetadataStore::Write_Internal(
    ExecutionContext* context,
    const MetadataAction::WriteInput& in,
    MetadataAction::WriteOutput* out) {
  LOG(FATAL) << "not implemented";
}

void MetadataStore::Append_Internal(
    ExecutionContext* context,
    const MetadataAction::AppendInput& in,
    MetadataAction::AppendOutput* out) {
  // Look up existing entry.
  MetadataEntry entry;
  if (!context->GetEntry(in.path(), &entry)) {
    // File doesn't exist!
    out->set_success(false);
    out->add_errors(MetadataAction::FileDoesNotExist);
    return;
  }

  // Only append to DATA files.
  if (entry.type() != DATA) {
    out->set_success(false);
    out->add_errors(MetadataAction::WrongFileType);
    return;
  }

  // TODO(agt): Check permissions.

  // Append data to end of file.
  for (int i = 0; i < in.data_size(); i++) {
    entry.add_file_parts()->CopyFrom(in.data(i));
  }

  // Write out new version of entry.
  context->PutEntry(in.path(), entry);
}

void MetadataStore::ChangePermissions_Internal(
    ExecutionContext* context,
    const MetadataAction::ChangePermissionsInput& in,
    MetadataAction::ChangePermissionsOutput* out) {
  LOG(FATAL) << "not implemented";
}

