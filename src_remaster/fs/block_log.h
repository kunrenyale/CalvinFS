// Author: Alex Thomson (thomson@cs.yale.edu)
//         Kun  Ren <kun.ren@yale.edu>
//
// TODO(agt): Reduce number of string copies.

#ifndef CALVIN_FS_BLOCK_LOG_H_
#define CALVIN_FS_BLOCK_LOG_H_

#include <google/protobuf/repeated_field.h>
#include <set>
#include <vector>
#include <map>

#include "common/types.h"
#include "components/log/log.h"
#include "components/log/log_app.h"
#include "components/log/paxos2.h"
#include "fs/batch.pb.h"
#include "fs/block_store.h"
#include "fs/calvinfs.h"
#include "machine/app/app.h"
#include "proto/action.pb.h"

using std::set;
using std::vector;
using std::make_pair;
using std::map;

class Header;
class Machine;
class MessageBuffer;

class SequenceSource : public Source<UInt64Pair*> {
 public:
  explicit SequenceSource(Source<PairSequence*>* source)
      : source_(source), current_(NULL), index_(0), offset_(0) {
  }
  virtual ~SequenceSource() {
    delete source_;
    delete current_;
  }

  virtual bool Get(UInt64Pair** p) {
    // Get a non-empty sequence or return false.
    while (current_ == NULL) {
      if (!source_->Get(&current_)) {
        return false;
      }
      if (current_->pairs_size() == 0) {
        delete current_;
        current_ = NULL;
LOG(ERROR) <<"^^^^^^^^^SequenceSource wrong!!!";

      } else {
        index_ = 0;
        offset_ = current_->misc();
//LOG(ERROR) <<"^^^^^^^^^SequenceSource get a sequence:"<< offset_ << " first block_id is:"<<current_->pairs(0).first();
      }
    }

    // Expose next element in sequence.
    *p = new UInt64Pair();
    (*p)->set_first(current_->pairs(index_).first());
    (*p)->set_second(offset_);
    offset_ +=  current_->pairs(index_).second();
    if (++index_ == current_->pairs_size()) {
      delete current_;
      current_ = NULL;
    }
    return true;
  }

 private:
  Source<PairSequence*>* source_;
  PairSequence* current_;
  int index_;
  int offset_;
};

class BlockLogApp : public App {
 public:
  BlockLogApp() : go_(true), going_(false), to_delete_(60) {}

  virtual ~BlockLogApp() {
    Stop();
  }

  virtual void Start() {
    // Get local config info.
    config_ = new CalvinFSConfigMap(machine());

    // Record local replica id.
    replica_ = config_->LookupReplica(machine()->machine_id());

    // Record local replica's paxos machine.
    uint32 replica_count = config_->config().block_replication_factor();
    local_paxos_leader_ = replica_ * (machine()->config().size() / replica_count);

    // Note what machines contain metadata shards on the same replica.
    for (auto it = config_->mds().begin(); it != config_->mds().end(); ++it) {
      if (it->first.second == replica_) {
        mds_.insert(it->second);
      }
    }

    // Get ptr to local block store app.
    blocks_ = reinterpret_cast<BlockStoreApp*>(machine()->GetApp("blockstore"))
        ->blocks_;

    // Get ptr to paxos leader (maybe).
    if (machine()->machine_id() == local_paxos_leader_) {
      paxos_leader_ =
          reinterpret_cast<Paxos2App*>(machine()->GetApp("paxos2"));
    }

    // Get reader of Paxos output (using closest paxos machine).
    batch_sequence_ =
        new SequenceSource(
            new RemoteLogSource<PairSequence>(machine(), local_paxos_leader_, "paxos2"));

    // Okay, finally, start main loop!
    going_ = true;
    while (go_.load()) {
      // Create new batch once per epoch.
      double next_epoch = GetTime() + 0.005;

      // Create batch (iff there are any pending requests).
      int count = queue_.Size();
      if (count != 0) {
        ActionBatch batch;
        uint64 actual_offset = 0;

        for (int i = 0; i < count; i++) {
          Action* a = NULL;
          queue_.Pop(&a);
          a->set_version_offset(actual_offset++);
          batch.mutable_entries()->AddAllocated(a);
        }

        // Avoid multiple allocation.
        string* block = new string();
        batch.SerializeToString(block);

        // Choose block_id.
        uint64 block_id =
            machine()->GetGUID() * 2 + (block->size() > 1024 ? 1 : 0);

        // Send batch to block stores.
        for (uint64 i = 0; i < config_->config().block_replication_factor();
             i++) {
          Header* header = new Header();
          header->set_from(machine()->machine_id());
          header->set_to(config_->LookupBlucket(config_->HashBlockID(block_id), i));
          header->set_type(Header::RPC);
          header->set_app(name());
          header->set_rpc("BATCH");
          header->add_misc_int(block_id);
          header->add_misc_int(actual_offset);
          machine()->SendMessage(header, new MessageBuffer(Slice(*block)));
        }

        // Scheduler block for eventual deallocation.
        //to_delete_.Push(block);
      }

      // Delete old blocks.
      /**string* block;
      while (to_delete_.Pop(&block)) {
        delete block;
      }**/

      // Sleep until next epoch.
      SpinUntil(next_epoch);
    }

    going_ = false;
  }

  virtual void Stop() {
    go_ = false;
    while (going_.load()) {
      usleep(10);
    }
  }

  // Takes ownership of '*entry'.
  virtual void Append(Action* entry) {
    entry->set_origin(replica_);
    queue_.Push(entry);
  }

  virtual void Append(const Slice& entry, uint64 count = 1) {
    CHECK(count == 1);
    Action* a = new Action();
    a->ParseFromArray(entry.data(), entry.size());
    a->set_origin(replica_);
    queue_.Push(a);
  }

  virtual void HandleMessage(Header* header, MessageBuffer* message) {
    // Don't run any RPCs before Start() is called.
    while (!going_.load()) {
      usleep(10);
      if (!go_.load()) {
        return;
      }
    }

    if (header->rpc() == "APPEND") {
      Action* a = new Action();
      a->ParseFromArray((*message)[0].data(), (*message)[0].size());
      a->set_origin(replica_);
 
      if (a->remaster() == true) {
        Lock l(&remaster_latch);
        bool should_wait = false;
        for (int i = 0; i < a->remastered_keys_size(); i++) {
          if (delayed_actions_by_key_.find(a->remastered_keys(i)) != delayed_actions_by_key_.end()) {
            should_wait = true;
            delayed_actions_by_key_[a->remastered_keys(i)].push_back(a->distinct_id());
            delayed_actions_by_id_[a->distinct_id()].insert(a->remastered_keys(i));
          }
        }

        if (should_wait == false) {
          queue_.Push(a); 
        } else {
          delayed_actions_[a->distinct_id()] = a;
          coordinated_machins_[a->distinct_id()].insert(machine()->machine_id());
        }
        
      } else if (a->single_replica() == true && a->wait_for_remaster_pros() == false) {
        queue_.Push(a);
      } else {
LOG(ERROR) << "Machine: "<<machine()->machine_id() << " =>Block log recevie a multi-replica action. action id is:"<< a->distinct_id() <<" from machine:"<<header->from();
        Lock l(&remaster_latch);
        // The multi-replica actions that generate remaster actions
        a->set_wait_for_remaster_pros(true);
        a->set_remaster_to(replica_);
        uint64 distinct_id = a->distinct_id();
        set<uint32> involved_other_replicas;
        // action_local_remastered_keys: local entries that need to remaster, replica_id=>keys
        map<uint32, set<string>> action_local_remastered_keys;
        // forwarded_entries： all entries that need to remaster, machine_id=>entries
        map<uint64, KeyMasterEntries> forwarded_entries;
  
        // Queue the multi-replica actions in the delayed queue, and send the remaster actions(generate a new action) to the involved replicas;
        for (int i = 0; i < a->keys_origins_size(); i++) {
          KeyMasterEntry map_entry = a->keys_origins(i);
          string key = map_entry.key();
          uint32 key_replica = map_entry.master();
          uint64 mds = config_->HashFileName(key);
          
          if (key_replica != replica_) {
            uint64 machineid = config_->LookupMetadataShard(mds, replica_);
            if (machineid == machine()->machine_id()) {
              if (local_remastered_keys_.find(key) == local_remastered_keys_.end()) {
                forwarded_entries[machineid].add_entries()->CopyFrom(map_entry);   
              }
            } else {
              forwarded_entries[machineid].add_entries()->CopyFrom(map_entry);
            }       
          }
        }

        for (auto it = forwarded_entries.begin(); it != forwarded_entries.end();it++) {
          uint64 machineid = it->first;
          KeyMasterEntries entries = it->second;

          coordinated_machins_[distinct_id].insert(machineid);

          if (machineid == machine()->machine_id()) {
            delayed_actions_[distinct_id] = a;

            for (int i = 0; i < entries.entries_size(); i++) {
              string key = entries.entries(i).key();
              uint32 key_replica = entries.entries(i).master();

              delayed_actions_by_key_[key].push_back(distinct_id);
              delayed_actions_by_id_[distinct_id].insert(key);

              action_local_remastered_keys[key_replica].insert(key);   
            }
                      
            // Generate remaster action
            Action* remaster_action = new Action();
            remaster_action->CopyFrom(*a);
            remaster_action->set_remaster(true);
            remaster_action->set_remaster_to(replica_);
            remaster_action->clear_readset();
            remaster_action->clear_writeset();
            remaster_action->clear_keys_origins();
            remaster_action->set_single_replica(true);
            remaster_action->set_wait_for_remaster_pros(true);

            for (auto it2 = action_local_remastered_keys.begin(); it2 != action_local_remastered_keys.end(); it2++) {
              uint32 remote_replica = it2->first;
              set<string> remote_keys = it2->second;
              
              remaster_action->set_remaster_from(remote_replica);
              remaster_action->clear_distinct_id();
              remaster_action->set_distinct_id(machine()->GetGUID());
              remaster_action->clear_remastered_keys();

              for (auto it3 = remote_keys.begin(); it3 != remote_keys.end(); it3++) {
                if (local_remastering_keys_.find(*it3) == local_remastering_keys_.end()) {
                  remaster_action->add_remastered_keys(*it3);
                  local_remastering_keys_.insert(*it3);
                }
              }

              if (remaster_action->remastered_keys_size() > 0) {
                // Send the action to the relevant replica
                Header* header = new Header();
                header->set_from(machine()->machine_id());
                header->set_to(config_->LookupMetadataShard(config_->GetMdsFromMachine(machine()->machine_id()), remote_replica));
                header->set_type(Header::RPC);
                header->set_app(name());
                header->set_rpc("APPEND");
                header->add_misc_int(distinct_id);
                string* block = new string();
                remaster_action->SerializeToString(block);
                machine()->SendMessage(header, new MessageBuffer(Slice(*block)));
              }
            }

            
          } else {
            // Send remote remaster requests to remote machine
            Header* header = new Header();
            header->set_from(machine()->machine_id());
            header->set_to(machineid);
            header->set_type(Header::RPC);
            header->set_app(name());
            header->set_rpc("REMASTER_REQUEST");
            header->add_misc_int(distinct_id);
            MessageBuffer* m = new MessageBuffer(entries);
            machine()->SendMessage(header, m);
          }
        }

        if (forwarded_entries.size() == 0) {
          a->set_single_replica(true);
          a->set_wait_for_remaster_pros(false);
          queue_.Push(a); 
        }

      }

    } else if (header->rpc() == "REMASTER_REQUEST") {
      // Slave machines receive the remaster request
      KeyMasterEntries remote_entries;
      remote_entries.ParseFromArray((*message)[0].data(), (*message)[0].size());
      uint64 distinct_id = header->misc_int(0);
      map<uint32, set<string>> action_local_remastered_keys;

      for (int j = 0; j < remote_entries.entries_size(); j++) {
        string key = remote_entries.entries(j).key();
        uint32 key_replica = remote_entries.entries(j).master();

        if (local_remastered_keys_.find(key) != local_remastered_keys_.end()) {
          continue;
        }

        action_local_remastered_keys[key_replica].insert(key);
              
        delayed_actions_by_key_[key].push_back(distinct_id);
        delayed_actions_by_id_[distinct_id].insert(key);
      }

      if (action_local_remastered_keys.size() == 0) {
        return;
      }

      coordinated_machine_by_id_[distinct_id] = header->from();

      // Generate remaster action
      Action* remaster_action = new Action();
      remaster_action->set_remaster(true);
      remaster_action->set_remaster_to(replica_);
      remaster_action->set_single_replica(true);
      remaster_action->set_wait_for_remaster_pros(true);

      for (auto it2 = action_local_remastered_keys.begin(); it2 != action_local_remastered_keys.end(); it2++) {
        uint32 remote_replica = it2->first;
        set<string> remote_keys = it2->second;
              
        remaster_action->set_remaster_from(remote_replica);
        remaster_action->clear_distinct_id();
        remaster_action->set_distinct_id(machine()->GetGUID());
        remaster_action->clear_remastered_keys();

        for (auto it3 = remote_keys.begin(); it3 != remote_keys.end(); it3++) {
          if (local_remastering_keys_.find(*it3) == local_remastering_keys_.end()) {
            remaster_action->add_remastered_keys(*it3);
            local_remastering_keys_.insert(*it3);
          }
        }

        if (remaster_action->remastered_keys_size() > 0) {
          // Send the action to the relevant replica
          Header* header = new Header();
          header->set_from(machine()->machine_id());
          header->set_to(config_->LookupMetadataShard(config_->GetMdsFromMachine(machine()->machine_id()), remote_replica));
          header->set_type(Header::RPC);
          header->set_app(name());
          header->set_rpc("APPEND");
          string* block = new string();
          remaster_action->SerializeToString(block);
          machine()->SendMessage(header, new MessageBuffer(Slice(*block)));
        }
      }

    } else if (header->rpc() == "REMASTER_REQUEST_ACK") {
      // Slave machines send ack to master machine when it finishes its local remaster
      Lock l(&remaster_latch);
      uint64 machine_from = header->from();
      uint64 distinct_id = header->misc_int(0);
    
      CHECK(coordinated_machins_.find(distinct_id) != coordinated_machins_.end());
      coordinated_machins_[distinct_id].erase(machine_from);
     
      if (coordinated_machins_[distinct_id].size() == 0) {
        // Now the action can go to log
        coordinated_machins_.erase(distinct_id);
        queue_.Push(delayed_actions_[distinct_id]);
        delayed_actions_.erase(distinct_id);
      }
      
    } else if (header->rpc() == "COMPLETED_REMASTER")  {
      // Local machine finishes local remaster
      Lock l(&remaster_latch);
      Scalar s;
      s.ParseFromArray((*message)[0].data(), (*message)[0].size());
      bool remaster_to = FromScalar<bool>(s);

      s.ParseFromArray((*message)[1].data(), (*message)[1].size());
      uint32 keys_num = FromScalar<uint32>(s);
LOG(ERROR) << "Machine: "<<machine()->machine_id() << " =>Block log recevie COMPLETED_REMASTER messagea. from machine:"<<header->from()<<"  keys num is:"<<keys_num;

      for (uint32 i = 0; i < keys_num; i++) {
        s.ParseFromArray((*message)[i+2].data(), (*message)[i+2].size());
        string key = FromScalar<string>(s);
        if (remaster_to == true) {
LOG(ERROR) << "Machine: "<<machine()->machine_id() << " =>Block log recevie COMPLETED_REMASTER messagea. from machine:"<<header->from()<<"  keys is:"<<key;
          local_remastered_keys_.insert(key);
          local_remastering_keys_.erase(key);
       
          CHECK(delayed_actions_by_key_.find(key) != delayed_actions_by_key_.end());

          vector<uint64> delayed_queue = delayed_actions_by_key_[key];
          for (uint32 j = 0; j < delayed_queue.size(); j++) {
            uint64 distinct_id = delayed_queue[j];
            delayed_actions_by_id_[distinct_id].erase(key);
            if (delayed_actions_by_id_[distinct_id].size() == 0) {
              delayed_actions_by_id_.erase(distinct_id);
              if (delayed_actions_.find(distinct_id) != delayed_actions_.end()) {
                // The master node
                coordinated_machins_[distinct_id].erase(machine()->machine_id());
                if (coordinated_machins_[distinct_id].size() == 0) {
                  coordinated_machins_.erase(distinct_id);
                  queue_.Push(delayed_actions_[distinct_id]);
                  delayed_actions_.erase(distinct_id);
                }
              } else {
                // the slave node, should send remaster_request_ack to master node
                CHECK(coordinated_machine_by_id_.find(distinct_id) != coordinated_machine_by_id_.end());
                Header* header = new Header();
                header->set_from(machine()->machine_id());
                header->set_to(coordinated_machine_by_id_[distinct_id]);
                header->set_type(Header::RPC);
                header->set_app(name());
                header->set_rpc("REMASTER_REQUEST_ACK");
                header->add_misc_int(distinct_id);
                MessageBuffer* m = new MessageBuffer();
                machine()->SendMessage(header, m);
                
                coordinated_machine_by_id_.erase(distinct_id);
              }
            }
          }
          
          delayed_actions_by_key_.erase(key); 

        } else {
          if (local_remastered_keys_.count(key) > 0) {
            local_remastered_keys_.erase(key);  
          }
        }
      }
LOG(ERROR) << "Machine: "<<machine()->machine_id() << " =>Block log finished COMPLETED_REMASTER messagea. from machine:"<<header->from();

    } else if (header->rpc() == "BATCH") {
      // Write batch block to local block store.
      uint64 block_id = header->misc_int(0);
      uint64 batch_size = header->misc_int(1);

      blocks_->Put(block_id, (*message)[0]);
//LOG(ERROR) << "Machine: "<<machine()->machine_id() << " =>Block log recevie a BATCH request. block id is:"<< block_id <<" from machine:"<<header->from()<<" , batch size is:"<<batch_size;
      // Parse batch.
      ActionBatch batch;
      batch.ParseFromArray((*message)[0].data(), (*message)[0].size());
      uint64 message_from_ = header->from();

      //  If (This batch come from this replica) → send SUBMIT to the Sequencer(LogApp) on the master node of the local paxos participants
      if (config_->LookupReplica(message_from_) == replica_) {
        Header* header = new Header();
        header->set_from(machine()->machine_id());
        header->set_to(local_paxos_leader_);  // Local Paxos leader.
        header->set_type(Header::RPC);
        header->set_app(name());
        header->set_rpc("SUBMIT");
        header->add_misc_int(block_id);
        header->add_misc_int(batch_size);
        machine()->SendMessage(header, new MessageBuffer());
      }

      // Forward sub-batches to relevant readers (same replica only).
      map<uint64, ActionBatch> subbatches;
      for (int i = 0; i < batch.entries_size(); i++) {
        set<uint64> recipients;

        if (batch.entries(i).remaster() == true) {
          for (int j = 0; j < batch.entries(i).remastered_keys_size(); j++) {
            uint64 mds = config_->HashFileName(batch.entries(i).remastered_keys(j));
            recipients.insert(config_->LookupMetadataShard(mds, replica_));
          }
        } else {

          for (int j = 0; j < batch.entries(i).readset_size(); j++) {
            uint64 mds = config_->HashFileName(batch.entries(i).readset(j));
            recipients.insert(config_->LookupMetadataShard(mds, replica_));
          }
          for (int j = 0; j < batch.entries(i).writeset_size(); j++) {
            uint64 mds = config_->HashFileName(batch.entries(i).writeset(j));
            recipients.insert(config_->LookupMetadataShard(mds, replica_));
          }
        }

        for (auto it = recipients.begin(); it != recipients.end(); ++it) {
          subbatches[*it].add_entries()->CopyFrom(batch.entries(i));
        }
      }

      for (auto it = mds_.begin(); it != mds_.end(); ++it) {
        header = new Header();
        header->set_from(machine()->machine_id());
        header->set_to(*it);
        header->set_type(Header::RPC);
        header->set_app(name());
        header->set_rpc("SUBBATCH");
        header->add_misc_int(block_id);
        machine()->SendMessage(header, new MessageBuffer(subbatches[*it]));
      }      

    } else if (header->rpc() == "SUBMIT") {

      uint64 block_id = header->misc_int(0);

      uint64 count = header->misc_int(1);
      paxos_leader_->Append(block_id, count);
//LOG(ERROR) << "Machine: "<<machine()->machine_id()<< "=>Block log recevie a SUBMIT request. block id is:"<< block_id<<" . size:"<<count<<" from machine:"<<header->from();
    } else if (header->rpc() == "SUBBATCH") {
      uint64 block_id = header->misc_int(0);
      ActionBatch* batch = new ActionBatch();
      batch->ParseFromArray((*message)[0].data(), (*message)[0].size());
      subbatches_.Put(block_id, batch);
//LOG(ERROR) << "Machine: "<<machine()->machine_id()<< "=>Block log recevie a SUBBATCH request. block id is:"<< block_id<<" from machine:"<<header->from();
    } else {
      LOG(FATAL) << "unknown RPC type: " << header->rpc();
    }
  }

  Source<Action*>* GetActionSource() {
    return new ActionSource(this);
  }

 private:
  // True iff main thread SHOULD run.
  std::atomic<bool> go_;

  // True iff main thread IS running.
  std::atomic<bool> going_;

  // CalvinFS configuration.
  CalvinFSConfigMap* config_;

  // Replica to which we belong.
  uint64 replica_;

  // This machine's local block store.
  BlockStore* blocks_;

  // List of machine ids that have metadata shards with replica == replica_.
  set<uint64> mds_;

  // Local paxos app (used only by machine 0).
  Paxos2App* paxos_leader_;

  // Number of votes for each batch (used only by machine 0).
  map<uint64, int> batch_votes_;

  // Subbatches received.
  AtomicMap<uint64, ActionBatch*> subbatches_;

  // Paxos log output.
  Source<UInt64Pair*>* batch_sequence_;

  // Pending append requests.
  AtomicQueue<Action*> queue_;

  // Delayed deallocation queue.
  // TODO(agt): Ugh this is horrible, we should replace this with ref counting!
  DelayQueue<string*> to_delete_;

  uint64 local_paxos_leader_;

  map<string, vector<uint64>> delayed_actions_by_key_;

  map<uint64, set<string>> delayed_actions_by_id_;

  map<uint64, Action*> delayed_actions_;
  
  map<uint64, uint64>  coordinated_machine_by_id_;

  map<uint64, set<uint64>> coordinated_machins_;

  set<string> local_remastered_keys_;

  set<string> local_remastering_keys_;

  Mutex remaster_latch;

  friend class ActionSource;
  class ActionSource : public Source<Action*> {
   public:
    virtual ~ActionSource() {}
    virtual bool Get(Action** a) {
      while (true) {
        // Make sure we have a valid (i.e. non-zero) subbatch_id_, or return
        // false if we can't get one.
        if (subbatch_id_ == 0) {
          UInt64Pair* p = NULL;
          if (log_->batch_sequence_->Get(&p)) {
            subbatch_id_ = p->first();
            subbatch_version_ = p->second();
            delete p;
//LOG(ERROR) << "*********Blocklog subbatch_id:"<< subbatch_id_ << " subbatch_version_:"<<subbatch_version_;
          } else {
            usleep(200);
            return false;
          }
        }

        // Make sure we have a valid pointer to the current (nonempty)
        // subbatch, or return false if we can't get one.
        if (subbatch_ == NULL) {
          // Have we received the subbatch corresponding to subbatch_id_?
          if (!log_->subbatches_.Lookup(subbatch_id_, &subbatch_)) {
            // Nope. Gotta try again later.
//LOG(ERROR) << "*********Have we received the subbatch corresponding to subbatch_id_? batch_id:"<<subbatch_id_;
            usleep(20);
            return false;
          } else {
            // Got the subbatch! Is it empty?
            if (subbatch_->entries_size() == 0) {
              // Doh, the batch was empty! Throw it away and keep looking.
//LOG(ERROR) <<"*********Doh, the batch was empty! Throw it away and keep looking.";
              delete subbatch_;
              log_->subbatches_.Erase(subbatch_id_);
              subbatch_ = NULL;
              subbatch_id_ = 0;
            } else {
              // Okay, got a non-empty subbatch! Reverse the order of elements
              // so we can now repeatedly call ReleaseLast on the entries.
//LOG(ERROR) <<"*********Okay, got a non-empty subbatch! Reverse the order of elements. the subbatch size is:"<<subbatch_->entries_size();
              for (int i = 0; i < subbatch_->entries_size() / 2; i++) {
                subbatch_->mutable_entries()->SwapElements(
                    i,
                    subbatch_->entries_size()-1-i);
              }
              // Now we are ready to start returning actions from this subbatch.
              break;
            }
          }
        } else {
          // Already had a good subbatch. Onward.
//LOG(ERROR) <<"*********Already had a good subbatch. Onward.";
          break;
        }
      }

      // Should be good to go now.
      CHECK(subbatch_->entries_size() != 0);
      *a = subbatch_->mutable_entries()->ReleaseLast();
      (*a)->set_version(subbatch_version_ + (*a)->version_offset());
      (*a)->clear_version_offset();
//LOG(ERROR) <<"^^^^^^^^^ActionSource get a txn: distinct_id is: "<<(*a)->distinct_id();
      if (subbatch_->entries_size() == 0) {
        // Okay, NOW the batch is empty.
        delete subbatch_;
        log_->subbatches_.Erase(subbatch_id_);
        subbatch_ = NULL;
        subbatch_id_ = 0;
      }
      return true;
    }

   private:
    friend class BlockLogApp;
    ActionSource(BlockLogApp* log)
      : log_(log), subbatch_id_(0), subbatch_(NULL) {
    }

    // Pointer to underlying BlockLogApp.
    BlockLogApp* log_;

    // ID of (and pointer to) current subbatch (which is technically owned by
    // log_->subbatches_).
    uint64 subbatch_id_;
    ActionBatch* subbatch_;

    // Version of initial action in subbatch_.
    uint64 subbatch_version_;
  };
};

#endif  // CALVIN_FS_BLOCK_LOG_H_

