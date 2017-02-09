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

      if ((a->single_replica() == true || a->remaster() == true) && a->wait_for_remaster_pros() == false) {
        queue_.Push(a);
if (a->remaster() == true) {
LOG(ERROR) << "Machine: "<<machine()->machine_id() << " =>Block log recevie a remaster action. action id is:"<< a->distinct_id() <<" from machine:"<<header->from();
} else {
LOG(ERROR) << "Machine: "<<machine()->machine_id() << " =>Block log recevie a normal action. action id is:"<< a->distinct_id() <<" from machine:"<<header->from();
}
      } else if (a->single_replica() == true && a->wait_for_remaster_pros() == true) {
        a->set_remaster_to(replica_);
        
        bool should_wait = false;
        // Queue the multi-replica actions in the delayed queue, and send the remaster actions(generate a new action) to the involved replicas;
        for (int i = 0; i < a->keys_origins_size(); i++) {
          KeyValueEntry map_entry = a->keys_origins(i);

          if (recent_remastered_keys.find(map_entry.key()) != recent_remastered_keys.end()) {
            continue;
          } else if (map_entry.value() != replica_) {
            should_wait = true;
            if (delayed_actions_by_key.find(map_entry.key()) != delayed_actions_by_key.end()) {
              delayed_actions_by_key[map_entry.key()].push_back(a);
            } else {
              vector<Action*> actions;
              actions.push_back(a);
              delayed_actions_by_key[map_entry.key()] = actions;
            }


            if (delayed_actions_by_id_.find(a->distinct_id()) != delayed_actions_by_id_.end()) {
              delayed_actions_by_id_[a->distinct_id()].insert(map_entry.key());
            } else {
              set<string> keys;
              keys.insert(map_entry.key());
              delayed_actions_by_id_[a->distinct_id()] = keys;
            }

          }
        }

        if (should_wait == false) {
          queue_.Push(a);      
        }

      } else {

LOG(ERROR) << "Machine: "<<machine()->machine_id() << " =>Block log recevie a multi-replica action. action id is:"<< a->distinct_id() <<" from machine:"<<header->from();

        // The multi-replica actions that generate remaster actions
        a->set_wait_for_remaster_pros(true);
        a->set_remaster_to(replica_);
        set<uint32> involved_other_replicas;
        map<uint32, set<string>> remastered_keys;
        bool should_wait = false;
  
        // Queue the multi-replica actions in the delayed queue, and send the remaster actions(generate a new action) to the involved replicas;
        for (int i = 0; i < a->keys_origins_size(); i++) {
          KeyValueEntry map_entry = a->keys_origins(i);
          if (recent_remastered_keys.find(map_entry.key()) != recent_remastered_keys.end()) {
            continue;
          } else if (map_entry.value() != replica_) {
            should_wait = true;
            involved_other_replicas.insert(map_entry.value());
            if (remastered_keys.find(map_entry.value()) != remastered_keys.end()) {
              remastered_keys[map_entry.value()].insert(map_entry.key());
            } else {
              set<string> keys;
              keys.insert(map_entry.key());
              remastered_keys[map_entry.value()] = keys;
            }
            
            if (delayed_actions_by_key.find(map_entry.key()) != delayed_actions_by_key.end()) {
              delayed_actions_by_key[map_entry.key()].push_back(a);
            } else {
              vector<Action*> actions;
              actions.push_back(a);
              delayed_actions_by_key[map_entry.key()] = actions;
            }

            if (delayed_actions_by_id_.find(a->distinct_id()) != delayed_actions_by_id_.end()) {
              delayed_actions_by_id_[a->distinct_id()].insert(map_entry.key());
            } else {
              set<string> keys;
              keys.insert(map_entry.key());
              delayed_actions_by_id_[a->distinct_id()] = keys;
            }

          }
        }

        if (should_wait == false) {
          a->set_single_replica(true);
          queue_.Push(a);
LOG(ERROR) << "Machine: "<<machine()->machine_id() << " =>Block log recevie a multi-replica action. action id is:"<< a->distinct_id() <<" from machine:"<<header->from()<<"-- put it into queue";
        } else {

          // Send the remaster actions(generate a new action) to the involved replicas;
          Action* remaster_action = new Action();
          remaster_action->CopyFrom(*a);
          remaster_action->set_remaster(true);
          remaster_action->set_remaster_to(replica_);
          remaster_action->clear_readset();
          remaster_action->clear_writeset();
          remaster_action->clear_keys_origins();
          remaster_action->clear_distinct_id();
          remaster_action->set_distinct_id(machine()->GetGUID());
          remaster_action->set_single_replica(true);
          remaster_action->set_wait_for_remaster_pros(false);

LOG(ERROR) << "Machine: "<<machine()->machine_id() << " =>Block log recevie a multi-replica action. action id is:"<< remaster_action->distinct_id() <<" from machine:"<<header->from()<<"-- send remaster action";
          for (auto it = involved_other_replicas.begin(); it != involved_other_replicas.end(); ++it) {
            uint32 sentto_replica = *it;
            remaster_action->set_remaster_from(sentto_replica);
            set<string> keys = remastered_keys[sentto_replica];
            for (auto it = keys.begin(); it != keys.end(); it++) {
              remaster_action->add_remastered_keys(*it);
            }
          
            Header* header = new Header();
            header->set_from(machine()->machine_id());
            header->set_to(sentto_replica*config_->GetPartitionsPerReplica() + rand()%config_->GetPartitionsPerReplica());
            header->set_type(Header::RPC);
            header->set_app(name());
            header->set_rpc("APPEND");
            string* block = new string();
            remaster_action->SerializeToString(block);
            machine()->SendMessage(header, new MessageBuffer(Slice(*block)));
          }
        }

      }

    } else if (header->rpc() == "COMPLETED_REMASTER")  {
      // After the completed remaster, now it might be safe to get multi-replica actions and relevant blocked actions off from the queue.
      Scalar s;
      s.ParseFromArray((*message)[0].data(), (*message)[0].size());
      uint32 keys_num = FromScalar<uint32>(s);
LOG(ERROR) << "Machine: "<<machine()->machine_id() << " =>Block log recevie COMPLETED_REMASTER messagea. from machine:"<<header->from()<<"keys num is:"<<keys_num;

      for (uint32 i = 0; i < keys_num; i++) {
        s.ParseFromArray((*message)[i+1].data(), (*message)[i+1].size());
        string key = FromScalar<string>(s);
LOG(ERROR) << "Machine: "<<machine()->machine_id() << " =>Block log recevie COMPLETED_REMASTER messagea. from machine:"<<header->from()<<"keys is:"<<key;
        recent_remastered_keys[key] = replica_;

        if (delayed_actions_by_key.find(key) != delayed_actions_by_key.end()) {

          vector<Action*> delayed_queue = delayed_actions_by_key[key];
          for (uint32 j = 0; j < delayed_queue.size(); j++) {
            Action* action = delayed_queue[j];
  
            delayed_actions_by_id_[action->distinct_id()].erase(key);
            if (delayed_actions_by_id_[action->distinct_id()].size() == 0) {
              queue_.Push(action);
              delayed_actions_by_id_.erase(action->distinct_id());
LOG(ERROR) << "Machine: "<<machine()->machine_id() << " =>Block log receive COMPLETED_REMASTER messagea. from machine:"<<header->from()<<"One action is ready now, is:"<<action->distinct_id();
            }

          }
          delayed_actions_by_key.erase(key); 
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

  map<string, vector<Action*>> delayed_actions_by_key;

  map<uint64, set<string>> delayed_actions_by_id_;

  map<string, uint32> recent_remastered_keys;

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
