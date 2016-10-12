// Author: Alex Thomson (thomson@cs.yale.edu)
// Author: Kun  Ren (kun.ren@yale.edu)
//

#include "components/scheduler/locking_scheduler.h"

#include <glog/logging.h>
#include <set>
#include <string>
#include "common/source.h"
#include "common/types.h"
#include "common/utils.h"
#include "components/scheduler/lock_manager.h"
#include "components/scheduler/scheduler.h"
#include "components/store/store_app.h"
#include "proto/header.pb.h"
#include "proto/action.pb.h"

using std::string;

REGISTER_APP(LockingScheduler) {
  return new LockingScheduler();
}

void LockingScheduler::MainLoopBody() {
  Action* action;

//CHECK(running_action_count_ < 3000);
//CHECK(active_actions_.size() < 3000);

  // Start processing the next incoming action request.
  if (static_cast<int>(active_actions_.size()) < kMaxActiveActions &&
      running_action_count_ < kMaxRunningActions &&
      action_requests_->Get(&action)) {
usleep(100);
    high_water_mark_ = action->version();
    active_actions_.insert(action->version());
    int ungranted_requests = 0;
//LOG(ERROR) << "Machine: "<<machine()->machine_id()<<":--Scheduler receive action: " << action->version()<<" distinct id is:"<<action->distinct_id();

    if (action->single_replica() == false) {
      bool ignore = true;
      set<string> writeset;
      for (int i = 0; i < action->writeset_size(); i++) {
        uint32 replica = store_->LookupReplicaByDir(action->writeset(i));
        if (replica == local_replica_) {
          actions_involved_replica_.insert(action->distinct_id());
        }
        if ((store_->IsLocal(action->writeset(i))) && (replica == action->origin())) {
          if (ignore == true) {
            ignore = false;
          }

          writeset.insert(action->writeset(i));
          if (!lm_.WriteLock(action, action->writeset(i))) {
            ungranted_requests++;
          }
        }
      }

      for (int i = 0; i < action->readset_size(); i++) {
        uint32 replica = store_->LookupReplicaByDir(action->readset(i));
        if (replica == local_replica_) {
          actions_involved_replica_.insert(action->distinct_id());
        }
        if ((store_->IsLocal(action->readset(i))) && (replica == action->origin())) {
          if (ignore == true) {
            ignore = false;
          }

          if (writeset.count(action->readset(i)) == 0) {
            if (!lm_.ReadLock(action, action->readset(i))) {
              ungranted_requests++;
            }
          }
        }
      }

//LOG(ERROR) << "Machine: "<<machine()->machine_id()<<":------------ scheduler after lock: " << action->version()<<" distinct id is:"<<action->distinct_id()<<"  create_new():"<<action->create_new()<<"  LocalReplica is:"<<store_->LocalReplica()<<"   action->origin() is: "<<action->origin()<<"  .action->lowest_involved_machine() is:"<<action->lowest_involved_machine();

      if (ignore == true) {
        // Finish this loop
        if (action->create_new() == true && local_replica_ != action->origin() && actions_involved_replica_.find(action->distinct_id()) !=  actions_involved_replica_.end()) {
          uint64 this_machine = machine()->machine_id();
          uint64 machine_sent = store_->GetHeadMachine(this_machine);
          Header* header = new Header();
          header->set_from(this_machine);
          header->set_to(machine_sent);
        
          header->add_misc_int(action->distinct_id());
          header->add_misc_int(action->involved_machines());
          header->set_type(Header::RPC);
          header->set_app("blocklog");
          header->set_rpc("CREATE_NEW");


          Action* new_action = new Action();
          new_action->CopyFrom(*action);
          new_action->clear_client_machine();
          new_action->clear_client_channel();
          new_action->clear_origin();
          new_action->set_create_new(false);
          string* block = new string();
          new_action->SerializeToString(block);

          machine()->SendMessage(header, new MessageBuffer(Slice(*block)));

          actions_involved_replica_.erase(action->distinct_id());
        }


//LOG(ERROR) << "Machine: "<<machine()->machine_id()<<":------------ scheduler ignore this txn: " << action->version()<<" distinct id is:"<<action->distinct_id();
        return;
      } 
    }  else {

      // Request write locks. Track requests so we can check that we don't
      // re-request any as read locks.
      set<string> writeset;
      for (int i = 0; i < action->writeset_size(); i++) {
        if (store_->IsLocal(action->writeset(i))) {
          writeset.insert(action->writeset(i));
          if (!lm_.WriteLock(action, action->writeset(i))) {
            ungranted_requests++;
          }
        }
      }

      // Request read locks.
      for (int i = 0; i < action->readset_size(); i++) {
        // Avoid re-requesting shared locks if an exclusive lock is already
        // requested.
        if (store_->IsLocal(action->readset(i))) {
          if (writeset.count(action->readset(i)) == 0) {
            if (!lm_.ReadLock(action, action->readset(i))) {
              ungranted_requests++;
            }
          }
        }
      }
    }

    // If all read and write locks were immediately acquired, this action
    // is ready to run.
    if (ungranted_requests == 0) {
      if (action->single_replica() == false && action->create_new() == true && local_replica_ != action->origin() && actions_involved_replica_.find(action->distinct_id()) !=  actions_involved_replica_.end()) {
        uint64 this_machine = machine()->machine_id();
        uint64 machine_sent = store_->GetHeadMachine(this_machine);
        Header* header = new Header();
        header->set_from(this_machine);
        header->set_to(machine_sent);

        header->add_misc_int(action->distinct_id());
        header->add_misc_int(action->involved_machines());
        header->set_type(Header::RPC);
        header->set_app("blocklog");
        header->set_rpc("CREATE_NEW");

        Action* new_action = new Action();
        new_action->CopyFrom(*action);
        new_action->clear_client_machine();
        new_action->clear_client_channel();
        new_action->clear_origin();
        new_action->set_create_new(false);
        string* block = new string();
        new_action->SerializeToString(block);

        machine()->SendMessage(header, new MessageBuffer(Slice(*block)));

        actions_involved_replica_.erase(action->distinct_id());
      }

      running_action_count_++;
      store_->RunAsync(action, &completed_);
//LOG(ERROR) << "Machine: "<<machine()->machine_id()<<":------------ store_->RunAsync(action, &completed_): " << action->version()<<" distinct id is:"<<action->distinct_id();
    }
  }

  // Process all actions that have finished running.
  while (completed_.Pop(&action)) {

    if (action->single_replica() == false) {
      // Release read locks.
      for (int i = 0; i < action->readset_size(); i++) {
        uint32 replica = store_->LookupReplicaByDir(action->readset(i));
        if ((store_->IsLocal(action->readset(i))) && (replica == action->origin())) {
          lm_.Release(action, action->readset(i));
        }
      }

      // Release write locks. (Okay to release a lock twice.)
      for (int i = 0; i < action->writeset_size(); i++) {
        uint32 replica = store_->LookupReplicaByDir(action->writeset(i));
        if ((store_->IsLocal(action->writeset(i))) && (replica == action->origin())) {
          lm_.Release(action, action->writeset(i));
        }
      }
    } else {
      // Release read locks.
      for (int i = 0; i < action->readset_size(); i++) {
        if (store_->IsLocal(action->readset(i))) {
          lm_.Release(action, action->readset(i));
        }
      }

      // Release write locks. (Okay to release a lock twice.)
      for (int i = 0; i < action->writeset_size(); i++) {
        if (store_->IsLocal(action->writeset(i))) {
          lm_.Release(action, action->writeset(i));
        }
      }
    }
//LOG(ERROR) << "Machine: "<<machine()->machine_id()<<":** scheduler finish running action: " << action->version()<<" distinct id is:"<<action->distinct_id();
    active_actions_.erase(action->version());
    running_action_count_--;
    safe_version_.store(
        active_actions_.empty()
        ? (high_water_mark_ + 1)
        : *active_actions_.begin());
  }

  // Start executing all actions that have newly acquired all their locks.
  while (lm_.Ready(&action)) {
    if (action->single_replica() == false && action->create_new() == true && local_replica_ != action->origin() && actions_involved_replica_.find(action->distinct_id()) !=  actions_involved_replica_.end()) {
      uint64 this_machine = machine()->machine_id();
      uint64 machine_sent = store_->GetHeadMachine(this_machine);
      Header* header = new Header();
      header->set_from(this_machine);
      header->set_to(machine_sent);

      header->add_misc_int(action->distinct_id());
      header->add_misc_int(action->involved_machines());
      header->set_type(Header::RPC);
      header->set_app("blocklog");
      header->set_rpc("CREATE_NEW");

      Action* new_action = new Action();
      new_action->CopyFrom(*action);
      new_action->clear_client_machine();
      new_action->clear_client_channel();
      new_action->clear_origin();
      new_action->set_create_new(false);
      string* block = new string();
      new_action->SerializeToString(block);

      machine()->SendMessage(header, new MessageBuffer(Slice(*block)));

      actions_involved_replica_.erase(action->distinct_id());
    }
    
    store_->RunAsync(action, &completed_);
  }
}

