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

  Action* action = NULL;

  // Process all actions that have finished running.
  while (completed_.Pop(&action)) {
    if (action->remaster() == true) {
      // Release the locks and wake up the waiting actions.
      for (int i = 0; i < action->remastered_keys_size(); i++) {
        KeyMasterEntry map_entry = action->remastered_keys(i);
        pair <string, uint64> key_info = make_pair(map_entry.key(), map_entry.counter()+1);

        if (map_entry.master() != action->remaster_to() && waiting_actions_by_key_.find(key_info) != waiting_actions_by_key_.end()) {
          vector<Action*> blocked_actions = waiting_actions_by_key_[key_info];

          for (auto it = blocked_actions.begin(); it != blocked_actions.end(); it++) {
            Action* a = *it;
            (waiting_actions_by_actionid_[a->distinct_id()]).erase(key_info);

            if ((waiting_actions_by_actionid_[a->distinct_id()]).size() == 0) {
              a->set_wait_for_remaster_pros(false);
              waiting_actions_by_actionid_.erase(a->distinct_id());

              if (blocking_actions_[a->origin()].front() == a) {
                ready_actions_.push(a);
                blocking_actions_[a->origin()].pop();

                while (!blocking_actions_[a->origin()].empty() && blocking_actions_[a->origin()].front()->wait_for_remaster_pros() == false) {
                  ready_actions_.push(blocking_actions_[a->origin()].front());
                  blocking_actions_[a->origin()].pop();
                }
            
                if (blocking_actions_[a->origin()].empty()) {
                  blocking_replica_id_.erase(a->origin());
                }
              }

            }
          }

          waiting_actions_by_key_.erase(key_info);
       }

       lm_.Release(action, map_entry.key());
    }

  } else {
    set<string> writeset;

    // Release write locks. 
    for (int i = 0; i < action->writeset_size(); i++) {
      if (store_->IsLocal(action->writeset(i))) {
        writeset.insert(action->writeset(i));
        lm_.Release(action, action->writeset(i));
      }
    }

    // Release read locks.
    for (int i = 0; i < action->readset_size(); i++) {
      if (store_->IsLocal(action->readset(i))) {
        if (writeset.count(action->readset(i)) == 0) {
          lm_.Release(action, action->readset(i));
        }
      }
    }

    /**if (action->has_client_machine() && rand() % action->involved_machines() == 0) {
      throughput_++;
    }

    if (start_measure_ == false) {
      start_measure_ = true;
      start_time_ = GetTime();
    } **/

  }

  active_actions_.erase(action->version());
  running_action_count_--;
  }

  // Start executing all actions that have newly acquired all their locks.
  while (lm_.Ready(&action)) {
    running_action_count_++;
    store_->RunAsync(action, &completed_);
  }

 /** double current_time = GetTime();
  if (start_measure_ == true && current_time - start_time_ > 0.2 && throughput_ > 0) {
    LOG(ERROR) << "[" << machine()->machine_id() << "] "<< "Scheduler:  Throughput is: "<<throughput_/(current_time-start_time_);
    throughput_ = 0;
    start_time_ = current_time;
  }**/


  // Start processing the next incoming action request.
  if (static_cast<int>(active_actions_.size()) < kMaxActiveActions &&
      running_action_count_ < kMaxRunningActions) {

    if (ready_actions_.size() != 0) {
      action = ready_actions_.front();
      ready_actions_.pop();
    } else if (!action_requests_->Get(&action)){
      return;
    }

    active_actions_.insert(action->version());
    int ungranted_requests = 0;
    
    if (action->origin() != local_replica_) {
      blocking_actions_[action->origin()].push(action);
      action->set_wait_for_remaster_pros(true);
      // Check the mastership of the records without locking
      set<pair<string,uint64>> keys;
      bool can_execute_now = store_->CheckLocalMastership(action, keys);
      if (can_execute_now == false) {
        blocking_replica_id_.insert(action->origin());

        // Put it into the queue and wait for the remaster action come
        waiting_actions_by_actionid_[action->distinct_id()] = keys;
        for (auto it = keys.begin(); it != keys.end(); it++) {
          waiting_actions_by_key_[*it].push_back(action);
        }

        return ;
      } else {
        action->set_wait_for_remaster_pros(false);
        if (action == blocking_actions_[action->origin()].front()) {
          blocking_actions_[action->origin()].pop();
        } else {
          return ;
        }
      } 
    }


    if (action->remaster() == true) {
      // Request the lock
      for (int i = 0; i < action->remastered_keys_size(); i++) {
        KeyMasterEntry entry = action->remastered_keys(i);
        if (!lm_.WriteLock(action, entry.key())) {
          ungranted_requests++;
        }
      }
    } else {
      // Request write locks. Track requests so we can check that we don't re-request any as read locks.
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
        // Avoid re-requesting shared locks if an exclusive lock is already requested.
        if (store_->IsLocal(action->readset(i))) {
          if (writeset.count(action->readset(i)) == 0) {
            if (!lm_.ReadLock(action, action->readset(i))) {
              ungranted_requests++;
            }
          }
        }
      }

    }

    // If all read and write locks were immediately acquired, this action is ready to run.
    if (ungranted_requests == 0) {
      running_action_count_++;
      store_->RunAsync(action, &completed_);
    } 
  }

}

