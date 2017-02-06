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

  // Start processing the next incoming action request.
  if (static_cast<int>(active_actions_.size()) < kMaxActiveActions &&
      running_action_count_ < kMaxRunningActions) {

    if (ready_actions.size() != 0) {
      action = ready_actions.front();
      ready_actions.pop();
    } else if (!action_requests_->Get(&action)) {
      return
    }

    high_water_mark_ = action->version();
    active_actions_.insert(action->version());
    int ungranted_requests = 0;

    if (action->wait_for_remaster_pros() == true && action->remaster_to() != local_replica_) {
      // Check the mastership of the records without locking
      set<string> keys;
      bool can_execute_now = store_->CheckLocalMastership(action, keys);
      if (can_execute_now == false) {
        // Put it into the queue and wait for the remaster action come
        waiting_actions_by_actionid[action->distinct_id()] = keys;
        for (auto it = keys.begin(); it != keys.end(); it++) {
          if (waiting_actions_by_key.find(*it) != waiting_actions_by_key.end()) {
            waiting_actions_by_key[*it].insert(action);
          } else {
            set<Action*> actions;
            actions.insert(action);
            waiting_actions_by_key[*it] = actions;
          }
        }
        
      }
    }


    if (action->remaster() == true) {
      // Request the lock
      for (int i = 0; i < action->remastered_keys_size(); i++) {
        if (store_->IsLocal(action->remastered_keys(i))) {
          if (!lm_.WriteLock(action, action->remastered_keys(i))) {
            ungranted_requests++;
          }
        }
      }
    } else {
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

      running_action_count_++;
      store_->RunAsync(action, &completed_);
    } 

  }

  // Process all actions that have finished running.
  while (completed_.Pop(&action)) {

    if (action->remaster() == true) {
      // Release the locks and wake up the waiting actions.
      for (int i = 0; i < action->remastered_keys_size(); i++) {
        if (store_->IsLocal(action->remastered_keys(i))) {
          set<Action*> blocked_actions = waiting_actions_by_key[action->remastered_keys(i)];   
          for (auto it = blocked_actions.begin(); it != blocked_actions.end(); it++) {
            Action* a = *it;
            (waiting_actions_by_actionid[a->distinct_id()]).erase(action->remastered_keys(i));
            if ((waiting_actions_by_actionid[a->distinct_id()]).size() == 0) {
              a->set_wait_for_remaster_pros(false);
              ready_actions->push_back(a);
              waiting_actions_by_actionid.erase(a->distinct_id());
            }
          }

          waiting_actions_by_key.erase(action->remastered_keys(i));

          lm_.Release(action, action->remastered_keys(i));
        }
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
    }

    active_actions_.erase(action->version());
    running_action_count_--;
    safe_version_.store(
        active_actions_.empty()
        ? (high_water_mark_ + 1)
        : *active_actions_.begin());
  }

  // Start executing all actions that have newly acquired all their locks.
  while (lm_.Ready(&action)) {   
    running_action_count_++;
    store_->RunAsync(action, &completed_);
  }
}

