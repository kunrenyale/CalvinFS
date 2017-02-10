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
  Action* action = NULL;

  // Start processing the next incoming action request.
  if (static_cast<int>(active_actions_.size()) < kMaxActiveActions &&
      running_action_count_ < kMaxRunningActions) {

    if (ready_actions.size() != 0) {
      action = ready_actions.front();
      ready_actions.pop();
    } else {
      action_requests_->Get(&action);
    }

    if (action != NULL) {
      high_water_mark_ = action->version();
      active_actions_.insert(action->version());
      int ungranted_requests = 0;

LOG(ERROR) << "Machine: "<<machine()->machine_id()<<":--Scheduler receive action:" << action->version()<<" distinct id is:"<<action->distinct_id()<<".  origin:"<<action->origin();
    
      if (action->wait_for_remaster_pros() == false && blocking_replica_ == true && blocking_replica_id_ == action->origin()) {
LOG(ERROR) << "Machine: "<<machine()->machine_id()<<":--Scheduler receive action:" << action->version()<<" distinct id is:"<<action->distinct_id()<<".  origin:"<<action->origin()<<"-- temporarily block the actions";
        // Temporarily block the actions from replica:blocking_replica_id_
        blocking_actions.push(action);
        return;
      } else if (action->wait_for_remaster_pros() == true && action->remaster_to() != local_replica_) {
LOG(ERROR) << "Machine: "<<machine()->machine_id()<<":--Scheduler receive action:" << action->version()<<" distinct id is:"<<action->distinct_id()<<".  origin:"<<action->origin()<<"-- check for pros actions";
        blocking_actions.push(action);
        // Check the mastership of the records without locking
        set<string> keys;
        bool can_execute_now = store_->CheckLocalMastership(action, keys);
        if (can_execute_now == false) {
LOG(ERROR) << "Machine: "<<machine()->machine_id()<<":--Scheduler receive action:" << action->version()<<" distinct id is:"<<action->distinct_id()<<".  origin:"<<action->origin()<<"-- check for pros actions(** check pros failed)";
          blocking_replica_ = true;
          blocking_replica_id_ = action->origin();

          // Put it into the queue and wait for the remaster action come
          waiting_actions_by_actionid[action->distinct_id()] = keys;
          for (auto it = keys.begin(); it != keys.end(); it++) {
            if (waiting_actions_by_key.find(*it) != waiting_actions_by_key.end()) {
              waiting_actions_by_key[*it].push_back(action);
            } else {
              vector<Action*> actions;
              actions.push_back(action);
              waiting_actions_by_key[*it] = actions;
            }
          }

          return ;
        } else if (action == blocking_actions.front()) {
          blocking_actions.pop();
        }
      }


      if (action->remaster() == true) {
LOG(ERROR) << "Machine: "<<machine()->machine_id()<<":--Scheduler receive action:" << action->version()<<" distinct id is:"<<action->distinct_id()<<".  origin:"<<action->origin()<<"-- received a remaster action";
        // Request the lock
        for (int i = 0; i < action->remastered_keys_size(); i++) {
          if (store_->IsLocal(action->remastered_keys(i))) {
LOG(ERROR) << "Machine: "<<machine()->machine_id()<<":--Scheduler receive action:" << action->version()<<" distinct id is:"<<action->distinct_id()<<".  origin:"<<action->origin()<<"### key:"<<action->remastered_keys(i);
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
  }

  // Process all actions that have finished running.
  while (completed_.Pop(&action)) {
//LOG(ERROR) << "Machine: "<<machine()->machine_id()<< " --Scheduler will release locks， action:"<<action->distinct_id();
    if (action->remaster() == true) {
      // Release the locks and wake up the waiting actions.
      for (int i = 0; i < action->remastered_keys_size(); i++) {
        if (store_->IsLocal(action->remastered_keys(i))) {
LOG(ERROR) << "Machine: "<<machine()->machine_id()<< " --Scheduler: remaster action completed， action:"<<action->distinct_id()<<" so can wake up key: "<<action->remastered_keys(i);
          vector<Action*> blocked_actions = waiting_actions_by_key[action->remastered_keys(i)];

          for (auto it = blocked_actions.begin(); it != blocked_actions.end(); it++) {
            Action* a = *it;
            (waiting_actions_by_actionid[a->distinct_id()]).erase(action->remastered_keys(i));

            if ((waiting_actions_by_actionid[a->distinct_id()]).size() == 0) {
LOG(ERROR) << "Machine: "<<machine()->machine_id()<< " --Scheduler: remaster action completed， action:"<<action->distinct_id()<<" so can wake up key: "<<action->remastered_keys(i)<<"   now action can be run, id:"<<a->distinct_id();
              a->set_wait_for_remaster_pros(false);
              waiting_actions_by_actionid.erase(a->distinct_id());

              CHECK(blocking_actions.front() == a);
              ready_actions.push(a);
              blocking_actions.pop();

              while (!blocking_actions.empty() && blocking_actions.front()->wait_for_remaster_pros() == false) {
                ready_actions.push(blocking_actions.front());
                blocking_actions.pop();
              }

              
              if (blocking_actions.empty()) {
                blocking_replica_ = false;
              }

            }
          }

          waiting_actions_by_key.erase(action->remastered_keys(i));

          lm_.Release(action, action->remastered_keys(i));
        }
      }
LOG(ERROR) << "Machine: "<<machine()->machine_id()<<":--Scheduler finish running a remaster action:  distinct id is:"<<action->distinct_id()<<".  origin:"<<action->origin();

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

LOG(ERROR) << "Machine: "<<machine()->machine_id()<<":--Scheduler finish running an action:  distinct id is:"<<action->distinct_id()<<".  origin:"<<action->origin();

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

