// Author: Kun Ren <kun@cs.yale.edu>
//

#include "components/scheduler/vll_scheduler.h"

#include <glog/logging.h>
#include <set>
#include <string>
#include <vector>
#include <bitset>
#include "common/source.h"
#include "common/types.h"
#include "common/utils.h"
#include "components/scheduler/scheduler.h"
#include "components/store/store_app.h"
#include "proto/action.pb.h"

using std::set;
using std::string;
using std::vector;
using std::bitset;

REGISTER_APP(VLLScheduler) {
  return new VLLScheduler();
}

void VLLScheduler::MainLoopBody() {
  Action* action;
  uint64 hash_index;
  bitset<ARRAY_SIZE> Dx;
  bitset<ARRAY_SIZE> Ds;

  // Start processing the next incoming action request.
  if (blocked_actions < MaxBlockedActions &&
      action_requests_->Get(&action)) {
    // BeginTransaction
    action->set_action_status(Action::FREE);
    
    // Request write locks.
    for (int i = 0; i < action->writeset_size(); i++) {
      if (store_->IsLocal(action->writeset(i))) {
        hash_index = FNVModHash(action->writeset(i)) % ARRAY_SIZE;
        Cx[hash_index]++;
        if (Cx[hash_index] > 1 || Cs[hash_index] > 0) {
          action->set_action_status(Action::BLOCKED);
        }
      }
    }
    
    // Request read locks.
    for (int i = 0; i < action->readset_size(); i++) {
      if (store_->IsLocal(action->readset(i))) {
        hash_index = FNVModHash(action->readset(i)) % ARRAY_SIZE;
        Cs[hash_index]++;
        if (Cx[hash_index] > 0) {
          action->set_action_status(Action::BLOCKED);
        }
      }
    }
    
    ActionQueue.insert(std::pair<uint64, Action*>(action->version(), action));

    // If all read and write locks were immediately acquired, this action
    // is ready to run.
    if (action->action_status() == Action::FREE) {
      store_->RunAsync(action, &completed_);
    }

    // Process all actions that have finished running.
    while (completed_.Pop(&action)) {
      // Release locks and removes the action from TxnQueue;
      
      // Release write locks.
      for (int i = 0; i < action->writeset_size(); i++) {
        hash_index = FNVModHash(action->writeset(i)) % ARRAY_SIZE;
        Cx[hash_index]--;
      }
    
      // Release read locks.
      for (int i = 0; i < action->readset_size(); i++) {
        hash_index = FNVModHash(action->readset(i)) % ARRAY_SIZE;
        Cs[hash_index]--;
      }
      
      // Remove the action from ActionQueue;
      std::map<uint64, Action*>::iterator it;
      it = ActionQueue.find(action->version());
      ActionQueue.erase(it);
    }
    
    // If the first action in the ActionQueue is BLOCKED, execute it.
    action = ActionQueue.begin()->second;
    if (action->action_status() == Action::BLOCKED) {
      action->set_action_status(Action::FREE);
      store_->RunAsync(action, &completed_);
    }
  } else {
    // Run SCA;
    
    // Create our 100KB bit arrays
    Dx.reset();
    Ds.reset();
    
    for (std::map<uint64, Action*>::iterator it = ActionQueue.begin();
         it != ActionQueue.end(); ++it) {
      action = it->second;
      
      // Check whether the Blocked actions can safely be run
      if (action->action_status() == Action::BLOCKED) {
        bool success = true;
        
        // Check for conflicts in WriteSet
        for (int i = 0; i < action->writeset_size(); i++) {
          hash_index = FNVModHash(action->writeset(i)) % ARRAY_SIZE;
          if (Dx[hash_index] == 1 || Ds[hash_index] == 1) {
            success = false;
          }
          Dx[hash_index] = 1;
        }
        
        // Check for conflicts in ReadSet
        for (int i = 0; i < action->readset_size(); i++) {
          hash_index = FNVModHash(action->readset(i)) % ARRAY_SIZE;
          if (Dx[hash_index] == 1) {
            success = false;
          }
          Ds[hash_index] = 1;
        }
        
        if (success == true) {
          store_->RunAsync(action, &completed_); 
        }
        
      } else {
        // If the transaction is free, just mark the bit-array
        for (int i = 0; i < action->writeset_size(); i++) {
          hash_index = FNVModHash(action->writeset(i)) % ARRAY_SIZE;
          Dx[hash_index] = 1;
        }
        for (int i = 0; i < action->writeset_size(); i++) {
          hash_index = FNVModHash(action->writeset(i)) % ARRAY_SIZE;
          Ds[hash_index] = 1;
        }
      }
    }    
  } 
}





