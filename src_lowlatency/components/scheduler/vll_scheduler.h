// Author: Kun Ren <kun@cs.yale.edu>
//

#ifndef CALVIN_COMPONENTS_SCHEDULER_VLL_SCHEDULER_H_
#define CALVIN_COMPONENTS_SCHEDULER_VLL_SCHEDULER_H_

#include <map>
#include <vector>
#include "common/atomic.h"
#include "machine/app/app.h"
#include "components/scheduler/scheduler.h"
#include "proto/action.pb.h"

using std::map;
using std::vector;

#define ARRAY_SIZE 819200

class VLLScheduler : public Scheduler {
 private:
  static const int MaxBlockedActions = 50;
 public:
  VLLScheduler() : safe_version_(1) {Cx.resize(ARRAY_SIZE, 0); Cs.resize(ARRAY_SIZE, 0);}
//  VLLScheduler() : safe_version_(1) {Cx(ARRAY_SIZE);}
  ~VLLScheduler() {}

  virtual uint64 SafeVersion() {
    return safe_version_;
  }
  virtual uint64 HighWaterMark() {
    return safe_version_;
  }

  virtual void MainLoopBody();
  
 private:
  uint64 safe_version_;
  int blocked_actions;
  
  vector<int> Cx;
  vector<int> Cs;
  
  // ActionQueue: order the actions by the order in which they request
  // their locks, So each action should go through the queue and will be deleted
  // when finished.
  map<uint64, Action*> ActionQueue;
  
  // Queue of completed actions.
  AtomicQueue<Action*> completed_;
};



#endif  // CALVIN_COMPONENTS_SCHEDULER_VLL_SCHEDULER_H_
