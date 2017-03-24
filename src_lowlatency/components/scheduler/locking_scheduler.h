// Author: Alex Thomson (thomson@cs.yale.edu)
// Author: Kun  Ren (kun.ren@yale.edu)
//

#ifndef CALVIN_COMPONENTS_SCHEDULER_LOCKING_SCHEDULER_H_
#define CALVIN_COMPONENTS_SCHEDULER_LOCKING_SCHEDULER_H_

#include <atomic>
#include <set>
#include "common/atomic.h"
#include "machine/app/app.h"
#include "components/scheduler/lock_manager.h"
#include "components/scheduler/scheduler.h"
#include "proto/action.pb.h"

using std::atomic;
using std::set;

class LockingScheduler : public Scheduler {

 public:
  LockingScheduler()
      : running_action_count_(0), throughput_(0), start_time_(GetTime()) {
  }
  virtual ~LockingScheduler() {}

  virtual void MainLoopBody();

 private:
  // Lock manager.
  LockManager lm_;

  // Queue of completed actions.
  AtomicQueue<Action*> completed_;

  // Track active (possibly blocked) and running actions.
  std::set<uint64> active_actions_;
  int running_action_count_;

  // calculate transaction throughput
  uint64 throughput_;

  double start_time_;

  // DISALLOW_COPY_AND_ASSIGN
  LockingScheduler(const LockingScheduler&);
  LockingScheduler& operator=(const LockingScheduler&);
};

#endif  // CALVIN_COMPONENTS_SCHEDULER_LOCKING_SCHEDULER_H_

