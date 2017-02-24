// Author: Alex Thomson (thomson@cs.yale.edu)
// Author: Kun  Ren (kun.ren@yale.edu)
//

#ifndef CALVIN_COMPONENTS_SCHEDULER_LOCKING_SCHEDULER_H_
#define CALVIN_COMPONENTS_SCHEDULER_LOCKING_SCHEDULER_H_

#include <atomic>
#include <set>
#include <queue>
#include <map>
#include <vector>
#include "common/atomic.h"
#include "machine/app/app.h"
#include "components/scheduler/lock_manager.h"
#include "components/scheduler/scheduler.h"
#include "proto/action.pb.h"

using std::atomic;
using std::set;
using std::queue;
using std::map;
using std::vector;

class LockingScheduler : public Scheduler {

 public:
  LockingScheduler()
      : running_action_count_(0), high_water_mark_(0), safe_version_(1) {
  }
  virtual ~LockingScheduler() {}

  virtual uint64 SafeVersion() {
    return safe_version_.load();
  }
  virtual uint64 HighWaterMark() {
    return high_water_mark_;
  }

  virtual void MainLoopBody();

 private:
  // Lock manager.
  LockManager lm_;
 
  // Queue of completed actions.
  AtomicQueue<Action*> completed_;

  // Track active (possibly blocked) and running actions.
  std::set<uint64> active_actions_;
  int running_action_count_;

  // Version of newest action.
  uint64 high_water_mark_;

  atomic<uint64> safe_version_;

  map<pair<string, uint64>, vector<Action*>> waiting_actions_by_key_;
  map<uint64, set<pair<string, uint64>>> waiting_actions_by_actionid_;

  queue<Action*> ready_actions_;

  map<uint32, queue<Action*>> blocking_actions_;
 
  set<uint32> blocking_replica_id_;

  // DISALLOW_COPY_AND_ASSIGN
  LockingScheduler(const LockingScheduler&);
  LockingScheduler& operator=(const LockingScheduler&);
};

#endif  // CALVIN_COMPONENTS_SCHEDULER_LOCKING_SCHEDULER_H_

