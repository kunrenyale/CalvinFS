// Author: Alex Thomson (thomson@cs.yale.edu)
//

#ifndef CALVIN_COMPONENTS_SCHEDULER_SERIAL_SCHEDULER_H_
#define CALVIN_COMPONENTS_SCHEDULER_SERIAL_SCHEDULER_H_

#include <glog/logging.h>
#include <atomic>
#include "common/atomic.h"
#include "common/source.h"
#include "components/scheduler/scheduler.h"
#include "components/store/store_app.h"
#include "machine/app/app.h"
#include "machine/machine.h"
#include "proto/action.pb.h"

using std::atomic;

class SerialScheduler : public Scheduler {
 public:
  SerialScheduler() {}
  ~SerialScheduler() {}

  virtual void MainLoopBody() {
    Action* action;
    if (action_requests_->Get(&action)) {
      store_->Run(action);
      delete action;
    }
  }

 private:

  // DISALLOW_COPY_AND_ASSIGN
  SerialScheduler(const SerialScheduler&);
  SerialScheduler& operator=(const SerialScheduler&);
};

#endif  // CALVIN_COMPONENTS_SCHEDULER_SERIAL_SCHEDULER_H_

