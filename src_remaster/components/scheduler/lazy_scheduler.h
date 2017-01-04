// Author: Alex Thomson (thomson@cs.yale.edu)
//

#ifndef CALVIN_COMPONENTS_SCHEDULER_LAZY_SCHEDULER_H_
#define CALVIN_COMPONENTS_SCHEDULER_LAZY_SCHEDULER_H_

#include <glog/logging.h>
#include <google/protobuf/message.h>
#include <atomic>
#include <string>
#include "common/atomic.h"
#include "common/source.h"
#include "common/types.h"
#include "common/utils.h"
#include "machine/app/app.h"
#include "machine/machine.h"
#include "machine/message_buffer.h"
#include "components/scheduler/scheduler.h"
#include "components/store/mvstore_app.h"
#include "proto/header.pb.h"
#include "proto/action.pb.h"

using std::atomic;
using std::string;

class LazyScheduler : public Scheduler {
 public:
  LazyScheduler()
      : go_(true), going_(false), store_name_("store"), store_(NULL) {
  }

  virtual ~LazyScheduler() {
    Stop();
  }

  virtual void Start() {
    store_ = reinterpret_cast<MVStoreApp*>(machine()->GetApp(store_name_));

    going_ = true;
    Action* action;
    while (go_) {
      // Get next action that appears in the log.
      if (action_requests_->Get(&action)) {
        store_->RunPreExec(action);

        // Only stickify if there was no preexec error.
        if (!action->preexec_error()) {
          // Hack to remove district record from new order stickies since it was
          // already incremented in PreExec phase.
          if (action->action_type() == "NO") {
            action->mutable_readset()->RemoveLast();
            action->mutable_writeset()->RemoveLast();
          }

          store_->Stickify(action);

          // If execution cannot be deferred. Run txn in background thread.
          if (action->run_strict()) {
            store_->StartEval(action->version());
          }
        }
      }
    }

    // Update current state.
    going_ = false;
  }

  virtual void Stop() {
    go_ = false;
    // Wait for main loop to stop.
    while (going_) {
      Noop<bool>(going_);
    }
  }

  virtual void HandleMessage(Header* header, MessageBuffer* message) {}

 private:
  // True iff main thread SHOULD run.
  bool go_;

  // True iff main thread IS running.
  bool going_;

  // Name of store app.
  string store_name_;

  // Backend on which to apply actions.
  MVStoreApp* store_;

  // DISALLOW_COPY_AND_ASSIGN
  LazyScheduler(const LazyScheduler&);
  LazyScheduler& operator=(const LazyScheduler&);
};

#endif  // CALVIN_COMPONENTS_SCHEDULER_LAZY_SCHEDULER_H_

