// Author: Alex Thomson (thomson@cs.yale.edu)
//

#ifndef CALVIN_DB_CLIENT_H_
#define CALVIN_DB_CLIENT_H_

#include <glog/logging.h>
#include <google/protobuf/message.h>
#include <atomic>
#include <string>
#include "common/atomic.h"
#include "common/source.h"
#include "common/types.h"
#include "common/utils.h"
#include "components/scheduler/scheduler.h"
#include "components/store/mvstore_app.h"
#include "machine/app/app.h"
#include "machine/machine.h"
#include "machine/message_buffer.h"
#include "proto/header.pb.h"
#include "proto/action.pb.h"

using std::atomic;
using std::string;

class MicroClient : public App {
 public:
  MicroClient() : go_(true), going_(false), source_(NULL), queue_(NULL) {}
  MicroClient(Source<Action*>* s, QueueSource<Action*>* q)
    : go_(true), going_(false), source_(s), queue_(q) {
  }

  void Setup(Source<Action*>* s, QueueSource<Action*>* q) {
    queue_ = q;
    source_ = s;
  }

  virtual ~MicroClient() {
    Stop();
  }

  virtual void Start() {
    going_ = true;
    Action* action;
    while (go_) {
      if (queue_ != NULL && source_ != NULL) {
        if (queue_->Size() >= 1000) {
          Spin(0.001);
        } else {
          for (int i = 0; i < 1000; i++) {
            if (source_->Get(&action)) {
              queue_->Add(action);
            }
          }
        }
      }
    }
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

  Source<Action*>* source_;
  QueueSource<Action*>* queue_;
};

#endif  // CALVIN_DB_CLIENT_H_

