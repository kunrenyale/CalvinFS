// Author: Alex Thomson (thomson@cs.yale.edu)
//

#ifndef CALVIN_COMPONENTS_SCHEDULER_SCHEDULER_H_
#define CALVIN_COMPONENTS_SCHEDULER_SCHEDULER_H_

#include <atomic>
#include "common/source.h"
#include "machine/machine.h"
#include "machine/app/app.h"

class Action;
class Header;
class MessageBuffer;
class StoreApp;

class Scheduler : public App {
 public:
  Scheduler()
    : go_(true), going_(false), action_requests_(&no_actions_), store_(NULL) {
  }

  virtual ~Scheduler() {
    Stop();
  }

  // Scheduler subclasses typically only have to implement the MainLoopBody
  // function. It is called repeatedly by Start(). Note that the MainLoopBody
  // should NOT infinite loop.
  virtual void MainLoopBody() = 0;

  // Start() repeatedly executes the scheduler's MainLoopBody.
  virtual void Start() {
    Spin(0.01);
    going_ = true;
    while (go_.load()) {
      MainLoopBody();
    }
    going_ = false;
  }

  // Stop() causes Start() to immediately return.
  virtual void Stop() {
    go_ = false;
    while (going_.load()) {
      // Wait for main loop to stop.
    }
  }

  // Scheduler is an App subclass mainly to bind it to a machine and allow it
  // to implement Start()/Stop(). Scheduler subclasses typically don't receive
  // RPC requests directly, so a default (panicky) HandleMessage implementation
  // is provided.
  virtual void HandleMessage(Header* header, MessageBuffer* message) {
    LOG(FATAL) << "RPC request sent to Scheduler";
  }

  // Does NOT take ownership of '*requests'.
  void SetActionSource(Source<Action*>* requests) {
    action_requests_ = requests;
  }

  // Stops the Scheduler from taking on new actions from its action source.
  // Note that this does NOT cause the Scheduler to stop processing any
  // action(s) it had already accepted from the source, nor does it stop the
  // Scheduler's main loop. 
  void ClearActionSource() {
    action_requests_ = &no_actions_;
  }

  // Binds the scheduler to a particular store, which must be registered on
  // the same machine as a StoreApp under 'store_app_name'. Once set, the store
  // to which the scheduler is bound cannot be unset or changed.
  //
  // Requires: machine()->HasApp(store_app_name)
  void SetStore(const string& store_app_name) {
    CHECK(store_ == NULL);
    CHECK(machine()->GetApp(store_app_name) != NULL);
    store_ = reinterpret_cast<StoreApp*>(machine()->GetApp(store_app_name));
  }

  void SetParameters(int a, int b) {
    kMaxActiveActions = a;
    kMaxRunningActions = b;
  }

 protected:
  // True iff main thread SHOULD run.
  std::atomic<bool> go_;

  // True iff main thread IS running.
  std::atomic<bool> going_;

  // Default (empty) action request source.
  EmptySource<Action*> no_actions_;

  // Pointer to action request stream (points to 'no_actions_' by default).
  Source<Action*>* action_requests_;

  // Store on which this scheduler will schedule actions.
  StoreApp* store_;

  int kMaxActiveActions;
  int kMaxRunningActions;
};

#endif  // CALVIN_COMPONENTS_SCHEDULER__SCHEDULER_H_

