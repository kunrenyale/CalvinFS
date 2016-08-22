// Author: Alex Thomson (thomson@cs.yale.edu)
//

#ifndef CALVIN_COMPONENTS_STORE_STORE_APP_H_
#define CALVIN_COMPONENTS_STORE_STORE_APP_H_

#include "machine/app/app.h"
#include "machine/message_buffer.h"
#include "components/store/store.h"
#include "proto/action.pb.h"

template <typename t>
class AtomicQueue;

class StoreApp : public App {
 public:
  // Takes ownership of '*store'.
  explicit StoreApp(Store* store) : store_(store) {}
  virtual ~StoreApp();
  virtual void HandleMessage(Header* header, MessageBuffer* message);

  // Synchronous access to the underlying store.
  void GetRWSets(Action* action);
  virtual void Run(Action* action);

  // Run the action in a background thread. The action is pushed to '*queue'
  // after it is completed.
  void RunAsync(Action* action, AtomicQueue<Action*>* queue);

  // TODO(agt): This is currently needed for testing, but code should be
  //            restructured to remove this.
  Store* store() { return store_; }

 protected:
  StoreApp() {}
  void HandleMessageBase(Header* header, MessageBuffer* message);

  // Main store.
  Store* store_;
};

#endif  // CALVIN_COMPONENTS_STORE_STORE_APP_H_

