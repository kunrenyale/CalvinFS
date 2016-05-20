// Author: Kun Ren <kun@cs.yale.edu>
//

#include "machine/app/app.h"

class TestAddApp : public App {
 public:
  explicit TestAddApp(int c) : counter_(c) {}
  virtual ~TestAddApp() {}
  virtual void HandleMessage(Header* header, MessageBuffer* message) {
    counter_++;
  }
  int counter() { return counter_; }

 private:
  int counter_;
};

REGISTER_APP(TestAddApp) {
  return new TestAddApp(10);
}


