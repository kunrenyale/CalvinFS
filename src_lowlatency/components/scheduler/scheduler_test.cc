// Author: Alexander Thomson (thomson@cs.yale.edu)
//

#include "components/scheduler/scheduler.h"
#include "components/scheduler/serial_scheduler.h"
#include "components/scheduler/locking_scheduler.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "common/source.h"
#include "machine/cluster_config.h"
#include "machine/machine.h"
#include "proto/action.pb.h"

class SchedulerTest {
 public:
  // Argument is string name of scheduler class.
  explicit SchedulerTest(const string& scheduler) {
    // Start store app.
    StartAppProto sap;
    sap.add_participants(0);
    sap.set_app("BTreeStoreApp");
    sap.set_app_name("store");
    m_.AddApp(sap);

    // Start scheduler app.
    sap.set_app(scheduler);
    sap.set_app_name("scheduler");
    m_.AddApp(sap);

    // Bind scheduler to store.
    s_ = reinterpret_cast<Scheduler*>(m_.GetApp("scheduler"));
    s_->SetStore("store", 0);
  }

  void CannotResetStore() {
    ASSERT_DEATH({ s_->SetStore("foo", 0); }, "");
  }

  // TODO(agt): More tests!

 private:
  Machine m_;
  Scheduler* s_;
};

#define SCHEDULER_TEST(SCHEDULER_TYPE,TEST_METHOD) \
TEST(SCHEDULER_TYPE##Test, TEST_METHOD) { \
  SchedulerTest(#SCHEDULER_TYPE).TEST_METHOD(); \
}

SCHEDULER_TEST(SerialScheduler, CannotResetStore)
SCHEDULER_TEST(LockingScheduler, CannotResetStore)

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

