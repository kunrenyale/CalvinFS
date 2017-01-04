// Author: Alex Thomson (thomson@cs.yale.edu)
//

#include "components/scheduler/serial_scheduler.h"

#include "machine/app/app.h"

REGISTER_APP(SerialScheduler) {
  return new SerialScheduler();
}

