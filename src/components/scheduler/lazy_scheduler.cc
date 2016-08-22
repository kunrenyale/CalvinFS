// Author: Alex Thomson (thomson@cs.yale.edu)
//

#include "components/scheduler/lazy_scheduler.h"

#include "machine/app/app.h"

REGISTER_APP(LazyScheduler) {
  return new LazyScheduler();
}

