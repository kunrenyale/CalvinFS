// Author: Alexander Thomson (thomson@cs.yale.edu)
//

#include "machine/app/reporter.h"

#include "machine/app/app.h"

// Make Reporter startable.
REGISTER_APP(Reporter) {
  return new Reporter();
}
