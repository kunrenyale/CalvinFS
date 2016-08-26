// Author: Alex Thomson (thomson@cs.yale.edu)
//

#include "db/client.h"

#include "machine/app/app.h"

REGISTER_APP(MicroClient) {
  return new MicroClient();
}

