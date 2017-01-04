// Author: Alex Thomson (thomson@cs.yale.edu)
//

#include "fs/block_log.h"

#include <set>
#include <vector>

#include "common/types.h"
#include "components/log/log.h"
#include "components/log/log_app.h"
#include "fs/block_store.h"
#include "machine/app/app.h"
#include "proto/start_app.pb.h"

using std::vector;

REGISTER_APP(BlockLogApp) {
  return new BlockLogApp();
}

