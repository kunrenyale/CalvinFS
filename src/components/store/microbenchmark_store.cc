// Author: Kun Ren <kun@cs.yale.edu>
//

#include "components/store/microbenchmark_store.h"

#include <glog/logging.h>
#include <leveldb/db.h>
#include <string>
#include "common/utils.h"
#include "components/store/btreestore.h"
#include "components/store/kvstore.h"
#include "components/store/microbenchmark_store.pb.h"

///////////////////   MicrobenchmarkStore Implementation   ////////////////////////

MicrobenchmarkStore::MicrobenchmarkStore(KVStore* store) {
  records_ = store;
}

MicrobenchmarkStore::MicrobenchmarkStore() {
  records_ = new BTreeStore();
}

MicrobenchmarkStore::~MicrobenchmarkStore() {
  delete records_;
}

bool MicrobenchmarkStore::IsLocal(const string& path) {
  return true;
}

void MicrobenchmarkStore::GetRWSets(Action* action) {
  action->clear_readset();
  action->clear_writeset();

  MicrobenchmarkAction::Type type =
      static_cast<MicrobenchmarkAction::Type>(action->action_type());

  if (type == MicrobenchmarkAction::EXECUTE) {
    MicrobenchmarkAction::ExecuteInput in;
    in.ParseFromString(action->input());
    for(uint64 i = 0; i < in.keys_size(); i++) {
      action->add_writeset(in.keys(i));
    }
    return;

  } else {
    LOG(FATAL) << "invalid action type";
  }
}

void MicrobenchmarkStore::Run(Action* action) {
  
}

void MicrobenchmarkStore::Run(Action* action) {

  // Should first create execution context
  /**ExecutionContext* context;
  if (machine_ == NULL) {
    context = new ExecutionContext(store_, action);
  } else {
    context =
        new DistributedExecutionContext(machine_, config_, store_, action);
  }**/
  
  MicrobenchmarkAction::Type type =
      static_cast<MicrobenchmarkAction::Type>(action->action_type());

  if (type == MicrobenchmarkAction::EXECUTE) {
    MicrobenchmarkAction::ExecuteInput in;
    in.ParseFromString(action->input());
    Execute(in);
    return;
  } else {
    LOG(FATAL) << "invalid action type";
  }
}

void MicrobenchmarkStore::Execute(const MicrobenchmarkAction::ExecuteInput& in) {
  for(uint64 i = 0; i < in.keys_size(); i++) {
    
  }
  
  
}




