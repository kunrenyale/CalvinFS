// Author: Kun Ren <kun@cs.yale.edu>
//

#include "components/store/tpcc_store.h"

#include <glog/logging.h>
#include <leveldb/db.h>
#include <string>
#include "common/utils.h"
#include "components/store/btreestore.h"
#include "components/store/kvstore.h"
#include "components/store/tpcc_store.pb.h"

///////////////////   MicrobenchmarkStore Implementation   ////////////////////////

TpccStore::TpccStore(KVStore* store) {
  records_ = store;
}

TpccStore::TpccStore() {
  records_ = new BTreeStore();
}

TpccStore::~TpccStore() {
  delete records_;
}

bool TpccStore::IsLocal(const string& path) {
  return true;
}

void TpccStore::GetRWSets(Action* action) {
  action->clear_readset();
  action->clear_writeset();

  TpccAction::Type type =
      static_cast<TpccAction::Type>(action->action_type());

  if (type == TpccAction::NEW_ORDER) {
    TpccAction::ExecuteInput in;
    in.ParseFromString(action->input());
    for(uint64 i = 0; i < in.keys_size(); i++) {
      action->add_writeset(in.keys(i));
    }
    return;

  } else if (type == TpccAction::PAYMENT) {
  
  } else {
    LOG(FATAL) << "invalid action type";
  }
}

void TpccStore::Run(Action* action) {
  TpccAction::Type type =
      static_cast<TpccAction::Type>(action->action_type());

  if (type == TpccAction::NEW_ORDER) {
    TpccAction::NewOrderInput in;
    in.ParseFromString(action->input());
    NewOrder(in);
    return;
  } else if (type == TpccAction::PAYMENT) {
    TpccAction::PaymentInput in;
    in.ParseFromString(action->input());
    Payment(in);
  } else {
    LOG(FATAL) << "invalid action type";
  }
}

void TpccStore::NewOrder(const TpccAction::ExecuteInput& in) {
  for(uint64 i = 0; i < in.keys_size(); i++) {
    
  }
  
  
}

void TpccStore::Payment(const TpccAction::ExecuteInput& in) {
  for(uint64 i = 0; i < in.keys_size(); i++) {
    
  }
  
  
}




