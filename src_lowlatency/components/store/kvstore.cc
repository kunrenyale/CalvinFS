// Author: Alexander Thomson <thomson@cs.yale.edu>
//

#include <glog/logging.h>
#include <string>
#include "btree/btree_map.h"
#include "common/utils.h"
#include "components/store/kvstore.h"
#include "components/store/kvstore.pb.h"
#include "proto/action.pb.h"

void KVStore::GetRWSets(Action* action) {
  action->clear_readset();
  action->clear_writeset();

  KVStoreAction::Type type =
      static_cast<KVStoreAction::Type>(action->action_type());

  if (type == KVStoreAction::EXISTS) {
    KVStoreAction::ExistsInput in;
    in.ParseFromString(action->input());
    action->add_readset(in.key());
    return;

  } else if (type == KVStoreAction::GET) {
    KVStoreAction::GetInput in;
    in.ParseFromString(action->input());
    action->add_readset(in.key());
    return;

  } else if (type == KVStoreAction::PUT) {
    KVStoreAction::PutInput in;
    in.ParseFromString(action->input());
    action->add_writeset(in.key());
    return;

  } else if (type == KVStoreAction::DELETE) {
    KVStoreAction::DeleteInput in;
    in.ParseFromString(action->input());
    action->add_writeset(in.key());
    return;

  } else {
    LOG(FATAL) << "invalid action type";
  }
}

bool KVStore::IsLocal(const string& path) {
  return true;
}

uint32 KVStore::LookupReplicaByDir(string dir) {
  return 0;
}

uint64 KVStore::GetHeadMachine(uint64 machine_id) {
  return 0;
}

uint32 KVStore::LocalReplica() {
  return -1;
}


void KVStore::Run(Action* action) {
  KVStoreAction::Type type =
      static_cast<KVStoreAction::Type>(action->action_type());

  if (type == KVStoreAction::EXISTS) {
    KVStoreAction::ExistsInput in;
    KVStoreAction::ExistsOutput out;
    in.ParseFromString(action->input());
    out.set_exists(Exists(in.key()));
    out.SerializeToString(action->mutable_output());
    return;

  } else if (type == KVStoreAction::GET) {
    KVStoreAction::GetInput in;
    KVStoreAction::GetOutput out;
    in.ParseFromString(action->input());
    action->add_readset(in.key());
    out.set_exists(Get(in.key(), out.mutable_value()));
    if (!out.exists()) {
      out.clear_value();
    }
    out.SerializeToString(action->mutable_output());
    return;

  } else if (type == KVStoreAction::PUT) {
    KVStoreAction::PutInput in;
    in.ParseFromString(action->input());
    Put(in.key(), in.value());
    return;

  } else if (type == KVStoreAction::DELETE) {
    KVStoreAction::DeleteInput in;
    in.ParseFromString(action->input());
    Delete(in.key());
    return;

  } else {
    LOG(FATAL) << "invalid action type";
  }
}

