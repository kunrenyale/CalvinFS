// Author: Kun Ren <kun@cs.yale.edu>
//

#include "components/store/hybrid_versioned_kvstore.h"

#include <string>
#include "common/types.h"
#include "common/utils.h"
#include "components/store/versioned_kvstore.h"
#include "components/store/btreestore.h"
#include "components/store/leveldbstore.h"
#include "components/store/kvstore.h"
#include "components/store/versioned_kvstore.pb.h"
#include "proto/action.pb.h"

using std::string;

static const uint64 kDeletedFlag = 1;
unsigned char GetByte(uint64 x, uint32 k);
void SetByte(uint64* x, uint32 k, unsigned char c);
void AppendVersion(string* key, uint64 version, uint64 flags = 0);
uint64 ParseVersion(const Slice& versioned_key, uint64* flags = NULL);
Slice StripVersion(const Slice& versioned_key);

///////////////////   HybridVersionedKVStore Implementation   /////////////////

HybridVersionedKVStore::HybridVersionedKVStore() {
  current_substore_ = new BTreeStore();
  old_substore_ = new VersionedKVStore(new LevelDBStore());
  delay_queue = new DelayQueue<string>(10);
}

HybridVersionedKVStore::~HybridVersionedKVStore() {
  delete current_substore_;
  delete old_substore_;
  delete delay_queue;
}

bool HybridVersionedKVStore::IsLocal(const string& path) {
  return true;
}

uint32 HybridVersionedKVStore::LookupReplicaByDir(string dir) {
  return 0;
}

uint64 HybridVersionedKVStore::GetHeadMachine(uint64 machine_id) {
  return 0;
}

uint32 HybridVersionedKVStore::LocalReplica() {
  return -1;
}

void HybridVersionedKVStore::GetRWSets(Action* action) {
  action->clear_readset();
  action->clear_writeset();

  VersionedKVStoreAction::Type type =
      static_cast<VersionedKVStoreAction::Type>(action->action_type());

  if (type == VersionedKVStoreAction::EXISTS) {
    VersionedKVStoreAction::ExistsInput in;
    in.ParseFromString(action->input());
    action->add_readset(in.key());
    return;

  } else if (type == VersionedKVStoreAction::GET) {
    VersionedKVStoreAction::GetInput in;
    in.ParseFromString(action->input());
    action->add_readset(in.key());
    return;

  } else if (type == VersionedKVStoreAction::PUT) {
    VersionedKVStoreAction::PutInput in;
    in.ParseFromString(action->input());
    action->add_writeset(in.key());
    return;

  } else if (type == VersionedKVStoreAction::DELETE) {
    VersionedKVStoreAction::DeleteInput in;
    in.ParseFromString(action->input());
    action->add_writeset(in.key());
    return;

  } else if (type == VersionedKVStoreAction::GET_VERSION) {
    VersionedKVStoreAction::GetVersionInput in;
    in.ParseFromString(action->input());
    action->add_readset(in.key());
    return;

  } else {
    LOG(FATAL) << "invalid action type";
  }
}

void HybridVersionedKVStore::Run(Action* action) {
  VersionedKVStoreAction::Type type =
      static_cast<VersionedKVStoreAction::Type>(action->action_type());

  if (type == VersionedKVStoreAction::EXISTS) {
    VersionedKVStoreAction::ExistsInput in;
    VersionedKVStoreAction::ExistsOutput out;
    in.ParseFromString(action->input());
    out.set_exists(Exists(in.key(), action->version()));
    out.SerializeToString(action->mutable_output());
    return;

  } else if (type == VersionedKVStoreAction::GET) {
    VersionedKVStoreAction::GetInput in;
    VersionedKVStoreAction::GetOutput out;
    in.ParseFromString(action->input());
    action->add_readset(in.key());
    out.set_exists(Get(in.key(), action->version(), out.mutable_value()));
    if (!out.exists()) {
      out.clear_value();
    }
    out.SerializeToString(action->mutable_output());
    return;

  } else if (type == VersionedKVStoreAction::PUT) {
    VersionedKVStoreAction::PutInput in;
    in.ParseFromString(action->input());
    Put(in.key(), in.value(), action->version());
    return;

  } else if (type == VersionedKVStoreAction::DELETE) {
    VersionedKVStoreAction::DeleteInput in;
    in.ParseFromString(action->input());
    Delete(in.key(), action->version());
    return;

  } else if (type == VersionedKVStoreAction::GET_VERSION) {
    VersionedKVStoreAction::GetVersionInput in;
    VersionedKVStoreAction::GetVersionOutput out;
    in.ParseFromString(action->input());
    action->add_readset(in.key());
    uint64 v;
    out.set_exists(GetVersion(in.key(), action->version(), &v));
    if (out.exists()) {
      out.set_version(v);
    }
    out.SerializeToString(action->mutable_output());
    return;

  } else {
    LOG(FATAL) << "invalid action type";
  }
}

bool HybridVersionedKVStore::Exists(const string& key, uint64 version) {
  string dummy;
  uint64 flags;
  if (Get(key, version, &dummy, &flags)) {
    return !(flags & kDeletedFlag);
  }
  return false;
}

void HybridVersionedKVStore::Put(
    const string& key,
    const string& value,
    uint64 version,
    uint64 flags) {
  // Create versioned key.
  string versioned_key = key;
  AppendVersion(&versioned_key, version, flags);

  // Seek to first possible record with key 'key'
  KVStore::Iterator* it = current_substore_->GetIterator();
  it->Seek(key);

  // If the key doesn't exist in current_substore_, just put the key and return.
  if (!it->Valid() || !Slice(it->Key()).starts_with(key) ||
      it->Key()[key.size()] != '\0') {
    delete it;
    current_substore_->Put(versioned_key, value);
    return;
  }

  // Put old version key into delay_queue
  delay_queue->Push(it->Key());
  delete it;

  // Then put the versioned_key into current_substore_
  current_substore_->Put(versioned_key, value);

  // Pop a key off the delay_queue and move that record from
  // current_substore_ to old_substore_
  string versioned_front_key;
  string front_key_value;
  if (delay_queue->Pop(&versioned_front_key) == true) {
    current_substore_->Get(versioned_front_key, &front_key_value);
    old_substore_->records_[0]->Put(versioned_front_key, front_key_value);
    current_substore_->Delete(versioned_front_key);
  }
}

bool HybridVersionedKVStore::Get(
    const string& key,
    uint64 version,
    string* value,
    uint64* flags) {

  uint64 f;
  if (flags == NULL) {
    flags = &f;
  }

  // Seek to first possible record with key 'key'
  KVStore::Iterator* it = current_substore_->GetIterator();
  it->Seek(key);

  // Check if the current key exists and starts with target prefix.
  if (!it->Valid() || !Slice(it->Key()).starts_with(key) ||
      it->Key()[key.size()] != '\0') {
    delete it;
    return false;
  }

  // Advance to first key for same object whose encoded version < 'version'.
  while (true) {
    // Check if the current key exists and starts with target prefix.
    if (!it->Valid() || !Slice(it->Key()).starts_with(key) ||
        it->Key()[key.size()] != '\0') {
      delete it;
      return old_substore_->Get(key, version, value, flags);
    }

    // If we can get the version in current_substore, get it and return;
    if (ParseVersion(it->Key(), flags) < version) {
      if (*flags & kDeletedFlag) {
        delete it;
        return false;
      } else {
        *value = it->Value();
        delete it;
        return true;
      }
    }

    // Move to the next record.
    it->Next();
  }
}

bool HybridVersionedKVStore::GetVersion(
    const string& key,
    uint64 version,
    uint64* written,
    uint64* flags) {

  uint64 f;
  if (flags == NULL) {
    flags = &f;
  }

  // Seek to first possible record with key 'key'
  KVStore::Iterator* it = current_substore_->GetIterator();
  it->Seek(key);

  // Check if the current key exists and starts with target prefix.
  if (!it->Valid() || !Slice(it->Key()).starts_with(key) ||
      it->Key()[key.size()] != '\0') {
    delete it;
    return false;
  }

  // Advance to first key for same object whose encoded version < 'version'.
  while (true) {
    // Check if the current key exists and starts with target prefix.
    if (!it->Valid() || !Slice(it->Key()).starts_with(key) ||
        it->Key()[key.size()] != '\0') {
      // Get old versions from old_substore.
      delete it;
      return old_substore_->GetVersion(key, version, written, flags);
    }

    // If we can get the version in current_substore, get it and return;
    uint64 v = ParseVersion(it->Key(), flags);
    if (v < version) {
      *written = v;
      delete it;
      return true;
    }

    // Move to the next record.
    it->Next();
  }
}

void HybridVersionedKVStore::Delete(const string& key, uint64 version) {
  Put(key, "", version, kDeletedFlag);
}
