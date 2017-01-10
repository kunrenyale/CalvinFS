// Author: Alexander Thomson <thomson@cs.yale.edu>
//
// TODO(agt): Document key layout.

#include "components/store/versioned_kvstore.h"

#include <glog/logging.h>
#include <leveldb/db.h>
#include <string>
#include "common/utils.h"
#include "components/store/btreestore.h"
#include "components/store/kvstore.h"
#include "components/store/versioned_kvstore.pb.h"

////////////////////////////       Constants       ////////////////////////////

static const int32 kReservedBits = 1;
static const uint64 kDeletedFlag = 1;

static const uint64 kAfterMaxVersion =
    static_cast<uint64>(0xFFFFFFFFFFFFFFFF >> kReservedBits);
static const uint64 kFlagsMask =
    static_cast<uint64>(0xFFFFFFFFFFFFFFFF >> (64 - kReservedBits));

////////////////////////////       Utilities       ////////////////////////////

// Returns the k'th lowest-order byte of x.
unsigned char GetByte(uint64 x, uint32 k) {
  CHECK_LT(k, static_cast<uint32>(8));
  return static_cast<unsigned char>(x >> (8 * k));
}

// Sets the k'th lowest-order byte of *x to c.
void SetByte(uint64* x, uint32 k, unsigned char c) {
  CHECK_LT(k, static_cast<uint32>(8));

  // Clear k'th byte.
  uint64 filter = 255;
  *x &= ~(filter << (8 * k));
  CHECK(GetByte(*x, k) == 0);

  // Set k'th byte.
  *x |= static_cast<uint64>(c) << (8 * k);
}

void AppendMaster(string* value, uint64 master) {
  // Append separator token (null character, which is not allowed in keys).
  value->append(1, '\0');
  // Append naster (most significant byte first).
  value->append(1, GetByte(master, 0));
}

uint64 ParseMaster(const Slice& mastered_value) {
  uint64 v = 0;

  SetByte(&v, 0, static_cast<unsigned char>(mastered_value[mastered_value.size() - 1]));

  return v;
}

Slice StripMaster(const Slice& mastered_value) {
  return Slice(mastered_value.data(), mastered_value.size() - 2);
}

///////////////////   VersionedKVStore Implementation   ////////////////////////

MasteredKVStore::MasteredKVStore(KVStore* store) {
  records_[0] = store;
}

MasteredKVStore::MasteredKVStore() {
  records_[0] = new BTreeStore();
}

MasteredKVStore::~MasteredKVStore() {
  delete records_[0];
}

void MasteredKVStore::GetRWSets(Action* action) {
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

void VersionedKVStore::Run(Action* action) {
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

bool MasteredKVStore::Exists(const string& key) {
  string dummy;
  uint64 flags;
  if (Get(key)) {
    return !(flags & kDeletedFlag);
  }
  return false;
}

void MasteredKVStore::Put(const string& key, const string& value, uint64 master) {
  // Create versioned key.
  string versioned_key = key;
  AppendMaster(&value, master);

  // Find and lock the map that 'key' lives in.
  int m = FNVHash(key) % kStoreCount;
  // Put (k,v)
  records_[m]->Put(key, value);
}

bool MasteredKVStore::IsLocal(const string& path) {
  return true;
}

uint32 MasteredKVStore::LookupReplicaByDir(string dir) {
  return 0;
}

uint64 MasteredKVStore::GetHeadMachine(uint64 machine_id) {
  return 0;
}

uint32 MasteredKVStore::LocalReplica() {
  return -1;
}

bool MasteredKVStore::Get(
    const string& key,
    uint64 version,
    string* value,
    uint64* flags) {
  // Set *flags to dummy value if caller did not provide non-NULL value.
  uint64 f;
  if (flags == NULL) {
    flags = &f;
  }

  // Find and lock the map that 'key' lives in.
  int m = FNVHash(key) % kStoreCount;

  // Seek to first possible record with key 'key'.
  KVStore::Iterator* it = records_[m]->GetIterator();
  it->Seek(key);

  // Advance to first key for same object whose encoded version < 'version'.
  while (true) {
    // Check if the current key exists and starts with target prefix.
    if (!it->Valid() || !Slice(it->Key()).starts_with(key) ||
        it->Key()[key.size()] != '\0') {
      delete it;
      return false;
    }

    // Check if the current key's version < 'version'.
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

bool MasteredKVStore::GetVersion(
    const string& key,
    uint64 version,
    uint64* written,
    uint64* flags) {
  // Make sure *flags is writeable.
  uint64 f;
  if (flags == NULL) {
    flags = &f;
  }

  // Find and lock the map that 'key' lives in.
  int m = FNVHash(key) % kStoreCount;

  // Seek to first possible record with key 'key'.
  KVStore::Iterator* it = records_[m]->GetIterator();
  it->Seek(key);

  // Advance to first key for same object whose encoded version < 'version'.
  while (true) {
    // Check if the current key exists and starts with target prefix.
    if (!it->Valid() || !Slice(it->Key()).starts_with(key) ||
        it->Key()[key.size()] != '\0') {
      delete it;
      return false;
    }

    // Check if the current key's version < 'version'.
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

void MasteredKVStore::Delete(const string& key, uint64 version) {
  Put(key, "", version, kDeletedFlag);
}

