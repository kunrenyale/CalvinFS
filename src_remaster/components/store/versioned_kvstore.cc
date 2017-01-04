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

void AppendVersion(string* key, uint64 version, uint64 flags = 0) {
  DCHECK(flags >> kReservedBits == 0);
  uint64 v = ((kAfterMaxVersion - version) << kReservedBits) | flags;
  // Append separator token (null character, which is not allowed in keys).
  key->append(1, '\0');
  // Append version (most significant byte first).
  for (int i = 7; i >= 0; i--) {
    key->append(1, GetByte(v, i));
  }
}

uint64 ParseVersion(const Slice& versioned_key, uint64* flags = NULL) {
  uint64 v = 0;
  for (int i = 7; i >= 0; i--) {
    SetByte(&v, i, static_cast<unsigned char>(
                          versioned_key[versioned_key.size() - 1 - i]));
  }
  if (flags != NULL) {
    *flags = v & kFlagsMask;
  }
  return kAfterMaxVersion - (v >> kReservedBits);
}

Slice StripVersion(const Slice& versioned_key) {
  return Slice(versioned_key.data(), versioned_key.size() - 9);
}

///////////////////   VersionedKVStore Implementation   ////////////////////////

VersionedKVStore::VersionedKVStore(KVStore* store) {
  records_[0] = store;
}

VersionedKVStore::VersionedKVStore() {
  records_[0] = new BTreeStore();
}

VersionedKVStore::~VersionedKVStore() {
  delete records_[0];
}

void VersionedKVStore::GetRWSets(Action* action) {
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

bool VersionedKVStore::Exists(const string& key, uint64 version) {
  string dummy;
  uint64 flags;
  if (Get(key, version, &dummy, &flags)) {
    return !(flags & kDeletedFlag);
  }
  return false;
}

void VersionedKVStore::Put(
    const string& key,
    const string& value,
    uint64 version,
    uint64 flags) {
  // Create versioned key.
  string versioned_key = key;
  AppendVersion(&versioned_key, version, flags);

  // Find and lock the map that 'key' lives in.
  int m = FNVHash(key) % kStoreCount;
  // Put (k,v)
  records_[m]->Put(versioned_key, value);
}

bool VersionedKVStore::IsLocal(const string& path) {
  return true;
}

uint32 VersionedKVStore::LookupReplicaByDir(string dir) {
  return 0;
}

uint64 VersionedKVStore::GetHeadMachine(uint64 machine_id) {
  return 0;
}

uint32 VersionedKVStore::LocalReplica() {
  return -1;
}

bool VersionedKVStore::Get(
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

bool VersionedKVStore::GetVersion(
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

void VersionedKVStore::Delete(const string& key, uint64 version) {
  Put(key, "", version, kDeletedFlag);
}

