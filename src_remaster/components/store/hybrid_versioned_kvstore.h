// Author: Kun Ren <kun@cs.yale.edu>
//

#ifndef CALVIN_COMPONENTS_STORE_HYBRID_VERSIONED_KVSTORE_H_
#define CALVIN_COMPONENTS_STORE_HYBRID_VERSIONED_KVSTORE_H_

#include <string>
#include "common/types.h"
#include "common/atomic.h"

#include "components/store/versioned_kvstore.h"
#include "components/store/kvstore.h"
#include "proto/action.pb.h"

class HybridVersionedKVStore : public VersionedKVStore {
 public:
  HybridVersionedKVStore();
  ~HybridVersionedKVStore();

  // Types of actions that VersionedKVStore can interpret.
  virtual void GetRWSets(Action* action);
  virtual void Run(Action* action);

  virtual bool IsLocal(const string& path);
  virtual uint32 LookupReplicaByDir(string dir);
  virtual uint64 GetHeadMachine(uint64 machine_id);
  virtual uint32 LocalReplica();
  virtual bool CheckLocalMastership(Action* action, set<string>& keys);


  // Returns true iff a record exists at version 'version' with key 'key'.
  bool Exists(const string& key, uint64 version);

  // Inserts the record ('key', 'value') at time 'version'.
  void Put(
      const string& key,
      const string& value,
      uint64 version,
      uint64 flags = 0);

  // If a record exists at time 'version' associated with 'key', sets '*value'
  // equal to the value associated with that record and returns true, else
  // returns false.
  bool Get(
      const string& key,
      uint64 version,
      string* value,
      uint64* flags = NULL);

  // If a record associated with 'key' exists (or is deleted) at time 'version',
  // sets '*version' equal to the version at which the record was last modified
  // and returns true, else returns false.
  bool GetVersion(
      const string& key,
      uint64 version,
      uint64* written,
      uint64* flags = NULL);

  // Erases record with key 'key' at version 'version'.
  void Delete(const string& key, uint64 version);

 private:
  // Current_substore_ uses BTreeStore as its underlying KVStore;
  // Old_substore_ uses LevelDBStore as its underlying KVStore.
  KVStore* current_substore_;
  VersionedKVStore* old_substore_;

  DelayQueue<string>* delay_queue;
};

#endif  // CALVIN_COMPONENTS_STORE_HYBRID_VERSIONED_KVSTORE_H_
