// Author: Alexander Thomson <thomson@cs.yale.edu>
//

#ifndef CALVIN_COMPONENTS_STORE_MASTERED_KVSTORE_H_
#define CALVIN_COMPONENTS_STORE_MASTERED_KVSTORE_H_

#include <string>
#include "common/types.h"

#include "components/store/kvstore.h"
#include "proto/action.pb.h"

class MasteredKVStore : public Store {
 public:
  explicit MasteredKVStore(KVStore* store);
  ~MasteredKVStore();

  // Types of actions that MasteredKVStore can interpret.
  virtual void GetRWSets(Action* action);
  virtual void Run(Action* action);

  // Returns true iff a record exists with key 'key'.
  bool Exists(const string& key);

  // Inserts the record ('key', 'value') .
  void Put(const string& key, const string& value);

  // If a record exists at master 'master' associated with 'key', sets '*value'
  // equal to the value associated with that record and returns true, else
  // returns false.
  bool Get(const string& key, uint64 master, string* value);

  // If a record associated with 'key' exists (or is deleted),
  // sets '*master' equal to the master, returns true, else returns false.
  bool Getmaster(const string& key, uint64* master);

  // Erases record with key 'key'.
  void Delete(const string& key);


  virtual bool IsLocal(const string& path);
  virtual uint32 LookupReplicaByDir(string dir);
  virtual uint64 GetHeadMachine(uint64 machine_id);
  virtual uint32 LocalReplica();

 protected:
  MasteredKVStore();

  // Number of underlying KVStores across which records are distributed
  // (to reduce latch contention).
  static const int kStoreCount = 1;

  // Underlying KVStore(s) in which records are stored.
  KVStore* records_[kStoreCount];
};

#endif  // CALVIN_COMPONENTS_STORE_MASTERED_KVSTORE_H_

