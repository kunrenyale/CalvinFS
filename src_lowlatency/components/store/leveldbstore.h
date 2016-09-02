// Author: Alexander Thomson <thomson@cs.yale.edu>
//
// KVStore implemented using leveldb.

#ifndef CALVIN_COMPONENTS_STORE_LEVELDBSTORE_H_
#define CALVIN_COMPONENTS_STORE_LEVELDBSTORE_H_

#include <leveldb/db.h>
#include <string>

#include "components/store/kvstore.h"

class LevelDBStore : public KVStore {
 public:
  LevelDBStore();
  virtual ~LevelDBStore();

  virtual bool Exists(const string& key);
  virtual void Put(const string& key, const string& value);
  virtual bool Get(const string& key, string* value);
  virtual void Delete(const string& key);
  virtual KVStore::Iterator* GetIterator();

  virtual bool IsLocal(const string& path);
  virtual uint32 LookupReplicaByDir(string dir);
  virtual uint64 GetHeadMachine(uint64 machine_id);
  virtual uint32 LocalReplica();

 private:
  friend class LevelDBIterator;

  // LevelDB database storing all records.
  leveldb::DB* records_;
};

#endif  // CALVIN_COMPONENTS_STORE_LEVELDBSTORE_H_
