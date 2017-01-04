// Author: Alexander Thomson <thomson@cs.yale.edu>
//
// KVStore implemented using google btree.

#ifndef CALVIN_COMPONENTS_STORE_BTREESTORE_H_
#define CALVIN_COMPONENTS_STORE_BTREESTORE_H_

#include <string>
#include "btree/btree_map.h"
#include "common/mutex.h"
#include "components/store/kvstore.h"

class BTreeStore : public KVStore {
 public:
  BTreeStore() {}
  virtual ~BTreeStore() {}

  virtual bool Exists(const string& key);
  virtual void Put(const string& key, const string& value);
  virtual bool Get(const string& key, string* value);
  virtual void Delete(const string& key);
  virtual int Size();
  virtual KVStore::Iterator* GetIterator();

  virtual bool IsLocal(const string& path);
  virtual uint32 LookupReplicaByDir(string dir);
  virtual uint64 GetHeadMachine(uint64 machine_id);
  virtual uint32 LocalReplica();

 protected:
  friend class BTreeIterator;

  // All records live in a btree.
  btree::btree_map<string, string> records_;

  // Mutex for atomic ops.
  MutexRW mutex_;
};

#endif  // CALVIN_COMPONENTS_STORE_BTREESTORE_H_
