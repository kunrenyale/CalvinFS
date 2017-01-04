// Author: Alexander Thomson <thomson@cs.yale.edu>
//
// Simple KVStore interface. KVStore is a subclass of Store, so it can process
// actions. Action Types and their associated input and output data formats
// are defined in components/store/kvstore.proto.

#ifndef CALVIN_COMPONENTS_STORE_KVSTORE_H_
#define CALVIN_COMPONENTS_STORE_KVSTORE_H_

#include <string>
#include "components/store/store.h"

using std::string;

class KVStore : public Store {
 public:
  virtual ~KVStore() {}

  // Inherited from Store, defined in kvstore.cc:
  virtual void GetRWSets(Action* action);
  virtual void Run(Action* action);

  ////////////////////// KVStore-specific functionality //////////////////////

  // Returns true iff a record exists with key 'key'.
  virtual bool Exists(const string& key) = 0;

  // Inserts the record ('key', 'value').
  virtual void Put(const string& key, const string& value) = 0;

  // If a record exists  associated with 'key', sets '*value'
  // equal to the value associated with that record and returns true, else
  // returns false.
  virtual bool Get(const string& key, string* value) = 0;

  // Erases record with key 'key'.
  virtual void Delete(const string& key) = 0;


  virtual bool IsLocal(const string& path);

  virtual uint32 LookupReplicaByDir(string dir);
  virtual uint64 GetHeadMachine(uint64 machine_id);
  virtual uint32 LocalReplica();

  // Thread-safe iterator over a current snapshot of the store. For some
  // implementations, this may hold a read lock on the store for its full
  // lifetime, so iterators should NEVER be long-lived objects.
  class Iterator {
   public:
    virtual ~Iterator() {}
    virtual bool Valid() = 0;
    virtual const string& Key() = 0;    // Requires: Valid()
    virtual const string& Value() = 0;  // Requires: Valid()
    virtual void Reset() = 0;
    virtual void Next() = 0;
    virtual void Seek(const string& target) = 0;
  };
  // Returns an Iterator that is logically positioned BEFORE the first KV pair.
  // The caller takes ownership of the returned Iterator and is responsible for
  // deleting it when it is done being used.
  virtual Iterator* GetIterator() = 0;
};

#endif  // CALVIN_COMPONENTS_STORE_KVSTORE_H_

