// Author: Alexander Thomson <thomson@cs.yale.edu>
//

#include "components/store/btreestore.h"

#include <string>

#include "btree/btree_map.h"
#include "common/mutex.h"
#include "components/store/store_app.h"
#include "machine/app/app.h"

using btree::btree_map;

REGISTER_APP(BTreeStoreApp) {
  return new StoreApp(new BTreeStore());
}

class BTreeIterator : public KVStore::Iterator {
 public:
  explicit BTreeIterator(BTreeStore* store)
      : store_(store), lock_(&store->mutex_), started_(false) {
  }

  virtual ~BTreeIterator() {}

  virtual bool Valid() {
    return started_ && iter_ != store_->records_.end();
  }

  virtual const string& Key() {
    return iter_->first;
  }

  virtual const string& Value() {
    return iter_->second;
  }

  virtual void Reset() {
    started_ = false;
  }

  virtual void Next() {
    if (!started_) {
      started_ = true;
      iter_ = store_->records_.begin();
    } else {
      ++iter_;
    }
  }

  virtual void Seek(const string& target) {
    started_ = true;
    iter_ = store_->records_.lower_bound(target);
  }

 private:
  BTreeStore* store_;
  ReadLock lock_;
  bool started_;
  btree::btree_map<string, string>::const_iterator iter_;
};

bool BTreeStore::IsLocal(const string& path) {
  return true;
}

uint32 BTreeStore::LookupReplicaByDir(string dir) {
  return 0;
}

uint64 BTreeStore::GetHeadMachine(uint64 machine_id) {
  return 0;
}

uint32 BTreeStore::LocalReplica() {
  return -1;
}

uint32 BTreeStore::GetLocalKeyMastership(string) {
  return -1;
}

bool BTreeStore::CheckLocalMastership(Action* action, set<string>& keys) {
  return false;
}


bool BTreeStore::Exists(const string& key) {
  ReadLock l(&mutex_);
  return records_.count(key) != 0;
}

void BTreeStore::Put(const string& key, const string& value) {
  WriteLock l(&mutex_);
  records_[key] = value;
}

bool BTreeStore::Get(const string& key, string* value) {
  ReadLock l(&mutex_);
  btree_map<string, string>::iterator it = records_.find(key);
  if (it != records_.end()) {
    *value = it->second;
    return true;
  }
  return false;
}

void BTreeStore::Delete(const string& key) {
  WriteLock l(&mutex_);
  records_.erase(key);
}

int BTreeStore::Size() {
  ReadLock l(&mutex_);
  return records_.size();
}
KVStore::Iterator* BTreeStore::GetIterator() {
  return new BTreeIterator(this);
}
