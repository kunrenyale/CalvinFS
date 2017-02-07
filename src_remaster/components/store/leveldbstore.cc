// Author: Alexander Thomson <thomson@cs.yale.edu>
//

#include "components/store/leveldbstore.h"

#include <glog/logging.h>
#include <leveldb/db.h>
#include <leveldb/env.h>
#include <leveldb/iterator.h>
#include <leveldb/status.h>

#include "common/types.h"
#include "components/store/store_app.h"
#include "machine/app/app.h"

REGISTER_APP(LevelDBStoreApp) {
  return new StoreApp(new LevelDBStore());
}

class LevelDBStoreIterator : public KVStore::Iterator {
 public:
  explicit LevelDBStoreIterator(leveldb::DB* db) {
    started_ = false;
    it_ = db->NewIterator(leveldb::ReadOptions());
  }

  virtual ~LevelDBStoreIterator() {
    delete it_;
  }

  virtual bool Valid() {
    return started_ && it_->Valid();
  }

  virtual const string& Key() {
    key_.assign(it_->key().data(), it_->key().size());
    return key_;
  }

  virtual const string& Value() {
    value_.assign(it_->value().data(), it_->value().size());
    return value_;
  }

  virtual void Reset() {
    started_ = false;
  }

  virtual void Next() {
    if (!started_) {
      started_ = true;
      it_->SeekToFirst();
      return;
    }
    it_->Next();
  }

  virtual void Seek(const string& target) {
    started_ = true;
    it_->Seek(target);
  }

 private:
  // True iff not positioned before first element.
  bool started_;

  leveldb::Iterator* it_;
  string key_;
  string value_;
};

LevelDBStore::LevelDBStore() {
  string path;
  leveldb::Env::Default()->GetTestDirectory(&path);
  path.append("/store-");
  static atomic<int> guid(0);
  path.append(IntToString(guid++));

  // Destroy any previous instantiation of the database.
  leveldb::DestroyDB(path, leveldb::Options());

  // Instantiate a new database.
  leveldb::Options options;
  options.create_if_missing = true;
  options.error_if_exists = true;
  options.compression = leveldb::kNoCompression;  // No compression.

  if (!leveldb::DB::Open(options, path, &records_).ok()) {
    LOG(ERROR) << "Error opening LevelDB database.";
  }
}

LevelDBStore::~LevelDBStore() {
  delete records_;
}

bool LevelDBStore::IsLocal(const string& path) {
  return true;
}

uint32 LevelDBStore::LookupReplicaByDir(string dir) {
  return 0;
}

uint64 LevelDBStore::GetHeadMachine(uint64 machine_id) {
  return 0;
}

uint32 LevelDBStore::LocalReplica() {
  return -1;
}

uint32 LevelDBStore::GetLocalKeyMastership(string) {
  return -1;
}

bool LevelDBStore::CheckLocalMastership(Action* action, set<string>& keys) {
  return false;
}

bool LevelDBStore::Exists(const string& key) {
  string s;
  return Get(key, &s);
}

void LevelDBStore::Put(const string& key, const string& value) {
  // Create versioned key.
  CHECK(records_->Put(leveldb::WriteOptions(), key, value).ok());
}

bool LevelDBStore::Get(const string& key, string* value) {
  leveldb::Status s = records_->Get(leveldb::ReadOptions(), key, value);
  if (s.ok()) {
    return true;
  } else {
    CHECK(s.IsNotFound()) << s.ToString();
  }
  return false;
}

void LevelDBStore::Delete(const string& key) {
  CHECK(records_->Delete(leveldb::WriteOptions(), key).ok());
}

KVStore::Iterator* LevelDBStore::GetIterator() {
  return new LevelDBStoreIterator(records_);
}

