// Author: Alexander Thomson <thomson@cs.yale.edu>
// Author:  Kun Ren <kun.ren@yale.edu>
//

#ifndef CALVIN_FS_BLOCK_STORE_H_
#define CALVIN_FS_BLOCK_STORE_H_

#include <leveldb/db.h>
#include <leveldb/slice.h>
#include <string>
#include "common/types.h"
#include "machine/app/app.h"

class BlockStore {
 public:
  virtual ~BlockStore() {}
  virtual bool Exists(uint64 block_id) = 0;
  virtual void Put(uint64 block_id, const Slice& value) = 0;
  virtual bool Get(uint64 block_id, string* value) = 0;
};

class LocalFileBlockStore : public BlockStore {
 public:
  LocalFileBlockStore();
  virtual ~LocalFileBlockStore() {}
  virtual bool Exists(uint64 block_id);
  virtual void Put(uint64 block_id, const Slice& data);
  virtual bool Get(uint64 block_id, string* data);

 private:
  string path_prefix_;
};

class LevelDBBlockStore : public BlockStore {
 public:
  LevelDBBlockStore();
  virtual ~LevelDBBlockStore();
  virtual bool Exists(uint64 block_id);
  virtual void Put(uint64 block_id, const Slice& data);
  virtual bool Get(uint64 block_id, string* data);

 private:
  leveldb::DB* blocks_;
};

class HybridBlockStore : public BlockStore {
 public:
  HybridBlockStore() {}
  virtual ~HybridBlockStore() {}
  virtual bool Exists(uint64 block_id);
  virtual void Put(uint64 block_id, const Slice& data);
  virtual bool Get(uint64 block_id, string* data);

 private:
  LocalFileBlockStore large_blocks_;
  LevelDBBlockStore small_blocks_;
};

// App wrapping a block store.
class BlockStoreApp : public App {
 public:
  explicit BlockStoreApp(BlockStore* blocks) : blocks_(blocks) {}
  virtual ~BlockStoreApp();
  virtual bool Exists(uint64 block_id);
  virtual void Put(uint64 block_id, const Slice& data);
  virtual bool Get(uint64 block_id, string* data);
  virtual void HandleMessage(Header* header, MessageBuffer* message);

 protected:
  friend class BlockLogApp;

  // Underlying block store.
  BlockStore* blocks_;

  // Default constructor accessable only to subclasses.
  BlockStoreApp() {}
};

class CalvinFSConfigMap;
class DistributedBlockStoreApp : public BlockStoreApp {
 public:
  explicit DistributedBlockStoreApp(BlockStore* blocks);
  virtual ~DistributedBlockStoreApp() {}

  virtual bool Exists(uint64 block_id);
  virtual void Put(uint64 block_id, const Slice& data);
  virtual bool Get(uint64 block_id, string* data);

  virtual void HandleMessage(Header* header, MessageBuffer* message);
  virtual void Start();

 private:
  bool IsLocal(uint64 block_id);

  // Replica to which the local machine belongs.
  uint64 replica_;

  // Global config map.
  CalvinFSConfigMap* config_;
};

#endif  // CALVIN_FS_BLOCK_STORE_H_

