// Author: Alexander Thomson <thomson@cs.yale.edu>
//
// Local filesystem impl. This does not implement blocks or metadata---it just
// wraps the local linux filesystem as exposed by leveldb::Env::Default().

#ifndef CALVIN_FS_LOCALFS_H_
#define CALVIN_FS_LOCALFS_H_

#include <atomic>
#include <leveldb/status.h>
#include "common/mutex.h"
#include "common/types.h"
#include "fs/fs.h"

namespace leveldb {
  class Env;
}  // namespace leveldb

// FileSystem type providing access to an HDFS deployment.
class LocalFS : public FS {
 public:
  // Default constructor uses Env::GetTestDirectory() to decide root dir.
  LocalFS();

  // Specify path to use as root directory.
  explicit LocalFS(const string& root);

  virtual ~LocalFS();
  virtual Status ReadFileToString(const string& path, string* data);
  virtual Status CreateDirectory(const string& path);
  virtual Status CreateFile(const string& path);
  virtual Status WriteStringToFile(const string& data, const string& path);
  virtual Status AppendStringToFile(const string& data, const string& path);
  virtual Status LS(const string& path, vector<string>* contents);
  virtual Status Remove(const string& path);
  virtual Status Copy(const string& from_path, const string& to_path);

 private:
  // Adds version information to a leveldb::Status returned by an underlying
  // filesystem operation.
  Status GetStatus(const leveldb::Status& s);

  MutexRW mutex_;
  leveldb::Env* env_;
  string root_;
  std::atomic<uint64> next_version_;
};

#endif  // CALVIN_FS_LOCALFS_H_

