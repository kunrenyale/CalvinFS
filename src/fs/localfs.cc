// Author: Alexander Thomson <thomson@cs.yale.edu>
//
// Summary of internal synchronization in the LocalFS implementation:
//
//  All leveldb::Env operations are already thread-safe and inherit the Posix
//  file system's operation synchronization.
//
//  * CreateFile and AppendStringToFile are RMW operations. Updates between the
//    read and write phases can cause non-linearizable behavior. They therefore
//    acquire exclusive locks on a global mutex.
//
//  * Read-only operations (ReadFileToString and LS) do NOT require a lock.
//    Since each one reads only a single file from the OS filesystem, any valid
//    state they see will be linearizable without additional synchronization.
//
//  * Blind-write operations (CreateDirectory, WriteStringToFile, and Remove)
//    may proceed concurrently with one another (and with reads) but could
//    conflict with RMW operations, so they acquire shared locks on the global
//    mutex.
//
// Note that this synchronization is extremely conservative, since EVERY append
// and (non-clobbering) file creation conflicts with every other. Total
// concurrency will therefore be pretty crappy. However, this implementation is
// meant as a baseline for testing, not as a performant solution, so whatever.
//

#include "fs/localfs.h"

#include <atomic>
#include <leveldb/env.h>
#include <stdlib.h>

#include "common/types.h"
#include "fs/fs.h"

LocalFS::LocalFS() : env_(leveldb::Env::Default()) {
  env_->GetTestDirectory(&root_);
  vector<string> children;
  Status s = LS("", &children);
  CHECK(s.ok()) << "bad root dir " << root_ << ": " << s.error();
  if (!children.empty()) {
    CHECK_EQ(0, system((string("rm -fr ") + root_ + "/*").c_str()));
  }
}

LocalFS::LocalFS(const string& root)
    : env_(leveldb::Env::Default()), root_(root) {
  vector<string> children;
  Status s = LS("", &children);
  CHECK(s.ok()) << "bad root dir " << root_ << ": " << s.error();
  if (!children.empty()) {
    CHECK_EQ(0, system((string("rm -fr ") + root_ + "/*").c_str()));
  }
}

LocalFS::~LocalFS() {}

Status LocalFS::ReadFileToString(const string& path, string* data) {
  // No lock required.
  return GetStatus(leveldb::ReadFileToString(env_, root_ + path, data));
}

Status LocalFS::CreateDirectory(const string& path) {
  // Shared lock.
  ReadLock l(&mutex_);
  return GetStatus(env_->CreateDir(root_ + path));
}

Status LocalFS::CreateFile(const string& path) {
  // Exclusive lock.
  WriteLock l(&mutex_);
  string rpath = root_ + path;
  if (env_->FileExists(rpath)) {
    return Status::Error("file already exists", next_version_++);
  }
  return GetStatus(leveldb::WriteStringToFile(env_, "", rpath));
}

Status LocalFS::WriteStringToFile(
    const string& data,
    const string& path) {
  // Shared lock.
  ReadLock l(&mutex_);
  return GetStatus(leveldb::WriteStringToFile(env_, data, root_ + path));
}

Status LocalFS::AppendStringToFile(
    const string& data,
    const string& path) {
  // Exclusive lock.
  WriteLock l(&mutex_);
  string contents;
  Status s = ReadFileToString(path, &contents);
  if (!s.ok()) {
    return s;
  }
  contents.append(data);
  return WriteStringToFile(contents, path);
}

Status LocalFS::LS(const string& path, vector<string>* contents) {
  // No lock required.
  contents->clear();
  Status s = GetStatus(
      env_->GetChildren(root_ + path, contents));

  // Remove '.' and '..'
  for (auto it = contents->begin(); it != contents->end(); ) {
    if (*it == "." || *it == "..") {
      it = contents->erase(it);
    } else {
      ++it;
    }
  }

  return s;
}

Status LocalFS::Remove(const string& path) {
  // Shared lock.
  ReadLock l(&mutex_);
  return GetStatus(env_->DeleteFile(root_ + path));
}

Status LocalFS::Copy(const string& from_path, const string& to_path) {
  // Exclusive lock.
  WriteLock l(&mutex_);
  string contents;
  Status s = ReadFileToString(root_ + from_path, &contents);
  if (!s.ok()) {
    return s;
  }
  return WriteStringToFile(contents, root_ + to_path);
}

// Translates leveldb::Status to fs Status.
Status LocalFS::GetStatus(const leveldb::Status& s) {
  if (s.ok()) {
    return Status::OK(next_version_++);
  }
  return Status::Error(s.ToString(), next_version_++);
}

