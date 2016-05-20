// Author: Alexander Thomson <thomson@cs.yale.edu>
//
// Defines the FS client API.

#ifndef CALVIN_FS_FS_H_
#define CALVIN_FS_FS_H_

#include <glog/logging.h>
#include <vector>
#include "common/types.h"
#include "common/utils.h"
#include "fs/status.h"

using std::vector;
class FileHandle;

// FS interface.
class FS {
 public:
  virtual ~FS() {}

  // cat [path]
  virtual Status ReadFileToString(const string& path, string* data) = 0;

  // mkdir [path]
  virtual Status CreateDirectory(const string& path) = 0;

  // touch [path]   (does not clobber existing file)
  virtual Status CreateFile(const string& path) = 0;

  // echo [data] > [path]   (clobbers existing file)
  virtual Status WriteStringToFile(const string& data, const string& path) = 0;

  // echo [data] >> [path]   (requires: file already exists)
  virtual Status AppendStringToFile(const string& data, const string& path) = 0;

  // ls [path]
  virtual Status LS(const string& path, vector<string>* contents) = 0;

  // rm [path]   (non-recursive: does not work on directories)
  virtual Status Remove(const string& path) = 0;

  // cp [from_path] [to_path]   (non-recursive: does not work on directories)
  virtual Status Copy(const string& from_path, const string& to_path) = 0;

  ////////////////////////////////////////////////////////////////////////////
  //
  // Everything below here is NON-URGENT for the SOSP submission.
  // Do not bother implementing for now.
  //

  // Gets a readable file handle.
  virtual Status Open(const string& path, FileHandle** file) {
    LOG(FATAL) << "NOT IMPLEMENTED";
    return Status::Error("NOT IMPLEMENTED");
  }

  // cat [from_path] >> [to_path]
  virtual Status AppendFileToFile(const string& data, const string& path) {
    LOG(FATAL) << "NOT IMPLEMENTED";
    return Status::Error("NOT IMPLEMENTED");
  }

  // mv [from_path] [to_path]
  virtual Status Rename(const string& from_path, const string& to_path) {
    LOG(FATAL) << "NOT IMPLEMENTED";
    return Status::Error("NOT IMPLEMENTED");
  }

  // chown [user] [path]; chgrp [group] [path]; chmod [mode] [path]
  virtual Status SetPermissions(
      uint64 owner,
      uint64 group,
      const string& mode,
      const string& path) {
    LOG(FATAL) << "NOT IMPLEMENTED";
    return Status::Error("NOT IMPLEMENTED");
  }
  virtual Status GetPermissions(
      const string& path,
      uint64* owner,
      uint64* group,
      string* mode) {
    LOG(FATAL) << "NOT IMPLEMENTED";
    return Status::Error("NOT IMPLEMENTED");
  }

  // recursive versions of above commands
  virtual Status RecursiveRemove(const string& path) {
    LOG(FATAL) << "NOT IMPLEMENTED";
    return Status::Error("NOT IMPLEMENTED");
  }
  virtual Status RecursiveCopy(
      const string& from_path,
      const string& to_path) {
    LOG(FATAL) << "NOT IMPLEMENTED";
    return Status::Error("NOT IMPLEMENTED");
  }
  virtual Status RecursiveRename(
      const string& from_path,
      const string& to_path) {
    LOG(FATAL) << "NOT IMPLEMENTED";
    return Status::Error("NOT IMPLEMENTED");
  }
};

////////////////////////////////////////////////////////////////////////////////
// Read-only snapshot of a file.
// This is NON-URGENT for the SOSP submission.
// Do not bother implementing for now.
class FileHandle {
 public:
  virtual ~FileHandle() {}
  virtual uint64 Version() = 0;
  virtual uint64 Size() = 0;
  virtual Status GetPermissions(uint64* owner, uint64* group, string* mode) = 0;
  virtual Status Read(uint64 offset, uint64 count, string* result) = 0;
  virtual Status ReadFileToString(string* result) = 0;
};

#endif  // CALVIN_FS_FS_H_

