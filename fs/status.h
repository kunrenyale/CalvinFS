// Author: Alexander Thomson <thomson@cs.yale.edu>
//
// Result type returned by all FS operations. This is essentially the same as
// leveldb::Status, but with version information added.

#ifndef CALVIN_FS_STATUS_H_
#define CALVIN_FS_STATUS_H_

#include <string>
#include "common/types.h"

using std::string;

class Status {
 public:
  Status() : error_(NULL), version_(0) {}
  ~Status() {
    if (error_ != NULL) {
      delete error_;
    }
  }
  // Intentionally implicit copy constructor.
  Status(const Status& other) : version_(other.version_) {
    if (other.error_ == NULL) {
      error_ = NULL;
    } else {
      error_ = new string(*other.error_);
    }
  }

  static Status OK(uint64 version = 0) {
    return Status(NULL, version);
  }
  static Status Error(const string& error, uint64 version = 0) {
    return Status(new string(error.data(), error.size()), version);
  }
  bool ok() {
    return error_ == NULL;
  }
  const string& error() {
    return *error_;
  }
  uint64 version() {
    return version_;
  }
  string ToString() const {
    string s("OK");
    if (error_ != NULL) {
      s = *error_;
    }
    if (version_ != 0) {
      s.append(string(" (") + UInt64ToString(version_) + ")");
    }
    return s;
  }

  Status& operator=(const Status& other) {
    if (error_ != NULL) {
      delete error_;
    }
    if (other.error_ == NULL) {
      error_ = NULL;
    } else {
      error_ = new string(*other.error_);
    }
    version_ = other.version_;
    return *this;
  }

  bool operator==(const Status& other) const {
    if (version_ != other.version_) {
      return false;
    }
    if (error_ == NULL || other.error_ == NULL) {
      return error_ == other.error_;
    }
    return *error_ == *other.error_;
  }

 private:
  Status(string* error, uint64 version) : error_(error), version_(version) {}

  string* error_;  // NULL = OK.
  uint64 version_;
};

inline std::ostream& operator<<(std::ostream& out, const Status& s) {
  return out << s.ToString();
}

#endif  // CALVIN_FS_STATUS_H_
