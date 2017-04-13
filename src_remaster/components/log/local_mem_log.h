// Author: Alex Thomson
// Author Kun Ren <kun.ren@yale.edu>
//
// Super-simple in-memory implementation of a components log. The log entries
// are in fact specific to a given Log object. This is not in any way a
// distributed log.

#ifndef CALVIN_COMPONENTS_LOG_LOCAL_MEM_LOG_H_
#define CALVIN_COMPONENTS_LOG_LOCAL_MEM_LOG_H_

#include <atomic>
#include <vector>

#include "machine/app/app.h"
#include "components/log/log.h"
#include "common/mutex.h"
#include "common/types.h"
#include "common/utils.h"
#include "proto/header.pb.h"

using std::atomic;
using std::vector;

class LocalMemLog : public Log {
 public:
  // Initially empty log.
  LocalMemLog();
  virtual ~LocalMemLog();

  // Actual log interface.
  virtual void Append(uint64 version, const Slice& entry);
  virtual void Append(uint64 version, uint64 count, const Slice& entry);
  virtual typename Log::Reader* GetReader();
  virtual uint64 LastVersion();

 private:
  friend class LocalMemLogReader;

  // Mutex guarding state.
  MutexRW mutex_;

  // Max version that appears in log so far.
  uint64 max_version_;

  // Array of entries.
  struct Entry {
    Entry() : version(0), count(0), entry(NULL) {}
    Entry(uint64 v, uint64 s, const Slice& e)
        : version(v), count(s), entry(new string(e.data(), e.size())) {
    }

    Entry(uint64 v, const Slice& e)
        : version(v), count(0), entry(new string(e.data(), e.size())) {
    }

    uint64 version;
    uint64 count;
    string* entry;
  };
  Entry* entries_;
  atomic<uint64> size_;
  uint64 allocated_;

  // DISALLOW_COPY_AND_ASSIGN
  LocalMemLog(const LocalMemLog&);
  LocalMemLog& operator=(const LocalMemLog&);
};

#endif  // CALVIN_COMPONENTS_LOG_LOCAL_MEM_LOG_H_

