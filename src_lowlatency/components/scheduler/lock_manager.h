// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// Deterministic lock manager. NOT thread safe.

#ifndef CALVIN_COMPONENTS_SCHEDULER_LOCK_MANAGER_H_
#define CALVIN_COMPONENTS_SCHEDULER_LOCK_MANAGER_H_

#include <deque>
#include <glog/logging.h>
#include <queue>
#include <tr1/unordered_map>
#include <vector>

#include "common/types.h"

using std::deque;
using std::queue;
using std::vector;
using std::tr1::unordered_map;

class Action;

class LockManager {
 public:
  ~LockManager() {}

  // Attempts to grant a read lock to the specified transaction, enqueueing
  // request in lock table. Returns true if lock is immediately granted, else
  // returns false.
  //
  // Requires: Neither ReadLock nor WriteLock has previously been called with
  //           this txn and key.
  bool ReadLock(Action* a, const string& key);

  // Attempts to grant a write lock to the specified transaction, enqueueing
  // request in lock table. Returns true if lock is immediately granted, else
  // returns false.
  //
  // Requires: Neither ReadLock nor WriteLock has previously been called with
  //           this txn and key.
  bool WriteLock(Action* a, const string& key);

  // Releases lock held by 'txn' on 'key', or cancels any pending request for
  // a lock on 'key' by 'txn'. If 'txn' held an EXCLUSIVE lock on 'key' (or was
  // the sole holder of a SHARED lock on 'key'), then the next request(s) in the
  // request queue is granted. If the granted request(s) corresponds to a
  // transaction that has now acquired ALL of its locks, that transaction is
  // appended to the 'ready_' queue.
  void Release(Action* a, const string& key);

  // If any actions are newly ready to run, returns true and sets '*a' to the
  // oldest one not yet returned by a call to Ready---else returns false.
  bool Ready(Action** a);

 private:
  // The LockManager's lock table tracks all lock requests. For a key k, if
  // 'lock_table_[k]' contains a nonempty queue, then the item with that
  // key is locked and either:
  //
  //  (a) first element in the deque specifies the owner if that item is a
  //      request for an EXCLUSIVE lock, or
  //
  //  (b) a SHARED lock is held by all elements of the longest prefix of the
  //      deque containing only SHARED lock requests.
  //
  static const int kLockTableSize = 1024;
  enum LockMode {
    SHARED = 0,
    EXCLUSIVE = 1,
  };
  struct LockRequest {
    LockRequest(LockMode m, Action* a) : action_(a), mode_(m) {}
    Action* action_;  // Pointer to action requesting the lock.
    bool mode_;       // EXCLUSIVE or SHARED lock request.
  };
  unordered_map<string, deque<LockRequest> > lock_table_;

  // Queue of pointers to transactions that:
  //  (a) were previously blocked on acquiring at least one lock, and
  //  (b) have now acquired all locks that they have requested.
  queue<Action*> ready_;

  // Tracks all actions still waiting on acquiring at least one lock. Entries
  // in 'waiting_' are invalided by any call to Release() with the entry's
  // action.
  unordered_map<Action*, int> waiting_;
};

#endif  // CALVIN_COMPONENTS_SCHEDULER_LOCK_MANAGER_H_

