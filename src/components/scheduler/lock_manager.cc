// Author: Alexander Thomson (thomson@cs.yale.edu)

#include "components/scheduler/lock_manager.h"

bool LockManager::WriteLock(Action* a, const string& key) {
  // Insert new request.
  deque<LockRequest>* q = &lock_table_[key];
  q->push_back(LockRequest(EXCLUSIVE, a));

  // Write lock request succeeds iff it is the only current requester.
  if (q->size() == 1) {
    return true;
  } else {
    // If request fails, increment action wait count and return false.
    if (waiting_.count(a) == 0) {
      waiting_[a] = 1;
    } else {
      waiting_[a]++;
    }
    return false;
  }
}

bool LockManager::ReadLock(Action* a, const string& key) {
  // Insert new request.
  deque<LockRequest>* q = &lock_table_[key];
  q->push_back(LockRequest(SHARED, a));

  // Read lock request fails if there is any previous write request.
  for (deque<LockRequest>::iterator it = q->begin(); it != q->end(); ++it) {
    if (it->mode_ == EXCLUSIVE) {
      // Request fails, increment action wait count and return false.
      if (waiting_.count(a) == 0) {
        waiting_[a] = 1;
      } else {
        waiting_[a]++;
      }
      return false;
    }
  }

  // No preceding write lock was found. Lock acquired.
  return true;
}

void LockManager::Release(Action* a, const string& key) {
  // Make sure the relevant request deque exists in the lock table. Otherwise
  // the lock is clearly not held.
  if (lock_table_.count(key) == 0) {
    return;
  }
  deque<LockRequest>* q = &lock_table_[key];

  // Seek to the target request. Note whether any write lock requests precede
  // the target.
  bool write_requests_precede_target = false;
  deque<LockRequest>::iterator it;
  for (it = q->begin(); it != q->end() && it->action_ != a; ++it) {
    if (it->mode_ == EXCLUSIVE) {
      write_requests_precede_target = true;
    }
  }

  // If we found the request, erase it. No need to do anything otherwise.
  if (it != q->end()) {
    // Save an iterator pointing to the target to call erase on after handling
    // lock inheritence, since erase(...) trashes all iterators.
    deque<LockRequest>::iterator target = it;

    // If there are more requests following the target request, one or more
    // may need to be granted as a result of the target's release.
    ++it;
    if (it != q->end()) {
      vector<Action*> new_owners;

      // Grant subsequent request(s) if:
      //  (a) The canceled request held a write lock.
      //  (b) The canceled request held a read lock ALONE.
      //  (c) The canceled request was a write request preceded only by read
      //      requests and followed by one or more read requests.
      if (target == q->begin() &&
          (target->mode_ == EXCLUSIVE ||
           (target->mode_ == SHARED && it->mode_ == EXCLUSIVE))) {  // (a)/(b)
        // If a write lock request follows, grant it.
        if (it->mode_ == EXCLUSIVE) {
          new_owners.push_back(it->action_);
        }
        // If a sequence of read lock requests follows, grant all of them.
        for (; it != q->end() && it->mode_ == SHARED; ++it) {
          new_owners.push_back(it->action_);
        }
      } else if (!write_requests_precede_target &&
                 target->mode_ == EXCLUSIVE && it->mode_ == SHARED) {  // (c)
        // If a sequence of read lock requests follows, grant all of them.
        for (; it != q->end() && it->mode_ == SHARED; ++it) {
          new_owners.push_back(it->action_);
        }
      }

      // Handle actions with newly granted locks that may now be ready to run.
      for (uint32 i = 0; i < new_owners.size(); i++) {
        // Make sure lock inheritor is actually waiting for at least one lock!
        CHECK(waiting_.count(new_owners[i]));

        // Decrement txn wait count.
        waiting_[new_owners[i]]--;

        // If lock inheritor is not waiting on more locks, it is ready to run.
        if (waiting_[new_owners[i]] == 0) {
          ready_.push(new_owners[i]);
          waiting_.erase(new_owners[i]);
        }
      }
    }

    // Now it is safe to actually erase the target request.
    q->erase(target);

    // Delete lock table entry if the deque is empty.
    if (q->size() == 0) {
      lock_table_.erase(key);
    }
  }
}

bool LockManager::Ready(Action** a) {
  if (!ready_.empty()) {
    *a = ready_.front();
    ready_.pop();
    return true;
  }
  return false;
}

