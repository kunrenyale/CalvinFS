// Author: Alex Thomson (thomson@cs.yale.edu)
//

#ifndef CALVIN_COMMON_SOURCE_H_
#define CALVIN_COMMON_SOURCE_H_

#include "common/atomic.h"
#include "common/utils.h"

template <typename T>
class Source {
 public:
  virtual ~Source() {}
  virtual bool Get(T* t) = 0;
};

template <typename T>
class EmptySource : public Source<T> {
 public:
  virtual ~EmptySource() {}
  virtual bool Get(T* t) { return false; }
};

template <typename T>
class QueueSource : public Source<T> {
 public:
  QueueSource() {}
  virtual ~QueueSource() {}
  virtual bool Get(T* t) {
    return queue_.Pop(t);
  }
  void Add(const T& t) {
    queue_.Push(t);
  }
  int Size() {
    return queue_.Size();
  }

 private:
  AtomicQueue<T> queue_;
};

template <typename T>
class ThrottleSource : public Source<T> {
 public:
  ThrottleSource(double delay, Source<T>* base)
    : base_(base), delay_(delay), last_get_(0) {
  }

  virtual ~ThrottleSource() {
    delete base_;
  }

  virtual bool Get(T* t) {
    double time = GetTime();
    if (time > last_get_ + delay_ && base_->Get(t)) {
      last_get_ = time;
      return true;
    }
    return false;
  }

 private:
  Source<T>* base_;
  double delay_;
  double last_get_;
};

template <typename T>
class LimitSource : public Source<T> {
 public:
  LimitSource(int limit, Source<T>* base)
    : base_(base), limit_(limit), count_(0) {
  }

  virtual ~LimitSource() {
    delete base_;
  }

  virtual bool Get(T* t) {
    if (count_ < limit_ && base_->Get(t)) {
      count_++;
      return true;
    }
    return false;
  }

 private:
  Source<T>* base_;
  int limit_;
  int count_;
};

#endif  // CALVIN_COMMON_SOURCE_H_

