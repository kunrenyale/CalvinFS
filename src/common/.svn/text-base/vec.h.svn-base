// Author: Alexander Thomson <thomson@cs.yale.edu>
//
// Allows terse construction of vectors.
//
// Vec might be better described by the name VectorStream, but since its
// purpose is to allow vectors to be built extremely tersely in unit tests.
//
// Terse Vec example:
//   vector<int> v = Vec<int>() | 4 | 8 | 15 | 16 | 23 || 42;
//
// Verbose alternative:
//   vector<int> v;
//   v.push_back(4);
//   v.push_back(8);
//   v.push_back(15);
//   v.push_back(16);
//   v.push_back(23);
//   v.push_back(42);
//
// Note: Vec is NOT fast, and should NOT be used in benchmarking or production
//       code.
//

#ifndef CALVIN_COMMON_VEC_H_
#define CALVIN_COMMON_VEC_H_

#include <vector>

using std::vector;

template<class T>
class Vec {
 public:
  inline Vec() {}
  explicit inline Vec(const vector<T>& v) : v_(v) {}
  explicit inline Vec(const Vec<T>& vec) : v_(vec.v_) {}

  // All but the last element in the terse vector declaration must be preceded
  // by '|'.
  inline Vec& operator | (T t) {
    v_.push_back(t);
    return *this;
  }

  // The final element in the terse vector declaration must be preceded by '||'.
  inline vector<T> operator||(T t) {
    v_.push_back(t);
    return v_;
  }

 private:
  // Vector being constructed.
  vector<T> v_;
};

#endif  // CALVIN_COMMON_VEC_H_

