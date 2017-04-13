// Author Alexander Thomson <thomson@cs.yale.edu>
// Author Thaddeus Diamond <diamond@cs.yale.edu>
// Author Kun Ren <kun.ren@yale.edu>
//
// Some miscellaneous commonly-used utility functions.
//
// TODO(alex): Organize these into reasonable categories, etc.
// TODO(alex): MORE/BETTER UNIT TESTING!

#ifndef CALVIN_COMMON_UTILS_H_
#define CALVIN_COMMON_UTILS_H_

#include <glog/logging.h>
#include <sys/time.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string>
#include <vector>

#include "common/types.h"
#include "proto/scalar.pb.h"

using std::string;
using std::vector;

template<typename T> string TypeName();
#define ADD_TYPE_NAME(T) \
template<> string TypeName<T>() { return #T; }

// Splits a string on 'delimiter'.
vector<string> SplitString(const string& input, char delimiter);

// Returns the number of seconds since midnight according to local system time,
// to the nearest microsecond.
double GetTime();

// The FNV-1 and FNV-1a hashes as described by Fowler-Noll-Vo.
uint32 FNVHash(const Slice& key);
uint32 FNVModHash(const Slice& key);

// Busy-wait (or yield) for 'duration' seconds.
void Spin(double duration);

// Busy-wait (or yield) until GetTime() >= 'time'.
void SpinUntil(double time);

// Produces a random alphabetic string of 'length' characters.
string RandomString(int length);

// Random byte sequence. Any byte may appear.
string RandomBytes(int length);

// Random byte sequence. Any byte except '\0' may appear.
string RandomBytesNoZeros(int length);

double RandomGaussian(double s);

// Returns human-readable numeric string representation of an (u)int{32,64}.
string Int32ToString(int32 n);
string Int64ToString(int64 n);
string UInt32ToString(uint32 n);
string UInt64ToString(uint64 n);
string FloatToString(float n);
string DoubleToString(double n);

inline string IntToString(int n) { return Int32ToString(n); }

// Converts a human-readable numeric string to an (u)int{32,64}. Dies on bad
// inputs.
int StringToInt(const Slice& s);

// Shorthand Scalar converters.
template<typename T>
Scalar ToScalar(const T& t);

template<typename T>
T FromScalar(const Scalar& s);

string ShowScalar(const Scalar& s);

// Used for fooling the g++ optimizer.
template<typename T>
void SpinUntilNE(T& t, const T& v);

template<typename T>
void Noop(T& t);

#endif  // CALVIN_COMMON_UTILS_H_

