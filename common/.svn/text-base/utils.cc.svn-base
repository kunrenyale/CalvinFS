// Author: Alexander Thomson <thomson@cs.yale.edu>
// Author: Thaddeus Diamond <diamond@cs.yale.edu>

#include "common/utils.h"

#include <cstdio>

template<typename T> string TypeName() {
  return "?";
}
ADD_TYPE_NAME(int32);
ADD_TYPE_NAME(string);
ADD_TYPE_NAME(Slice);

vector<string> SplitString(const string& input, char delimiter) {
  string current;
  vector<string> result;
  for (uint32 i = 0; i < input.size(); i++) {
    if (input[i] == delimiter) {
      // reached delimiter
      result.push_back(current);
      current.clear();
    } else {
      current.push_back(input[i]);
    }
  }
  result.push_back(current);
  return result;
}

double GetTime() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec + tv.tv_usec/1e6;
}

uint32 FNVHash(const Slice& key) {
  uint32 hash = 2166136261;                       // FNV Hash offset
  for (uint32 i = 0; i < key.size(); i++) {
    hash = (hash * 1099511628211) ^ key[i];       // H(x) = H(x-1) * FNV' XOR x
  }
  return hash;
}

uint32 FNVModHash(const Slice& key) {
  uint32 hash = 2166136261;                       // FNV Hash offset
  for (uint32 i = 0; i < key.size(); i++) {
    hash = (hash ^ key[i]) * 1099511628211;       // H(x) = H(x-1) * FNV' XOR x
  }
  return hash;
}

void Spin(double duration) {
  usleep(1000000 * duration);
}

void SpinUntil(double time) {
  if (time > GetTime()) {
    usleep(1000000 * (time - GetTime()));
  }
}

string RandomString(int length) {
  string s;
  for (int i = 0; i < length; i++) {
    s += rand() % 26 + 'A';
  }
  return s;
}

string RandomBytes(int length) {
  string s;
  for (int i = 0; i < length; i++) {
    s += rand() % 256;
  }
  return s;
}

string RandomBytesNoZeros(int length) {
  string s;
  for (int i = 0; i < length; i++) {
    s += 1 + rand() % 255;
  }
  return s;
}

#define RANF() ((double)rand()/(1.0+(double)RAND_MAX))
double RandomGaussian(double s) {
  double x1, x2, w, z;
  do {
      x1 = 2.0 * RANF() - 1.0;
      x2 = 2.0 * RANF() - 1.0;
      w = x1 * x1 + x2 * x2;
  } while ( w >= 1.0 );
  return s * x1 * sqrt( (-2.0 * log( w ) ) / w );
  z = 0.5 + s * x1 * sqrt( (-2.0 * log( w ) ) / w );
  if (z < 0 || z >= 1) return RANF();
  else return z;
}

string Int32ToString(int32 n) {
  char s[64];
  snprintf(s, sizeof(s), "%d", n);
  return string(s);
}

string Int64ToString(int64 n) {
  char s[64];
  snprintf(s, sizeof(s), "%ld", n);
  return string(s);
}

string UInt32ToString(uint32 n) {
  char s[64];
  snprintf(s, sizeof(s), "%u", n);
  return string(s);
}

string UInt64ToString(uint64 n) {
  char s[64];
  snprintf(s, sizeof(s), "%lu", n);
  return string(s);
}

string FloatToString(float n) {
  char s[64];
  snprintf(s, sizeof(s), "%f", n);
  return string(s);
}

string DoubleToString(double n) {
  char s[64];
  snprintf(s, sizeof(s), "%f", n);
  return string(s);
}

int StringToInt(const Slice& s) {
  int x = strtol(s.ToString().c_str(), NULL, 10);
  if (IntToString(x) != s.ToString()) {
    LOG(FATAL) << "invalid numeric string: " << s;
  }
  CHECK_EQ(IntToString(x), s.ToString()) << "invalid numeric string: " << s;
  return x;
}

/////////////////////////////    ToScalar    /////////////////////////////

// Unknown type -> UNIT (and fatal error)
template<typename T>
Scalar ToScalar(const T& t) {
  LOG(FATAL) << "Illegal template used in FromScalar";
  Scalar s;
  s.set_type(Scalar::UNIT);
  return s;
}

// Specialization: bool
template<>
Scalar ToScalar<bool>(const bool& b) {
  Scalar s;
  s.set_type(Scalar::BOOL);
  s.set_bool_value(b);
  return s;
}

// Specialization: int32
template<>
Scalar ToScalar<int32>(const int32& b) {
  Scalar s;
  s.set_type(Scalar::INT32);
  s.set_int32_value(b);
  return s;
}

// Specialization: int64
template<>
Scalar ToScalar<int64>(const int64& b) {
  Scalar s;
  s.set_type(Scalar::INT64);
  s.set_int64_value(b);
  return s;
}

// Specialization: uint32
template<>
Scalar ToScalar<uint32>(const uint32& b) {
  Scalar s;
  s.set_type(Scalar::UINT32);
  s.set_uint32_value(b);
  return s;
}

// Specialization: uint64
template<>
Scalar ToScalar<uint64>(const uint64& b) {
  Scalar s;
  s.set_type(Scalar::UINT64);
  s.set_uint64_value(b);
  return s;
}

// Specialization: float
template<>
Scalar ToScalar<float>(const float& b) {
  Scalar s;
  s.set_type(Scalar::FLOAT);
  s.set_float_value(b);
  return s;
}

// Specialization: double
template<>
Scalar ToScalar<double>(const double& b) {
  Scalar s;
  s.set_type(Scalar::DOUBLE);
  s.set_double_value(b);
  return s;
}

// Specialization: string
template<>
Scalar ToScalar<string>(const string& b) {
  Scalar s;
  s.set_type(Scalar::STRING);
  s.set_string_value(b);
  return s;
}

/////////////////////////////   FromScalar   /////////////////////////////

// Unknown type -> fatal error (returns default value)
template<typename T>
T FromScalar(const Scalar& s) {
  LOG(FATAL) << "Illegal template used in FromScalar";
  return T();
}

template<>
bool FromScalar<bool>(const Scalar& s) {
  CHECK_EQ(Scalar::BOOL, s.type());
  return s.bool_value();
}

template<>
int32 FromScalar<int32>(const Scalar& s) {
  CHECK_EQ(Scalar::INT32, s.type());
  return s.int32_value();
}

template<>
int64 FromScalar<int64>(const Scalar& s) {
  CHECK_EQ(Scalar::INT64, s.type());
  return s.int64_value();
}

template<>
uint32 FromScalar<uint32>(const Scalar& s) {
  CHECK_EQ(Scalar::UINT32, s.type());
  return s.uint32_value();
}

template<>
uint64 FromScalar<uint64>(const Scalar& s) {
  CHECK_EQ(Scalar::UINT64, s.type());
  return s.uint64_value();
}

template<>
float FromScalar<float>(const Scalar& s) {
  CHECK_EQ(Scalar::FLOAT, s.type());
  return s.float_value();
}
template<>
double FromScalar<double>(const Scalar& s) {
  CHECK_EQ(Scalar::DOUBLE, s.type());
  return s.double_value();
}

template<>
string FromScalar<string>(const Scalar& s) {
  CHECK_EQ(Scalar::STRING, s.type());
  return s.string_value();
}

string ShowScalar(const Scalar& s) {
  switch (s.type()) {
    case Scalar::UNIT  : return "";
    case Scalar::BOOL  : return s.bool_value() ? "true" : "false";
    case Scalar::INT32 : return Int32ToString(s.int32_value());
    case Scalar::INT64 : return Int64ToString(s.int64_value());
    case Scalar::UINT32: return UInt32ToString(s.uint32_value());
    case Scalar::UINT64: return UInt64ToString(s.uint64_value());
    case Scalar::FLOAT : return FloatToString(s.float_value());
    case Scalar::DOUBLE: return DoubleToString(s.double_value());
    case Scalar::STRING: return s.string_value();
    default            : return "";
  }
}

///////////////////////////////////////////////////////////////////////////

template<typename T>
void SpinUntilNE(T& t, const T& v) {
  while (t == v) {
    usleep(10);
  }
}

class MessageBuffer;
template<>
void SpinUntilNE<MessageBuffer*>(
    MessageBuffer*& t,
    MessageBuffer* const& v) {
  while (t == v) {
    usleep(10);
  }
}

template<typename T> void Noop(T& t) {}
template<> void Noop<MessageBuffer*>(MessageBuffer*& t) {}
template<> void Noop<bool>(bool& t) {}
template<typename T> class AtomicQueue;
template<> void Noop<AtomicQueue<int>*>(AtomicQueue<int>*& t) {}

