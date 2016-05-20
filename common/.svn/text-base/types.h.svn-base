// Author: Alex Thomson
//
// Just some shorthand typedefs for commonly used types.

#ifndef CALVIN_COMMON_TYPES_H_
#define CALVIN_COMMON_TYPES_H_

#include <stdint.h>
#include <iostream>  // NOLINT
#include <string>
#include "leveldb/slice.h"

// Slice and string are common enough that they're worth including here.
using std::string;
using leveldb::Slice;

// Abbreviated signed int types.
typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;

// Abbreviated unsigned int types.
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;

// Slices should be loggable.
inline std::ostream& operator<<(std::ostream& out, const Slice& s) {
  return out << s.ToString();
}

#endif  // CALVIN_COMMON_TYPES_H_

