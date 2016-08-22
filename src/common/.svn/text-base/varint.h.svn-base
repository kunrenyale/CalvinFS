// Author Alexander Thomson <thomson@cs.yale.edu>
//
// CVarint is a variable-length encoding of integers, depending on the size.
// This is based on google Varint library, which I couldn't find, although I
// found public references to it, and I think that protobufs uses it. See
// https://developers.google.com/protocol-buffers/docs/encoding for google's
// documentation on how Varints work.
//
// The general idea here, though is this. Say you want to encode a sequence of
// uint64s (most of which are actually pretty small) into a blob. Here's what
// you do:
//
//   // Create a string to store the encoded sequence.
//   string s;
//   // Append some numbers.
//   CVarint::Append64(&s, 91);
//   CVarint::Append64(&s, 119);
//   CVarint::Append64(&s, 267);
//
//   // Now let's read these numbers back.
//   uint64 x, y, z;
//   const char* pos = s.data();  // Tracks our current offset into the string.
//   pos = CVarint::Parse64(pos, &x);  //  x now stores 91
//   pos = CVarint::Parse64(pos, &y);  //  y now stores 119
//   pos = CVarint::Parse64(pos, &z);  //  z now stores 267
//
// TODO(Alex): Find Google implementation of this to use instead; it's
//              probably way better!

#ifndef CALVIN_COMMON_VARINT_H_
#define CALVIN_COMMON_VARINT_H_

#include <string>
#include "common/types.h"

using std::string;

namespace varint {

// Appends a variable-length encoding of x to *s.
inline static void Append64(string* s, uint64 x) {
  for (int i = 0; x & ~127 && i < 8; i++) {
    // If x takes > 7 bits to encode. Append a 1 followed by the 7 lowest-
    // order bits of x.
    s->append(1, (1<<7) | static_cast<uint8>(x));
    // Then throw away those lowest order bits and repeat for the next 7 bits.
    x >>= 7;
  }
  // What remains of x takes <= 7 bits to encode. Append a 0 followed by the
  // 7 lowest-order bits of x.
  s->append(1, static_cast<uint8>(x));
}

// Reads the next var-length encoded value into '*x'. Returns a pointer to
// the first character following the varint encoding that was just read.
inline static const char* Parse64(const char* pos, uint64* x) {
  *x = 0;
  int offset = 0;

  // First check if this encodes to only one byte (possibly a common case?).
  if (!(*pos & (1<<7))) {
    *x = *pos;
    return pos + 1;
  }

  // If not, do it the slow way.
  while ((*pos) & (1<<7) && offset < 56) {
    // Read this byte (except for the first bit into x).
    uint64 bits = *pos & 127;
    *x |= bits << offset;
    pos++;
    // This is NOT the last byte we're reading, so shift everything we've read
    // so far left.
    offset += 7;
  }
  // This is the last byte (thus it starts with 0, so we don't need to
  // bitwise-and it with 127---OR it starts with 1 because the highest order
  // of the top bit started with 1, so that's ok too).
  uint64 bits = *pos;
  *x |= bits << offset;

  // Return a pointer to the next char after the last one that was part of x.
  return pos + 1;
}

}  // namespace varint

#endif  // CALVIN_COMMON_VARINT_H_

