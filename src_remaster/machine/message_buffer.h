// Author: Alex Thomson
//
// A MessageBuffer is a collection of zero or more MessageParts.
//
// A MessagePart is either like a std::string or like a std::Slice. It points
// to a buffer of bytes of some length, which it MAY OR MAY NOT own. The user
// must explicitly manage memory ownership, so use MessageParts carefully and
// only in critical path places where avoiding extra memcopies is important.
//
// Once created, a MessagePart is IMMUTABLE.
// Data pointed to by the MessagePart, may not be deleted or modified for
// the lifetime of the MessagePart, regardless of memory ownership.
//
// There are four ways to create a MessagePart:
//    1) give it a pointer to a buffer of which it does NOT take ownership
//    2) give it ownership of a buffer
//    3) give it ownership of a string (which owns a byte buffer)
//    4) give it ownership of a zmq::message_t (which owns a byte buffer)
//
// MessageBuffers are simple collections of MessageParts. They are not
// immutable once created since you can always append new parts to the end.

#ifndef CALVIN_MACHINE_MESSAGE_BUFFER_H_
#define CALVIN_MACHINE_MESSAGE_BUFFER_H_

#include <glog/logging.h>
#include <google/protobuf/message.h>
#include <string>
#include <vector>

#include "machine/connection/zmq_cpp.h"
#include "common/types.h"

using std::vector;

enum MessagePartType {
  NO_OWNERSHIP = 1,
  OWNS_BUFFER = 2,
  OWNS_STRING = 3,
  OWNS_ZMQ_MSG = 4,
};

class MessagePart {
 public:
  // Constructor 1: MessagePart does NOT take ownership of the Slice.
  explicit MessagePart(const Slice& s)
      : type_(NO_OWNERSHIP), buffer_(s), object_(NULL) {
  }

  // Constructor 2: MessagePart takes ownership of (heap-allocated) buffer
  // at location 'ptr' of size 'len'.
  //
  // Requires: Buffer was created by a call to 'malloc', which returned 'ptr'.
  MessagePart(char* ptr, int len)
      : type_(OWNS_BUFFER), buffer_(ptr, len), object_(NULL) {
  }

  // Constructor 3: MessagePart takes ownership of (heap-allocated) string.
  explicit MessagePart(string* s)
      : type_(OWNS_STRING), buffer_(*s), object_(reinterpret_cast<void*>(s)) {
  }

  // Constructor 4: MessagePart takes ownership of (heap-allocated) zmq message.
  explicit MessagePart(zmq::message_t* m)
      : type_(OWNS_ZMQ_MSG), buffer_((const char*)m->data(), m->size()),
        object_(reinterpret_cast<void*>(m)) {
  }

  ~MessagePart() {
    switch (type_) {
      case OWNS_BUFFER:
        if (!buffer_.empty()) {
          free(const_cast<char*>(buffer_.data()));
        }
        break;
      case OWNS_STRING:
        delete reinterpret_cast<string*>(object_);
        break;
      case OWNS_ZMQ_MSG:
        delete reinterpret_cast<zmq::message_t*>(object_);
        break;
      default:
        break;
    }
  }

  // Exposes the Slice pointing to the buffer
  inline const Slice& buffer() const {
    return buffer_;
  }

 private:
  // Specifies whether this message part owns memory or not, and in what form.
  MessagePartType type_;

  // Points to the buffer regardless of which constructor is used.
  // (Points to string_buffer_->data() if constructor 3 was used.)
  Slice buffer_;

  // Points to the string or zmq::message_t owned by the MessagePart iff
  // constructor 3 or 4 was used. NULL otherwise.
  void* object_;

  // DISALLOW_DEFAULT_CONSTRUCTOR
  MessagePart();

  // DISALLOW_COPY_AND_ASSIGN
  MessagePart(const MessagePart&);
  MessagePart& operator=(const MessagePart&);
};

class MessageBuffer {
 public:
  // Default: MessageBuffer containing no parts.
  MessageBuffer() {}

  // 1-arg constructors for single-part MessageBuffers.
  explicit MessageBuffer(const Slice& s) {
    parts_.push_back(new MessagePart(s));
  }
  explicit MessageBuffer(char* ptr, int len) {
    parts_.push_back(new MessagePart(ptr, len));
  }
  explicit MessageBuffer(string* s) {
    parts_.push_back(new MessagePart(s));
  }
  explicit MessageBuffer(zmq::message_t* m) {
    parts_.push_back(new MessagePart(m));
  }
  explicit MessageBuffer(const google::protobuf::Message& m) {
    string* s = new string();
    m.SerializeToString(s);
    parts_.push_back(new MessagePart(s));
  }

  ~MessageBuffer() {
    for (uint32 i = 0; i < parts_.size(); i++) {
      if (parts_[i] != NULL) {
        delete parts_[i];
      }
    }
  }

  // Adds a part to the end of the MessageBuffer.
  inline void Append(const Slice& s) {
    parts_.push_back(new MessagePart(s));
  }
  inline void Append(char* ptr, int len) {
    parts_.push_back(new MessagePart(ptr, len));
  }
  inline void Append(string* s) {
    parts_.push_back(new MessagePart(s));
  }
  inline void Append(zmq::message_t* m) {
    parts_.push_back(new MessagePart(m));
  }
  inline void Append(const google::protobuf::Message& m) {
    string* s = new string();
    m.SerializeToString(s);
    parts_.push_back(new MessagePart(s));
  }

  // This MessageBuffer takes ownership of '*part'.
  inline void AppendPart(MessagePart* part) {
    parts_.push_back(part);
  }

  // Returns the number of parts contained in the MessageBuffer.
  inline uint32 size() const {
    return parts_.size();
  }

  // Returns false iff message contains at least one part (even if that part is
  // of length 0).
  inline bool empty() const {
    return parts_.empty();
  }

  // Erases and removes all message parts.
  inline void clear() {
    while (!empty()) {
      delete PopBack();
    }
  }

  // Returns the ith message part.
  inline const Slice& operator[](uint32 i) const {
    CHECK(i < parts_.size());
    CHECK(parts_[i] != NULL);
    return parts_[i]->buffer();
  }

  // Returns the explicit MessagePart object constituting the ith part.
  inline const MessagePart& GetPart(uint32 i) const {
    CHECK(i < parts_.size());
    CHECK(parts_[i] != NULL);
    return *parts_[i];
  }

  // Caller takes ownership of the ith MessagePart. That part can no longer be
  // accessed from the MessageBuffer.
  inline MessagePart* StealPart(uint32 i) {
    CHECK(i < parts_.size());
    CHECK(parts_[i] != NULL);
    MessagePart* part = parts_[i];
    parts_[i] = NULL;
    return part;
  }

  // Caller takes ownership of the last MessagePart. That part is removed from
  // the MessageBuffer.
  inline MessagePart* PopBack() {
    CHECK(!parts_.empty());
    MessagePart* part = parts_.back();
    parts_.resize(parts_.size() - 1);
    return part;
  }

  inline bool operator==(const MessageBuffer& other) const {
    if (size() != other.size()) {
      return false;
    }
    for (uint32 i = 0; i < other.size(); i++) {
      if (GetPart(i).buffer() != other[i]) {
        return false;
      }
    }
    return true;
  }

 private:
  // Parts of message.
  vector<MessagePart*> parts_;

  // DISALLOW_COPY_AND_ASSIGN
  MessageBuffer(const MessageBuffer&);
  MessageBuffer& operator=(const MessageBuffer&);
};

// Log all parts delimited by spaces.
inline std::ostream& operator<<(
    std::ostream& out,
    const MessageBuffer& m) {
  for (uint32 i = 0; i < m.size(); i++) {
    out << m.GetPart(i).buffer().ToString();
    if (i != m.size() - 1) {
      out << " ";
    }
  }
  return out;
}

#endif  // CALVIN_MACHINE_MESSAGE_BUFFER_H_

