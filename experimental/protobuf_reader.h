// Author: Alexander Thomson <thomson@cs.yale.edu>
//

#ifndef CALVIN_EXPERIMENTAL_PROTOBUF_READER_H_
#define CALVIN_EXPERIMENTAL_PROTOBUF_READER_H_

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/wire_format.h>
#include <google/protobuf/wire_format_lite.h>
#include <leveldb/slice.h>

#include <string>

#include "common/types.h"

using google::protobuf::io::CodedInputStream;
using google::protobuf::internal::WireFormat;
using google::protobuf::internal::WireFormatLite;
using leveldb::Slice;
using std::string;

class ProtobufReader {
 public:
  ProtobufReader() {}
  explicit ProtobufReader(const Slice& encoded) : encoded_(encoded) {}

  template<typename T>
  bool ReadField(uint32 field, T* value);

  template<typename T>
  bool ReadRepeatedField(uint32 field, int index, T* value);

 private:
  template<typename T>
  bool ReadValue(CodedInputStream* input, T* value);

  Slice encoded_;

  // Intentionally copyable.
};

////////////////////////////////////////////////////////////////////////////////

template<typename T>
void CheckTag(uint32 tag) {
  LOG(FATAL) << "CheckTag<T>: unknown template type";
}

// Specialization: uint32
template<>
void CheckTag<uint32>(uint32 tag) {
  CHECK_EQ(WireFormatLite::WIRETYPE_VARINT,
           WireFormatLite::GetTagWireType(tag));
}

// Specialization: string
template<>
void CheckTag<string>(uint32 tag) {
  CHECK_EQ(WireFormatLite::WIRETYPE_LENGTH_DELIMITED,
           WireFormatLite::GetTagWireType(tag));
}

// Specialization: Slice
template<>
void CheckTag<Slice>(uint32 tag) {
  CHECK_EQ(WireFormatLite::WIRETYPE_LENGTH_DELIMITED,
           WireFormatLite::GetTagWireType(tag));
}

// Specialization: ProtobufReader
template<>
void CheckTag<ProtobufReader>(uint32 tag) {
  CHECK_EQ(WireFormatLite::WIRETYPE_LENGTH_DELIMITED,
           WireFormatLite::GetTagWireType(tag));
}

template<typename T>
bool ProtobufReader::ReadValue(CodedInputStream* input, T* value) {
  LOG(FATAL) << "ReadValue<T>: unknown template type";
  return false;
}

////////////////////////////////////////////////////////////////////////////////

// Specialization: uint32
template<>
bool ProtobufReader::ReadValue<uint32>(CodedInputStream* input, uint32* value) {
  return input->ReadVarint32(value);
}

// Specialization: string
template<>
bool ProtobufReader::ReadValue<string>(CodedInputStream* input, string* value) {
  uint32 length;
  if (!input->ReadVarint32(&length)) return false;
  value->assign(encoded_.data() + input->CurrentPosition(), length);
  return input->Skip(length);
}

// Specialization: Slice
template<>
bool ProtobufReader::ReadValue<Slice>(CodedInputStream* input, Slice* value) {
  uint32 length;
  if (!input->ReadVarint32(&length)) return false;
  *value = Slice(encoded_.data() + input->CurrentPosition(), length);
  return input->Skip(length);
}

// Specialization: ProtobufReader
template<>
bool ProtobufReader::ReadValue<ProtobufReader>(
    CodedInputStream* input,
    ProtobufReader* value) {
  uint32 length;
  if (!input->ReadVarint32(&length)) return false;
  *value = ProtobufReader(Slice(encoded_.data() + input->CurrentPosition(),
                          length));
  return input->Skip(length);
}

////////////////////////////////////////////////////////////////////////////////

template<typename T>
bool ProtobufReader::ReadField(uint32 field, T* value) {
  CodedInputStream input(
      reinterpret_cast<const uint8*>(encoded_.data()),
      encoded_.size());
  while (input.CurrentPosition() < (int)encoded_.size()) {
    uint32 tag = input.ReadTag();
    if (tag >> 3 == field) {
      return ReadValue<T>(&input, value);
    }
    WireFormat::SkipField(&input, tag, NULL);
  }
  return false;
}

template<typename T>
bool ProtobufReader::ReadRepeatedField(uint32 field, int index, T* value) {
  int current = 0;
  CodedInputStream input(
      reinterpret_cast<const uint8*>(encoded_.data()),
      encoded_.size());
  while (input.CurrentPosition() < (int)encoded_.size()) {
    uint32 tag = input.ReadTag();
    if (tag >> 3 == field) {
      if (current == index) {
        return ReadValue<T>(&input, value);
      } else {
        current++;
      }
    }
    WireFormat::SkipField(&input, tag, NULL);
  }
  return false;
}

#endif  // CALVIN_EXPERIMENTAL_PROTOBUF_READER_H_

