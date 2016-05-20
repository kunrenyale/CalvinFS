// Author: Alex Thomson
//
// Treats a Log (a sequence of string entries) as a Source<T*> where T is a
// protobuf type.

#ifndef CALVIN_COMPONENTS_LOG_LOG_SOURCE_H_
#define CALVIN_COMPONENTS_LOG_LOG_SOURCE_H_

#include "common/source.h"
#include "components/log/log.h"

template<class T>
class LogSource : public Source<T*> {
 public:
  // Takes ownership of 'reader'.
  explicit LogSource(Log::Reader* reader) : reader_(reader) {
    reader_->Reset();
  }
  explicit LogSource(Log* log) : reader_(log->GetReader()) {}

  virtual ~LogSource() {
    delete reader_;
  }

  virtual bool Get(T** t) {
    if (reader_->Next()) {
      *t = new T();
      (*t)->ParseFromArray(reader_->Entry().data(), reader_->Entry().size());
      return true;
    }
    return false;
  }

 private:
  // Reader of underlying log.
  Log::Reader* reader_;
};

#endif  // CALVIN_COMPONENTS_LOG_LOG_SOURCE_H_

