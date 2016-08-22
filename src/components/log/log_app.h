// Author: Alex Thomson
//
// An App that owns a Log, and a way of remotely reading that Log.

#ifndef CALVIN_COMPONENTS_LOG_LOG_APP_H_
#define CALVIN_COMPONENTS_LOG_LOG_APP_H_

#include <atomic>
#include <map>

#include "common/mutex.h"
#include "common/source.h"
#include "common/types.h"
#include "components/log/log.h"
#include "machine/app/app.h"
#include "machine/machine.h"

using std::atomic;
using std::map;
using std::pair;

struct RemoteReaderState;

class LogApp : public App {
 private:
  friend class LogAppTest;

 public:
  // Takes ownership of '*log'.
  explicit LogApp(Log* log) : log_(log) {}
  virtual ~LogApp();

  // Subclasses of LogApp may NOT override this HandleMessage implementation.
  // However, they may extend RPC functionality by overriding the
  // 'HandleOtherMessages' method below.
  virtual void HandleMessage(Header* header, MessageBuffer* message) {
    if (!HandleRemoteReaderMessage(header, message)) {
      HandleOtherMessages(header, message);
    }
  }

  virtual void Append(const Slice& entry, uint64 count = 1);
  virtual Log::Reader* GetReader() {
    return log_->GetReader();
  }

 protected:
  // Subclasses of LogApp may override this method to implement additional RPC
  // functionality.
  virtual void HandleOtherMessages(Header* header, MessageBuffer* message) {
    LOG(FATAL) << "unknown message type";
  }

  // Underlying log.
  Log* log_;

  // Only subclasses may use default constructor.
  LogApp() {}

 private:
  // Remote reader-related RPCs are handled here.
  bool HandleRemoteReaderMessage(Header* header, MessageBuffer* message);

  // Remote readers of the log.
  // TODO(agt): Make this thread safe!
  Mutex rr_mutex_;
  map<pair<uint64, string>, Log::Reader*> remote_readers_;
};

template<class T>
class RemoteLogSource : public Source<T*> {
 public:
  RemoteLogSource(
      Machine* machine,
      uint64 source_machine,
      const string& source_app_name);

  virtual ~RemoteLogSource() {}
  virtual bool Get(T** t);

 private:
  // Initialization method called by constructors.
  void Init();

  // Local machine.
  Machine* machine_;

  // ID of Machine on which the log resides.
  uint64 source_machine_;

  // Name of app on source machine.
  string source_app_name_;

  // Local machine's data channel on which to receive entries.
  AtomicQueue<MessageBuffer*>* inbox_;
};

#endif  // CALVIN_COMPONENTS_LOG_LOG_APP_H_

