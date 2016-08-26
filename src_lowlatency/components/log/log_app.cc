// Author: Alex Thomson
//

#include "components/log/log_app.h"

#include "common/source.h"
#include "common/utils.h"
#include "components/log/local_mem_log.h"
#include "machine/app/app.h"
#include "machine/machine.h"
#include "proto/action.pb.h"

//////////////////////////////      LogApp      //////////////////////////////

// Make LogApp startable.
REGISTER_APP(LogApp) {
  return new LogApp(new LocalMemLog());
}

LogApp::~LogApp() {
  for (auto it = remote_readers_.begin(); it != remote_readers_.end(); ++it) {
    delete it->second;
  }
  delete log_;
}

bool LogApp::HandleRemoteReaderMessage(Header* header, MessageBuffer* message) {
  if (header->rpc() == "NEW_READER") {
    Lock l(&rr_mutex_);
    remote_readers_[make_pair(header->from(), header->data_channel())] =
        log_->GetReader();
    machine()->SendReplyMessage(header, message);

  } else if (header->rpc() == "GET") {
    Log::Reader* r =
        remote_readers_[make_pair(header->from(), header->data_channel())];
    if (r->Next()) {
      message->Append(r->Entry());
      message->Append(new string(UInt64ToString(r->Version())));
    }
    machine()->SendReplyMessage(header, message);

  } else {
    return false;
  }

  return true;
}

void LogApp::Append(const Slice& entry, uint64 count) {
  static atomic<uint64> next_(1);
  log_->Append((next_+=count) - count, entry);
}

//////////////////////////////  RemoteLogSource  //////////////////////////////

// Generic constructor.
template<class T>
RemoteLogSource<T>::RemoteLogSource(Machine* machine,
                                    uint64 source_machine,
                                    const string& source_app_name)
  : machine_(machine),
    source_machine_(source_machine),
    source_app_name_(source_app_name) {
  Init();
}

// Specialized constructors.
#define SPECIALIZED_CONSTRUCTOR(TypeParam) \
template<> \
RemoteLogSource<TypeParam>::RemoteLogSource(Machine* machine, \
                                            uint64 source_machine, \
                                            const string& source_app_name) \
  : machine_(machine), \
    source_machine_(source_machine), \
    source_app_name_(source_app_name) { \
  Init(); \
}

SPECIALIZED_CONSTRUCTOR(string)
SPECIALIZED_CONSTRUCTOR(Action)
SPECIALIZED_CONSTRUCTOR(UInt64Pair)
SPECIALIZED_CONSTRUCTOR(PairSequence)

template<class T>
void RemoteLogSource<T>::Init() {
  // Get inbox ptr.
  inbox_ = machine_->DataChannel(
      source_app_name_ + UInt64ToString(source_machine_));

  // Request new session from source app.
  Header* header = new Header();
  header->set_from(machine_->machine_id());
  header->set_to(source_machine_);
  header->set_type(Header::RPC);
  header->set_app(source_app_name_);
  header->set_rpc("NEW_READER");
  header->set_data_channel(source_app_name_ + UInt64ToString(source_machine_));
  machine_->SendMessage(header, new MessageBuffer());

  // Wait for response.
  MessageBuffer* m = NULL;
  while (!inbox_->Pop(&m)) {
    usleep(10);
  }
  delete m;
}

// Helper method for Get.
template<typename T>
T* ParseFromMessageBuffer(MessageBuffer* m);

// Specialization: string (for succinct testing)
template<>
string* ParseFromMessageBuffer<string>(MessageBuffer* m) {
  return new string((*m)[0].data(), (*m)[0].size());
}

// Specialization: Action
template<>
Action* ParseFromMessageBuffer<Action>(MessageBuffer* m) {
  CHECK(m->size() == 2);
  Action* a = new Action();
  a->ParseFromArray((*m)[0].data(), (*m)[0].size());
  a->set_version(StringToInt((*m)[1]));
  return a;
}

// Specialization: UInt64Pair
template<>
UInt64Pair* ParseFromMessageBuffer<UInt64Pair>(MessageBuffer* m) {
  CHECK(m->size() == 2);
  UInt64Pair* p = new UInt64Pair();
  Scalar s;
  s.ParseFromArray((*m)[0].data(), (*m)[0].size());
  p->set_first(FromScalar<uint64>(s));
  p->set_second(StringToInt((*m)[1]));
  return p;
}

// Specialization: PairSequence
template<>
PairSequence* ParseFromMessageBuffer<PairSequence>(MessageBuffer* m) {
  CHECK(m->size() == 2);
  PairSequence* p = new PairSequence();
  p->ParseFromArray((*m)[0].data(), (*m)[0].size());
  p->set_misc(StringToInt((*m)[1]));
  return p;
}

template<typename T>
T* ParseFromMessageBuffer(MessageBuffer* m) {
  T* t = new T();
  t->ParseFromArray((*m)[0].data(), (*m)[0].size());
  return t;
}

template<class T>
bool RemoteLogSource<T>::Get(T** t) {
  // Maybe there is already an entry in the queue.
  MessageBuffer* m = NULL;
  if (!inbox_->Pop(&m)) {
    // Nope. Request new entry from source app.
    Header* header = new Header();
    header->set_from(machine_->machine_id());
    header->set_to(source_machine_);
    header->set_type(Header::RPC);
    header->set_app(source_app_name_);
    header->set_rpc("GET");
    header->set_data_channel(source_app_name_ + UInt64ToString(source_machine_));
    machine_->SendMessage(header, new MessageBuffer());

    // Wait for something to show up.
    while (!inbox_->Pop(&m)) {
      usleep(10);
    }
  }

  // Parse response.
  if (m->size() != 0) {
    *t = ParseFromMessageBuffer<T>(m);
    delete m;
    return true;
  }

  delete m;
  return false;
}

