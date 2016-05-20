// Author: Alex Thomson (thomson@cs.yale.edu)
//

#ifndef CALVIN_COMPONENTS_LOG_PAXOS2_H_
#define CALVIN_COMPONENTS_LOG_PAXOS2_H_

#include <set>
#include <vector>

#include "common/mutex.h"
#include "common/types.h"
#include "components/log/log.h"
#include "components/log/log_app.h"
#include "machine/app/app.h"
#include "proto/start_app.pb.h"
#include "proto/scalar.pb.h"

using std::vector;

class Header;
class Machine;
class MessageBuffer;

class Paxos2App : public LogApp {
 public:
  Paxos2App(Log* log, const vector<uint64>& participants);
  Paxos2App(Log* log, uint64 count);
  virtual ~Paxos2App() {
    Stop();
  }
  virtual void Start();
  virtual void Stop();
  void Append(uint64 blockid, uint64 count = 1);

 protected:
  virtual void HandleOtherMessages(Header* header, MessageBuffer* message);

  // Returns true iff leader.
  bool IsLeader();

  // Leader's main loop.
  void RunLeader();

  // Followers' main loop.
  void RunFollower();

  // Participant list.
  vector<uint64> participants_;

  // True iff main thread SHOULD run.
  std::atomic<bool> go_;

  // True iff main thread IS running.
  std::atomic<bool> going_;

  // Current request sequence that will get replicated.
  PairSequence sequence_;
  std::atomic<uint64> count_;
  Mutex mutex_;
};

#endif  // CALVIN_COMPONENTS_LOG_PAXOS2_H_
