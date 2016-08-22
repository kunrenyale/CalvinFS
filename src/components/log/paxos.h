// Author: Alex Thomson (thomson@cs.yale.edu)
//

#ifndef CALVIN_COMPONENTS_LOG_PAXOS_H_
#define CALVIN_COMPONENTS_LOG_PAXOS_H_

#include <set>
#include <vector>

#include "common/types.h"
#include "components/log/log.h"
#include "components/log/log_app.h"
#include "machine/app/app.h"
#include "proto/start_app.pb.h"

using std::vector;

class Header;
class Machine;
class MessageBuffer;

class FakePaxosApp : public LogApp {
 public:
  FakePaxosApp(Log* log, const vector<uint64>& participants);
  FakePaxosApp(Log* log, uint64 count);
  virtual ~FakePaxosApp() {}
  virtual void Start();
  virtual void Stop();
  virtual void Append(const Slice& entry, uint64 count = 1);

 private:
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
};

#endif  // CALVIN_COMPONENTS_LOG_PAXOS_H_
