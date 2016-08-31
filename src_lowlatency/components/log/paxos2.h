// Author: Alex Thomson (thomson@cs.yale.edu)
//

#ifndef CALVIN_COMPONENTS_LOG_PAXOS2_H_
#define CALVIN_COMPONENTS_LOG_PAXOS2_H_

#include <set>
#include <vector>
#include <map>

#include "common/atomic.h"
#include "common/mutex.h"
#include "common/types.h"
#include "components/log/log.h"
#include "components/log/log_app.h"
#include "machine/app/app.h"
#include "proto/start_app.pb.h"
#include "proto/scalar.pb.h"

using std::vector;
using std::map;

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

  uint32 replica_count;
  uint32 partitions_per_replica;

  AtomicQueue<MessageBuffer*> sequences_other_replicas;

  // Record the current sequence index
  uint64 local_sequences_index;
  // Map the local sequence index to the version.
  map<uint64, uint64> local_versions_index_table;
  // Map the replica ID to its locking running index
  AtomicMap<uint32, uint64> next_sequences_index;
  
};

#endif  // CALVIN_COMPONENTS_LOG_PAXOS2_H_
