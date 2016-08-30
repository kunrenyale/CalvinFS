// Author: Alex Thomson (thomson@cs.yale.edu)
//

#include "components/log/paxos2.h"

#include <atomic>
#include <glog/logging.h>
#include <google/protobuf/message.h>
#include <queue>
#include <set>
#include <utility>
#include <vector>

#include "common/types.h"
#include "components/log/log.h"
#include "components/log/log_reader.pb.h"
#include "components/log/local_mem_log.h"
#include "machine/machine.h"
#include "machine/message_buffer.h"
#include "proto/header.pb.h"
#include "proto/scalar.pb.h"


using std::atomic;
using std::make_pair;
using std::pair;
using std::queue;
using std::set;
using std::vector;

// Make Paxos2App startable.
REGISTER_APP(Paxos2App) {
  Scalar s;
  s.ParseFromString(ARG);
  return new Paxos2App(new LocalMemLog(), FromScalar<uint64>(s));
}

REGISTER_APP(Paxos2App2) {
  StartAppProto sap;
  sap.ParseFromString(ARG);
  vector<uint64> participants;
  for (int i = 0; i < sap.participants_size(); i++) {
    participants.push_back(sap.participants(i));
  }
  return new Paxos2App(new LocalMemLog(), participants);
}

Paxos2App::Paxos2App(Log* log, const vector<uint64>& participants)
    : participants_(participants), go_(true), going_(false), count_(0) {
  log_ = log;
}

Paxos2App::Paxos2App(Log* log, uint64 count)
    : go_(true), going_(false), count_(0) {
  log_ = log;
  for (uint64 i = 0; i < count; i++) {
    participants_.push_back(i);
  }
}

bool Paxos2App::IsLeader() {
  return machine()->machine_id() == participants_[0];
}

void Paxos2App::Append(uint64 blockid, uint64 count) {
  // Forward append request to leader.
  Header* header = new Header();
  header->set_from(machine()->machine_id());
  header->set_to(participants_[0]);
  header->set_type(Header::RPC);
  header->set_app(name());
  header->set_rpc("APPEND");
  header->add_misc_int(blockid);
  header->add_misc_int(count);
  machine()->SendMessage(header, new MessageBuffer());
}

void Paxos2App::Start() {
  going_ = true;
  replica_count = (machine()->config().size() >= 3) ? 3 : 1;
  partitions_per_replica = machine()->config().size() / replica_count;

  if (IsLeader()) {
    RunLeader();
  } else {
    RunFollower();
  }
  going_ = false;
}

void Paxos2App::Stop() {
  go_ = false;
  while (going_.load()) {
    // Wait for main loop to stop.
    usleep(10);
  }
}

void Paxos2App::HandleOtherMessages(Header* header, MessageBuffer* message) {
  if (header->rpc() == "APPEND") {
    Lock l(&mutex_);
    UInt64Pair* p = sequence_.add_pairs();
    p->set_first(header->misc_int(0));
    p->set_second(header->misc_int(1));
    count_ += p->second();

  } else if (header->rpc() == "NEW-SEQUENCE") { 
    sequences_other_replicas.Push(message);   
  }else {
    LOG(FATAL) << "unknown message type: " << header->rpc();
  }

  delete header;
  delete message;
}

void Paxos2App::RunLeader() {
  uint64 next_version = 1;
  int quorum = static_cast<int>(participants_.size()) / 2 + 1;
  set<atomic<int>*> ack_ptrs;
  MessageBuffer* m = NULL;
  bool isFirst = true;
  bool isLocal = false;

  while (go_.load()) {
    // Sleep while there are NO requests.
    while (count_.load() == 0 && sequences_other_replicas.Size() == 0) {
      usleep(10);
      if (!go_.load()) {
        return;
      }
    }

    string encoded;
    uint64 version;
    if (count_.load() != 0) {
      // Propose a new sequence.
      {
        Lock l(&mutex_);
        version = next_version;
        next_version += count_.load();
        count_ = 0;
        sequence_.set_misc(version);
        sequence_.SerializeToString(&encoded);
        sequence_.Clear();
        isLocal = true;
      }
    } else if (sequences_other_replicas.Size() != 0) {
      sequences_other_replicas.Pop(&m);
      encoded = ((*m)[0]).ToString();
      isLocal = false;
    }

    atomic<int>* acks = new atomic<int>(1);
    ack_ptrs.insert(acks);
    for (uint32 i = 1; i < participants_.size(); i++) {
      Header* h = new Header();
      h->set_from(machine()->machine_id());
      h->set_to(participants_[i]);
      h->set_type(Header::DATA);
      h->set_data_channel("paxos2");
      m = new MessageBuffer(new string(encoded));
      m->Append(ToScalar<uint64>(version));
      m->Append(ToScalar<uint64>(reinterpret_cast<uint64>(acks)));
      machine()->SendMessage(h, m);
    }

    // Collect Acks.
    while (acks->load() < quorum) {
      usleep(10);
      if (!go_.load()) {
        return;
      }
    }

    // Commit!
    for (uint32 i = 1; i < participants_.size(); i++) {
      Header* h = new Header();
      h->set_from(machine()->machine_id());
      h->set_to(participants_[i]);
      h->set_type(Header::DATA);
      h->set_data_channel("paxos2");
      h->set_ack_counter(reinterpret_cast<uint64>(acks));
      machine()->SendMessage(h, new MessageBuffer());
    }
    log_->Append(version, encoded);

    if (isLocal == true && isFirst == true) {
      // Send the sequence to the LeaderPaxosApp of all the other replicas;

      for (uint64 i = 0; i < replica_count;i++) {
        if (i != machine()->machine_id()/partitions_per_replica) {
          Header* header = new Header();
          header->set_from(machine()->machine_id());
          header->set_to(i*partitions_per_replica);
          header->set_type(Header::RPC);
          header->set_app(name());
          header->set_rpc("NEW-SEQUENCE");
          m = new MessageBuffer(new string(encoded));
	  m->Append(ToScalar<uint64>(version));
          machine()->SendMessage(header, m);
	}
      }

      isFirst = false;
    } else if (isLocal == false) {
      
    }



//    // Clean up old ack counters.
//    while (!ack_ptrs.empty() &&
//           (*ack_ptrs.begin())->load() == (int)participants_.size()) {
//      delete *ack_ptrs.begin();
//      ack_ptrs.erase(ack_ptrs.begin());
//    }
  }
}

void Paxos2App::RunFollower() {
  auto channel = machine()->DataChannel("paxos2");
  queue<MessageBuffer*> uncommitted;
  while (go_.load()) {
    // Get message from leader.
    MessageBuffer* m = NULL;
    while (!channel->Pop(&m)) {
      usleep(10);
      if (!go_.load()) {
        return;
      }
    }
    if (m->size() == 3) {
      // New proposal.
      uncommitted.push(m);
      // Send ack to leader.
      Header* h = new Header();
      h->set_from(machine()->machine_id());
      h->set_to(participants_[0]);
      h->set_type(Header::ACK);
      Scalar s;
      s.ParseFromArray((*m)[2].data(), (*m)[2].size());
      h->set_ack_counter(FromScalar<uint64>(s));
      machine()->SendMessage(h, new MessageBuffer());
    } else {
      // Commit message.
      CHECK(!uncommitted.empty());
      delete m;
      m = uncommitted.front();
      uncommitted.pop();
      Scalar s;
      s.ParseFromArray((*m)[1].data(), (*m)[1].size());
      log_->Append(FromScalar<uint64>(s), (*m)[0]);
      delete m;
    }
  }
}

