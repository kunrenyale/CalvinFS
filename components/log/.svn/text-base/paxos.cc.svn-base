// Author: Alex Thomson (thomson@cs.yale.edu)
//

#include "components/log/paxos.h"

#include <atomic>
#include <glog/logging.h>
#include <google/protobuf/message.h>
#include <queue>
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
using std::vector;

// Make FakePaxosApp startable.
REGISTER_APP(FakePaxosApp) {
  Scalar s;
  s.ParseFromString(ARG);
  return new FakePaxosApp(new LocalMemLog(), FromScalar<uint64>(s));
}

REGISTER_APP(FakePaxosApp2) {
  StartAppProto sap;
  sap.ParseFromString(ARG);
  vector<uint64> participants;
  for (int i = 0; i < sap.participants_size(); i++) {
    participants.push_back(sap.participants(i));
  }
  return new FakePaxosApp(new LocalMemLog(), participants);
}

FakePaxosApp::FakePaxosApp(Log* log, const vector<uint64>& participants)
    : participants_(participants), go_(true), going_(false) {
  log_ = log;
}

FakePaxosApp::FakePaxosApp(Log* log, uint64 count)
    : go_(true), going_(false) {
  log_ = log;
  for (uint64 i = 0; i < count; i++) {
    participants_.push_back(i);
  }
}

void FakePaxosApp::Append(const Slice& entry, uint64 count) {
  // Forward append request to leader.
  Header* header = new Header();
  header->set_from(machine()->machine_id());
  header->set_to(participants_[0]);
  header->set_type(Header::DATA);
  header->set_data_channel(name() + "_append");

  // Request ack.
  atomic<int> ack(0);
  header->set_ack_counter(reinterpret_cast<uint64>(&ack));

  // Send message.
  MessageBuffer* m = new MessageBuffer(new string(entry.data(), entry.size()));
  m->Append(new string(UInt64ToString(count)));
  machine()->SendMessage(header, m);

  while (ack.load() == 0) {
    // Wait for ack.
  }
}

void FakePaxosApp::Start() {
  going_ = true;
  if (machine()->machine_id() == participants_[0]) {
    RunLeader();
  } else {
    RunFollower();
  }
  going_ = false;
}

void FakePaxosApp::Stop() {
  go_ = false;
  while (going_.load()) {
    // Wait for main loop to stop.
    usleep(10);
  }
}

void FakePaxosApp::RunLeader() {
  auto append_channel = machine()->DataChannel(name() + "_append");
  auto ack_channel = machine()->DataChannel(name() + "_ack");
  string propose_channel_name = name() + "_propose";
  string commit_channel_name = name() + "_commit";

  int quorum = static_cast<int>(participants_.size()) / 2 + 1;

  // version -> (entry, ack count)
  map<uint64, pair<MessageBuffer*, int> > uncommitted;

  // (commit_time, entry)
  queue<pair<double, MessageBuffer*> > committed;

  uint64 next_version = 1;
  MessageBuffer* m = NULL;
  while (go_.load()) {
    // Handle a new append.
    if (append_channel->Pop(&m)) {
      uint64 count = StringToInt((*m)[1]);
      uint64 v = next_version;
      next_version += count;
      uncommitted[v] = make_pair(m, 1);

      // Forward message to followers.
      string version(UInt64ToString(v));
      Slice entry = (*m)[0];
      for (uint32 i = 1; i < participants_.size(); i++) {
        Header* h = new Header();
        h->set_from(machine()->machine_id());
        h->set_to(participants_[i]);
        h->set_type(Header::DATA);
        h->set_data_channel(propose_channel_name);
        m = new MessageBuffer();
        m->Append(new string(version));
        m->Append(entry);
        machine()->SendMessage(h, m);
      }
    }

    // Collect Acks.
    while (ack_channel->Pop(&m)) {
      uint64 v = StringToInt((*m)[0]);
      if (uncommitted.count(v)) {
        uncommitted[v].second++;
      }
    }

    // Commit as much as possible.
    while (!uncommitted.empty() &&
           uncommitted.begin()->second.second >= quorum) {
      // Send commit messages to followers.
      string version(UInt64ToString(uncommitted.begin()->first));
      for (uint32 i = 1; i < participants_.size(); i++) {
        Header* h = new Header();
        h->set_from(machine()->machine_id());
        h->set_to(participants_[i]);
        h->set_type(Header::DATA);
        h->set_data_channel(commit_channel_name);
        machine()->SendMessage(h, new MessageBuffer(new string(version)));
      }
      // Commit to log.
      log_->Append(
          uncommitted.begin()->first,
          (*uncommitted.begin()->second.first)[0]);
      // Move the entry to 'committed' so that it will eventually get
      // deallocated.
      committed.push(make_pair(GetTime(), uncommitted.begin()->second.first));
      uncommitted.erase(uncommitted.begin());
    }

    // Deallocate old messages.
    while (!committed.empty() && committed.front().first < GetTime() - 1) {
      delete committed.front().second;
      committed.pop();
    }
  }
}

void FakePaxosApp::RunFollower() {
  auto propose_channel = machine()->DataChannel(name() + "_propose");
  auto commit_channel = machine()->DataChannel(name() + "_commit");
  string ack_channel_name = name() + "_ack";

  // TODO(agt): Technically uncommitted should be durable.
  //
  // version -> entry
  map<uint64, MessagePart*> uncommitted;

  MessageBuffer* m = NULL;
  while (go_.load()) {
    // Handle all new proposals.
    while (propose_channel->Pop(&m)) {
      // Record entry as uncommitted.
      uint64 v = StringToInt((*m)[0]);
      uncommitted[v] = m->PopBack();

      // Send ack.
      Header* h = new Header();
      h->set_from(machine()->machine_id());
      h->set_to(participants_[0]);
      h->set_type(Header::DATA);
      h->set_data_channel(ack_channel_name);
      machine()->SendMessage(h, m);

      // SPECULATION:
      CHECK(v == uncommitted.rbegin()->first);
    }

    // Process one commit.
    if (commit_channel->Front(&m)) {
      uint64 v = StringToInt((*m)[0]);

      // Don't pop/commit if its not even in uncommitted yet.
      if (uncommitted.count(v) > 0) {
        commit_channel->Pop(&m);

        // SPECULATION:
        CHECK(v == uncommitted.begin()->first);

        // Write entry to log.
        log_->Append(v, uncommitted[v]->buffer());
        delete m;
        delete uncommitted[v];
        uncommitted.erase(v);
      }
    }
  }
}

