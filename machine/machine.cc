// Author: Alex Thomson

#include "machine/machine.h"

#include <glog/logging.h>
#include <atomic>

#include "machine/cluster_config.h"
#include "machine/connection/connection.h"
#include "machine/connection/connection_zmq.h"
#include "machine/app/app.h"
#include "machine/thread_pool/thread_pool.h"
#include "common/atomic.h"
#include "common/types.h"
#include "common/utils.h"
#include "proto/header.pb.h"

using std::atomic;

class ConnectionLoopMessageHandler : public MessageHandler {
 public:
  explicit ConnectionLoopMessageHandler(Machine* machine, ThreadPool* tp)
    : machine_(machine), tp_(tp) {
  }
  virtual ~ConnectionLoopMessageHandler() {}

  virtual void HandleMessage(Header* header, MessageBuffer* message) {
    CHECK(header->has_type());
    // TODO(agt): Better header validity checking.

    switch (header->type()) {
      // Hand RPC requests and CALLBACK invocations off to ThreadPool.
      //
      // TODO(agt): Support inline RPCs?
      case Header::RPC:
      case Header::CALLBACK:
        tp_->HandleMessage(header, message);
        break;

      // Ack: increment ack counter
      case Header::ACK:
        ++(*reinterpret_cast<atomic<int>*>(header->ack_counter()));
        delete header;
        delete message;
        break;

      // Data packets can be delivered directly.
      case Header::DATA:
        if (header->has_data_ptr()) {
          *reinterpret_cast<MessageBuffer**>(header->data_ptr()) = message;
        } else if (header->has_data_channel()) {
          machine_->DataChannel(header->data_channel())->Push(message);
        } else {
          LOG(FATAL) << "DATA message header lacks data: "
                     << header->DebugString();
        }

        // Send (non-data) response if requested (either ack or callback).
        header->clear_data_ptr();
        header->clear_data_channel();
        machine_->SendReplyMessage(header, new MessageBuffer());
        break;

      case Header::SYSTEM:
        // LOCAL AddApp() calls.
        if (header->rpc() == "addapp") {
          StartAppProto sap;
          sap.ParseFromArray((*message)[0].data(), (*message)[0].size());
          machine_->AddAppInternal(sap);
          tp_->HandleMessage(header, message);
        }
        break;

      default:
        LOG(FATAL) << "unknown Header::type(): " << header->type();
    }
  }

 private:
  Machine* machine_;
  ThreadPool* tp_;
};

class WorkerThreadMessageHandler : public MessageHandler {
 public:
  explicit WorkerThreadMessageHandler(Machine* machine) : machine_(machine) {}
  virtual ~WorkerThreadMessageHandler() {}
  virtual void HandleMessage(Header* header, MessageBuffer* message) {
    // Handle system messages specially.
    if (header->type() == Header::SYSTEM) {
      if (header->rpc() == "addapp") {
        StartAppProto sap;
        sap.ParseFromArray((*message)[0].data(), (*message)[0].size());

        // Send ack.
        machine_->SendReplyMessage(header, message);

        // Start app.
        machine_->StartAppInternal(sap);
        return;

      } else {
        LOG(FATAL) << "unrecognized SYSTEM message";
      }
    }

    // Handle RPC request
    CHECK(header->has_app()) << header->DebugString();
    machine_->GetApp(header->app())->HandleMessage(header, message);
  }

 private:
  Machine* machine_;
};

Machine::Machine(uint64 machine_id, const ClusterConfig& config)
    : machine_id_(machine_id), config_(config),
      next_guid_(1000), stop_(false),
      next_barrier_(0) {
  InitializeComponents();
  Spin(3);
}

Machine::Machine(
    uint64 machine_id,
    const ClusterConfig& config,
    ThreadPool* tp,
    Connection* connection)
      : machine_id_(machine_id), config_(config), thread_pool_(tp),
        connection_(connection), next_guid_(1000),
        stop_(false), next_barrier_(0) {
  Spin(3);
}

Machine::Machine()
    : machine_id_(0), config_(ClusterConfig::LocalCluster(1)),
      next_guid_(1000), stop_(false), next_barrier_(0) {
  InitializeComponents();
  Spin(3);
}

Machine::~Machine() {
  for (map<string, App*>::iterator it = apps_.begin(); it != apps_.end();
       ++it) {
    it->second->Stop();
  }
  delete connection_;
  delete thread_pool_;
  for (map<string, App*>::iterator it = apps_.begin(); it != apps_.end();
       ++it) {
    delete it->second;
  }
}

void Machine::SendMessage(Header* header, MessageBuffer* message) {
  // TODO(agt): Check header validity.
  message->Append(*header);
  connection_->SendMessage(header->to(), message);
  delete header;
}

void Machine::SendReplyMessage(Header* header, MessageBuffer* message) {
  header->set_to(header->from());
  header->set_from(machine_id());
  if (header->to() == kExternalMachineID) {
    connection_->SendMessageExternal(header, message);
    return;
  }
  if (header->has_data_ptr() || header->has_data_channel()) {
    header->set_type(Header::DATA);
    SendMessage(header, message);
  } else if (header->has_callback_app() && header->has_callback_rpc()) {
    header->set_type(Header::CALLBACK);
    header->set_app(header->callback_app());
    header->set_rpc(header->callback_rpc());
    header->clear_callback_app();
    header->clear_callback_rpc();
    SendMessage(header, message);
  } else if (header->has_ack_counter()) {
    // Send an empty message instead of the provided message.
    header->set_type(Header::ACK);
    SendMessage(header, new MessageBuffer());
    delete message;
  } else {
    // No reply requested.
    delete header;
    delete message;
  }
}

AtomicQueue<MessageBuffer*>* Machine::DataChannel(const string& channel) {
  AtomicQueue<MessageBuffer*>* inbox = NULL;
  if (!inboxes_.Lookup(channel, &inbox)) {
    AtomicQueue<MessageBuffer*>* newinbox = new AtomicQueue<MessageBuffer*>();
    inbox = inboxes_.PutNoClobber(channel, newinbox);
    if (inbox != newinbox) {
      // Oops it already got inserted.
      delete newinbox;
    }
  }
  return inbox;
}

void Machine::CloseDataChannel(const string& channel) {
  AtomicQueue<MessageBuffer*>* inbox = NULL;
  if (inboxes_.Lookup(channel, &inbox)) {
    delete inbox;
  }
  inboxes_.Erase(channel);
}

void Machine::AddApp(const StartAppProto& sap) {
  Header* header = new Header();
  header->set_to(machine_id());
  header->set_from(machine_id());
  header->set_type(Header::SYSTEM);
  header->set_rpc("addapp");
  // App's Start() method should run with high priority.
  header->set_priority(Header::HIGH);
  // Request ack.
  atomic<int> ack(0);
  header->set_ack_counter(reinterpret_cast<uint64>(&ack));

  // Send message.
  SendMessage(header, new MessageBuffer(sap));

  // Wait for ack.
  usleep(100);
  while (ack.load() == 0) {
    usleep(10);
  }
}

void Machine::InitializeComponents() {
  thread_pool_ = new ThreadPool(new WorkerThreadMessageHandler(this));
  connection_ = new ConnectionZMQ(
      machine_id_,
      config_,
      new ConnectionLoopMessageHandler(this, thread_pool_));
  Spin(0.1);
}

void Machine::AddAppInternal(const StartAppProto& sap) {
  CHECK(sap.has_app());
  CHECK(sap.has_app_name());
  CHECK(GetState()->startable_apps_.count(sap.app()) != 0)
        << "AppStarter::HandleMessage(): unknown app: " << sap.app();
  apps_[sap.app_name()] = GetState()->startable_apps_[sap.app()]
                          ->Go(sap.has_app_args() ? sap.app_args() : "");
  apps_[sap.app_name()]->name_ = sap.app_name();
  apps_[sap.app_name()]->machine_ = this;
}

void Machine::StartAppInternal(const StartAppProto& sap) {
  // Run App Start() method (may not terminate).
  apps_[sap.app_name()]->Start();
}

App* Machine::GetApp(const string& name) {
  if (apps_.count(name) == 0) {
    return NULL;
  }
  return apps_[name];
}

map<string, App*> Machine::GetApps() {
  return apps_;
}

void Machine::GlobalBarrier() {
  string channel_name("global-barrier-" + UInt64ToString(next_barrier_++));
  AtomicQueue<MessageBuffer*>* channel = DataChannel(channel_name);
  for (int i = 0; i < config().size(); i++) {
    Header* header = new Header();
    header->set_from(machine_id());
    header->set_to(i);
    header->set_type(Header::DATA);
    header->set_data_channel(channel_name);
    SendMessage(header, new MessageBuffer());
  }
  for (int i = 0; i < config().size(); i++) {
    MessageBuffer* m = NULL;
    while (!channel->Pop(&m)) {
      usleep(100);
    }
    delete m;
  }
  CloseDataChannel(channel_name);
  Spin(0.1);
}

