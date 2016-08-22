// Author: Alexander Thomson (thomson@cs.yale.edu)

#ifndef CALVIN_CHAMELEON_LAZY_LAZY_STORE_H_
#define CALVIN_CHAMELEON_LAZY_LAZY_STORE_H_

#include <glog/logging.h>

#include "machine/machine.h"
#include "components/log/local_mem_log.h"
#include "components/backend/mvstore_backend.h"
#include "components/scheduler/serial_scheduler.h"
#include "components/storage/mvstore.h"
#include "proto/action.pb.h"
#include "proto/mvstore.pb.h"
#include "proto/scheduler_options.pb.h"
#include "proto/start_app.pb.h"

using machine::Machine;
using machine::MessageBuffer;
using machine::ClusterConfig;

namespace components {

class LazyStore {
 public:
  explicit LazyStore(Machine* m) : m_(m) {
    StartAppProto sap;

    sap.add_participants(log);
    sap.set_app("LocalMemLog");
    sap.set_app_name("log");
    m_->AddApp(sap);

    sap.clear_participants();
    for (uint32 i = 0; i < participants_.size(); i++) {
      sap.add_participants(participants_[i]);
    }

    sap.set_app("MVStoreBackend");
    sap.set_app_name("backend");
    m_->AddApp(sap);

    sap.set_app("SerialScheduler");
    sap.set_app_name("scheduler");
    SchedulerOptions so;
    so.set_log_machine(log);
    so.set_log_app_name("log");
    so.set_backend_app_name("backend");
    so.SerializeToString(sap.mutable_app_args());
    m_->AddApp(sap);
  }

  ~LazyStore() {}

  inline void Put(const string& key, const string& value) {
    MVStorePutInput in;
    in.set_key(key);
    in.set_value(value);

    Action action;
    action.set_client("client");
    action.set_action_type("PUT");
    in.SerializeToString(action.mutable_input());
    MVStoreBackend::PutAction().ComputeRWSets(&action);

    Header* header = new Header();
    header->set_from(m_->machine_id());
    header->set_to(log_);
    header->set_type(Header::RPC);
    header->set_app("log");
    header->set_rpc("Append");
    atomic<int> ack(0);
    header->set_ack_counter(reinterpret_cast<uint64>(&ack));
    m_->SendMessage(header, new MessageBuffer(action));
  }

  inline bool Get(const string& key, uint64 version, string* value) {
    MVStoreGetInput in;
    in.set_key(key);
    in.set_version(version);

    Action action;
    action.set_client("client");
    action.set_action_type("GET");
    in.SerializeToString(action.mutable_input());
    MVStoreBackend::GetAction().ComputeRWSets(&action);

    MessageBuffer* response = NULL;
    Header* header = new Header();
    header->set_from(m_->machine_id());
    header->set_to(GetOwner(key));
    header->set_type(Header::RPC);
    header->set_app("backend");
    header->set_rpc("Get");
    header->set_data_ptr(reinterpret_cast<uint64>(&response));

    m_->SendMessage(header, new MessageBuffer(action));

    // Wait for response.
    SpinUntilNE<MessageBuffer*>(response, NULL);

    // Parse response.
    action.ParseFromArray((*response)[0].data(), (*response)[0].size());
    MVStoreGetOutput out;
    out.ParseFromString(action.output());
    if (out.exists()) {
      *value = out.value();
      return true;
    }
    return false;
  }

 private:
  // Returns the machine id of the participant at which the record with key
  // 'key' is stored.
  inline uint64 GetOwner(const string& key) {
    return participants_[FNVHash(key) % participants_.size()];
  }

  Machine* m_;
};

}  // namespace components

#endif  // CALVIN_CHAMELEON_LAZY_LAZY_STORE_H_

