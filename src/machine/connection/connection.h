// Author: Alex Thomson
//
// Interface for machine connection implementations.

#ifndef CALVIN_MACHINE_CONNECTION_CONNECTION_H_
#define CALVIN_MACHINE_CONNECTION_CONNECTION_H_

#include <string>
#include "machine/cluster_config.h"
#include "proto/header.pb.h"

using std::string;

class MessageHandler;
class MessageBuffer;

static const uint64 kExternalMachineID = 0xFFFFFFFFFFFFFFFF;

class Connection {
 public:
  // All subclasses of Connection are required to have a constructor of the
  // exact format:
  //
  //    ConnectionSubclass(
  //        uint64 id,
  //        const ClusterConfig& config,
  //        Connection::MessageHandler* handler);
  //
  // which sets up connections with all other Machines specified in config and
  // starts a main listener loop running in the background that calls
  // 'handler->Handle()' upon receiving a message.

  // Destructor should kill/join any background threads, etc., and also delete
  // 'handler_'.
  virtual ~Connection() {}

  // Sends '*message' to the machine with unique id 'recipient'. Takes
  // ownership of '*message' and deletes it when sending is complete.
  virtual void SendMessage(uint64 recipient, MessageBuffer* message) = 0;
  virtual void SendMessageExternal(Header* header, MessageBuffer* message) = 0;

 protected:
  // Unique machine id for this connection instance.
  uint64 id_;

  // Configuration information for this cluster.
  ClusterConfig config_;

  // Message handler specifying action taken upon receiving a message.
  MessageHandler* handler_;

  // Hostname/IP of this machine.
  string hostname_;

  // Port on which to listen for incoming messages from other machines.
  int port_;
};

#endif  // CALVIN_MACHINE_CONNECTION_CONNECTION_H_

