// Author: Alex Thomson
//
// ZeroMQ-based Connection implementation.

#ifndef CALVIN_MACHINE_CONNECTION_CONNECTION_ZMQ_H_
#define CALVIN_MACHINE_CONNECTION_CONNECTION_ZMQ_H_

#include <map>

#include "machine/cluster_config.h"
#include "machine/connection/connection.h"
#include "machine/connection/zmq_cpp.h"
#include "machine/message_buffer.h"
#include "common/atomic.h"
#include "common/mutex.h"
#include "common/types.h"
#include "proto/header.pb.h"

class ConnectionZMQ : public Connection {
 public:
  ConnectionZMQ(
      uint64 id,
      const ClusterConfig& config,
      MessageHandler* handler);
  virtual ~ConnectionZMQ();
  virtual void SendMessage(uint64 recipient, MessageBuffer* message);
  virtual void SendMessageExternal(Header* header, MessageBuffer* message);

 private:
  // Socket initialization function called by constructor.
  void Init();

  // Main listener loop.
  static void* ListenerLoop(void* arg);

  // False until destructor is called. Signals ListenerLoop to stop and return.
  bool destructor_called_;

  // Thread in which to run the main loop.
  pthread_t thread_;

  // Socket listening for messages from other machines. Type = ZMQ_PULL.
  zmq::socket_t* socket_in_;

  // Sockets for outgoing traffic to other machines. Keyed by machine_id.
  // Type = ZMQ_PUSH.
  map<uint64, zmq::socket_t*> sockets_out_;

  // Mutexes guarding out-bound sockets.
  map<uint64, Mutex*> mutexes_;

  // DISALLOW_COPY_AND_ASSIGN
  ConnectionZMQ(const ConnectionZMQ&);
  ConnectionZMQ& operator=(const ConnectionZMQ&);
};

#endif  // CALVIN_MACHINE_CONNECTION_CONNECTION_ZMQ_H_

