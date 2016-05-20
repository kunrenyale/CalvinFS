// Author: Kun Ren <kun@cs.yale.edu>
//

#ifndef CALVIN_MACHINE_EXTERNAL_CONNECTION_H_
#define CALVIN_MACHINE_EXTERNAL_CONNECTION_H_

#include <map>
#include <utility>
#include <vector>

#include "common/utils.h"
#include "machine/cluster_config.h"
#include "machine/connection/connection.h"
#include "machine/connection/zmq_cpp.h"
#include "machine/message_buffer.h"
#include "common/atomic.h"
#include "common/types.h"
#include "machine/connection/connection_zmq.h"

using std::pair;

class ExternalConnection {
 public:
  ExternalConnection(int port, const ClusterConfig& config);
  ~ExternalConnection();
  void SendMessage(Header* header, MessageBuffer* message);

  // Blocking receive.
  void GetMessage(Header** h, MessageBuffer** m);

 private:
  void Init();

  int port_;
  ClusterConfig config_;
  zmq::socket_t* socket_in_;
  map<uint64, zmq::socket_t*> sockets_out_;
};

#endif  // CALVIN_MACHINE_EXTERNAL_CONNECTION_H_

