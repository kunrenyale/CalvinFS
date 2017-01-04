// Author: Kun Ren <kun@cs.yale.edu>
// Author: Alexander Thomson <thomson@cs.yale.edu>
//

#include <stdlib.h>
#include <map>
#include <utility>
#include <vector>

#include "machine/external_connection.h"
#include "common/utils.h"
#include "proto/header.pb.h"

using std::map;

zmq::context_t* external_context_ = NULL;
int external_connection_count_ = 0;

zmq::context_t* ExternalGetZMQContext() {
  if (external_context_ == NULL) {
    external_context_ = new zmq::context_t(1);
  }
  external_connection_count_++;
  return external_context_;
}

ExternalConnection::ExternalConnection(int port, const ClusterConfig& config) {
  port_ = port;
  config_ = config;
  Init();
}

ExternalConnection::~ExternalConnection() {
  delete socket_in_;
  for (map<uint64, zmq::socket_t*>::iterator it = sockets_out_.begin();
       it != sockets_out_.end(); ++it) {
    delete it->second;
  }
}

void ExternalConnection::Init() {
  char endpoint[256];
  snprintf(endpoint, sizeof(endpoint), "tcp://*:%d", port_);
  socket_in_ = new zmq::socket_t(*ExternalGetZMQContext(), ZMQ_PULL);
  socket_in_->bind(endpoint);


  // Wait a bit for other nodes to bind sockets before connecting to them.
  Spin(0.1);

  // Connect to remote outgoing sockets.
  for (map<uint64, MachineInfo>::const_iterator it =
       config_.machines().begin();
       it != config_.machines().end(); ++it) {
    snprintf(endpoint, sizeof(endpoint), "tcp://%s:%d",
             it->second.host().c_str(), it->second.port());
    sockets_out_[it->second.id()] =
        new zmq::socket_t(*ExternalGetZMQContext(), ZMQ_PUSH);
    sockets_out_[it->second.id()]->connect(endpoint);
  }
}

void ExternalDeleteMessagePart(void *data, void *hint) {
  delete reinterpret_cast<MessagePart*>(hint);
}

void ExternalConnection::SendMessage(Header* header, MessageBuffer* message) {
  uint64 recipient = header->to();
  message->Append(*header);
  delete header;
  for (uint32 i = 0; i < message->size(); i++) {
    // Create message.
    void* data = reinterpret_cast<void*>(const_cast<char*>(
                 (*message)[i].data()));
    int size = (*message)[i].size();
    MessagePart* part = message->StealPart(i);
    zmq::message_t msg(data, size,
                       ExternalDeleteMessagePart,
                       part);
    // Send message. All but the last are sent with ZMQ's SNDMORE flag.
    if (i == message->size() - 1) {
      sockets_out_[recipient]->send(msg);
    } else {
      sockets_out_[recipient]->send(msg, ZMQ_SNDMORE);
    }
  }
  delete message;
}

void ExternalConnection::GetMessage(Header** h, MessageBuffer** m) {
  zmq::message_t* msg_part = new zmq::message_t();
  *m = new MessageBuffer();
  while (true) {
    if (socket_in_->recv(msg_part, ZMQ_DONTWAIT)) {
      // See if that was the final message part for this message.
      int more;
      size_t moresize = sizeof(more);
      socket_in_->getsockopt(ZMQ_RCVMORE, &more, &moresize);
      if (!more) {
        // Final message part. Decode as header.
        *h = new Header();
        (*h)->ParseFromArray(msg_part->data(), msg_part->size());
        delete msg_part;

        // And we're done.
        break;

      } else {
        // More parts remain for this message. Add this one to the output
        // message and create a new one to receive the next part.
        (*m)->Append(msg_part);
        msg_part = new zmq::message_t();
      }
    }
  }
}

