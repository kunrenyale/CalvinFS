// Author: Alex Thomson
//
// A MessageHandler just specifies some behavior to invoke upon receiving a
// message (with header).
//

#ifndef CALVIN_MACHINE_MESSAGE_HANDLER_H_
#define CALVIN_MACHINE_MESSAGE_HANDLER_H_

class Header;
class MessageBuffer;

class MessageHandler {
 public:
  virtual ~MessageHandler() {}

  // Takes ownership of '*header' and '*message' and does something with them.
  virtual void HandleMessage(Header* header, MessageBuffer* message) = 0;
};

#endif  // CALVIN_MACHINE_MESSAGE_HANDLER_H_
