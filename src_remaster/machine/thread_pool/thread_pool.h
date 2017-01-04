// Author: Kun Ren <kun@cs.yale.edu>
// Author: Alexander Thomson <thomson@cs.yale.edu>
//
// Thread pool implementation in which there are a dynamical set of threads
// executing RPCs. Since the number of threads can grow dynamically, there is
// no risk ofdeadlock.
//

#ifndef CALVIN_MACHINE_THREAD_POOL_THREAD_POOL_H_
#define CALVIN_MACHINE_THREAD_POOL_THREAD_POOL_H_

#include <pthread.h>
#include <vector>
#include <utility>

#include "machine/message_handler.h"
#include "common/atomic.h"
#include "common/types.h"

using std::pair;
using std::vector;

class Header;
class MessageBuffer;
class SubPool;

class ThreadPool : public MessageHandler {
 public:
  explicit ThreadPool(MessageHandler* handler);
  virtual ~ThreadPool();
  virtual void HandleMessage(Header* header, MessageBuffer* message);

 private:
  static void* MonitorThread(void* arg);
  void ShowStatus();
  SubPool* high_;
  SubPool* low_;

  MessageHandler* handler_;
  bool stopped_all_;

  pthread_t monitor_thread_;

  // DISALLOW_DEFAULT_CONSTRUCTOR
  ThreadPool();

  // DISALLOW_COPY_AND_ASSIGN
  ThreadPool(const ThreadPool&);
  ThreadPool& operator=(const ThreadPool&);
};

#endif  // CALVIN_MACHINE_THREAD_POOL_THREAD_POOL_H_

