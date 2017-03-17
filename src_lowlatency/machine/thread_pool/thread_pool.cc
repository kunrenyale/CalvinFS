// Author: Kun Ren <kun@cs.yale.edu>
// Author: Alexander Thomson <thomson@cs.yale.edu>
//

#include "machine/thread_pool/thread_pool.h"

#include <glog/logging.h>
#include <pthread.h>
#include <stdlib.h>
#include <atomic>
#include <queue>
#include <vector>
#include <map>

#include "machine/message_buffer.h"
#include "common/atomic.h"
#include "common/types.h"
#include "common/utils.h"
#include "proto/header.pb.h"

using std::make_pair;
using std::pair;
using std::map;
using std::atomic;

class SubPool : public MessageHandler {
 public:
  SubPool(MessageHandler* handler, int priority);
  virtual ~SubPool();
  virtual void HandleMessage(Header* header, MessageBuffer* message);

 private:
  friend class ThreadPool;
  // Initializes queues, creates worker threads. Called by constructor.
  void Start();

  // Function executed by each pthread.
  static void* RunThread(void* arg);

  // Total number of threads.
  int Thread_count();

  // Number of currently idle threads.
  int Idle_thread_count();

  // Changes the total number of threads to 'n'. May involve creating
  // new threads or killing existing threads.
  void Resize_thread_count(int n);

  // Handler used by worker threads.
  MessageHandler* handler_;

  int thread_count_;
  // Idle thread count of the subPool
  atomic<int> idle_thread_count_;
  map<int, pthread_t> threads_;
  map<int, bool> stopped_;

  // Min idle threads that allowed in the thread pool
  int min_idle_;
  // Max idle threads that allowed in the thread pool
  int max_idle_;
  // The priority of the subPool, 0: high, 1: low
  int priority_;

  bool stopped_all_;

  int assigned_thread_count_;

  // RPC/message queue.
  AtomicQueue<pair<Header*, MessageBuffer*> > queue_;

  AtomicQueue<int> deleted_threads_;

  // DISALLOW_DEFAULT_CONSTRUCTOR
  SubPool();

  // DISALLOW_COPY_AND_ASSIGN
  SubPool(const SubPool&);
  SubPool& operator=(const SubPool&);
};

/////////////////////ThreadPool implementation/////////////////////////////

ThreadPool::ThreadPool(MessageHandler* handler) {
  handler_ = handler;
  high_ = new SubPool(handler, 0);
  low_ = new SubPool(handler, 1);

  stopped_all_ = false;

  // Create monitor thread with high priority.
  cpu_set_t cpuset;
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
  CPU_ZERO(&cpuset);
  for (int i = 0; i < 8; i++) {
    CPU_SET(i, &cpuset);
  }
  pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
  pthread_create(&monitor_thread_,
                 &attr,
                 MonitorThread,
                 reinterpret_cast<void*>(this));
}

void ThreadPool::HandleMessage(Header* header, MessageBuffer* message) {
  switch (header->priority()) {
    case Header::HIGH:
      high_->HandleMessage(header, message);
      break;
    case Header::LOW:
      low_->HandleMessage(header, message);
      break;
    default:
      LOG(FATAL) << "Bad priority";
      break;
  }
}

ThreadPool::~ThreadPool() {
  // Stop Monitor thread
  stopped_all_ = true;
  pthread_join(monitor_thread_, NULL);
  // Delete subPools
  delete high_;
  delete low_;
  // Delete handler.
  delete handler_;
}

////////////////////////SubPool implementation/////////////////////////////

void SubPool::HandleMessage(Header* header, MessageBuffer* message) {
  CHECK(!stopped_all_) << "Stopped thread pool asked to handle message.";
  queue_.Push(make_pair(header, message));
}

SubPool::SubPool(MessageHandler* handler, int priority) {
  handler_ = handler;
  priority_ = priority;
  min_idle_ = 400;
  max_idle_ = 628;
  thread_count_ = min_idle_;
  idle_thread_count_ = 0;
  assigned_thread_count_ = thread_count_;
  stopped_all_ = false;
  for (int i = 0; i < thread_count_; i++) {
    stopped_[i] = false;
  }

  Start();
}

SubPool::~SubPool() {
  stopped_all_ = true;
  // Stop all works thread of the Subpool
  for (int i = 0; i < assigned_thread_count_; i++) {
    if (stopped_.count(i) > 0) {
      stopped_[i] = true;
      pthread_join(threads_[i], NULL);
    }
  }
}

void SubPool::Start() {
  cpu_set_t cpuset;
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
  switch (priority_) {
    case 0:
      // High priority threads can use all 8 cores
      CPU_ZERO(&cpuset);
      for (int i = 0; i < 8; i++) {
        CPU_SET(i, &cpuset);
      }
      break;
    case 1:
      // Low priority threads can use 6 cores
      CPU_ZERO(&cpuset);
      for (int i = 1; i < 4; i++) {
        CPU_SET(i, &cpuset);
      }
      for (int i = 5; i < 8; i++) {
        CPU_SET(i, &cpuset);
      }
      break;
    default:
      LOG(FATAL) << "Bad priority";
      break;
  }

  pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
  for (int i = 0; i < thread_count_; i++) {
    threads_[i] = (pthread_t)0;
    pthread_create(&threads_[i],
                   &attr,
                   RunThread,
                   new pair<int, SubPool*>(i, this));
  }
}

int SubPool::Thread_count() {
  return thread_count_;
}

int SubPool::Idle_thread_count() {
  return idle_thread_count_;
}

void SubPool::Resize_thread_count(int n) {
  thread_count_ = n;
}

// Work threads function
void* SubPool::RunThread(void* arg) {
  int thread = reinterpret_cast<pair<int, SubPool*>*>(arg)->first;
  SubPool* tp = reinterpret_cast<pair<int, SubPool*>*>(arg)->second;
  pair<Header*, MessageBuffer*> message;
  int sleep_duration = 1;  // in microseconds

  ++tp->idle_thread_count_;
  while (!tp->stopped_[thread]) {
    if (tp->queue_.Pop(&message)) {
      if (message.first == NULL) {
        // Tell the monitor thread that I am going to die and should be deleted.
        tp->deleted_threads_.Push(thread);
        // Die.
        --tp->idle_thread_count_;
        return NULL;
      }
      --tp->idle_thread_count_;
      tp->handler_->HandleMessage(message.first, message.second);
      // Reset backoff.
      sleep_duration = 1;
      ++tp->idle_thread_count_;
    } else {
      usleep(sleep_duration);
      // Back off exponentially.
      if (sleep_duration < 64)
        sleep_duration *= 2;
    }
  }

  // Go through ALL queues looking for remaining requests until there are none.
  if (tp->stopped_all_) {
    bool found_request = false;
    do {
      found_request = false;
      if (tp->queue_.Pop(&message)) {
        found_request = true;
        tp->handler_->HandleMessage(message.first, message.second);
      }
    } while (found_request);
  }
  --tp->idle_thread_count_;
  return NULL;
}

// Show the current SubPool status(total threads, idle threads count and
// queue size).
void ThreadPool::ShowStatus() {
  LOG(ERROR) << "Status H:   idle threads: " << high_->idle_thread_count_.load()
             << ", total threads: " << high_->thread_count_ << ", queue size: "
             << high_->queue_.Size();
  LOG(ERROR) << "Status L:   idle threads: " << low_->idle_thread_count_.load()
             << ", total threads: " << low_->thread_count_ << ", queue size: "
             << low_->queue_.Size();
  LOG(ERROR) << "";
}

void* ThreadPool::MonitorThread(void* arg) {
  ThreadPool* tp = reinterpret_cast<ThreadPool*>(arg);
  SubPool* high = tp->high_;
  SubPool* low = tp->low_;

  usleep(1000*400);

  while (!tp->stopped_all_) {
    // Show the SubPools status.
//  tp->ShowStatus();

    // Check high priority threads
    if (high->idle_thread_count_ < high->min_idle_) {
      // Need to create some threads
      int add_thread_count = 4;
      // Set affinity
      cpu_set_t cpuset;
      pthread_attr_t attr;
      pthread_attr_init(&attr);
      pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
      CPU_ZERO(&cpuset);
      for (int i = 0; i < 8; i++) {
        CPU_SET(i, &cpuset);
      }
      pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);

      for (int i = 0; i < add_thread_count; i++) {
        high->threads_[high->assigned_thread_count_ + i] = (pthread_t)0;
        high->stopped_[high->assigned_thread_count_ + i] = false;
        int error = pthread_create(&high->threads_[high->assigned_thread_count_ + i],
                       &attr,
                       SubPool::RunThread,
                       new pair<int, SubPool*>(high->assigned_thread_count_ + i,
                                               high));

        if (error != 0) {
          LOG(ERROR) << ":------------ Excess the max threads limit: " << high->assigned_thread_count_;
          CHECK(error == 0);      
        }
      }
      high->Resize_thread_count(high->Thread_count() + add_thread_count);
      high->assigned_thread_count_ += add_thread_count;
//LOG(ERROR) << ":------------ Need to create more threads, now is: " << high->thread_count_;
    } else if (high->idle_thread_count_ > high->max_idle_) {
      // Need to delete some threads
      /**int delete_thread_count = 4;
      for (int i = 0; i < delete_thread_count; i++) {
        high->queue_.Push(pair<Header*, MessageBuffer*>(NULL, NULL));
        int deleted_thread;
        while (!high->deleted_threads_.Pop(&deleted_thread)) {
          // wait for a thread to die
        }
        pthread_join(high->threads_[deleted_thread], NULL);
        high->threads_.erase(deleted_thread);
        high->stopped_.erase(deleted_thread);
      }
      high->Resize_thread_count(high->thread_count_ - delete_thread_count);**/
//LOG(ERROR) << ":------------ Need to delete some threads, now is: " << high->thread_count_;
    }

    // Check low priority threads
    if (low->idle_thread_count_ < low->min_idle_) {
      // Need to create some threads
      int add_thread_count = 4;

      cpu_set_t cpuset;
      pthread_attr_t attr;
      pthread_attr_init(&attr);
      pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
      CPU_ZERO(&cpuset);
      for (int i = 1; i < 4; i++) {
        CPU_SET(i, &cpuset);
      }
      for (int i = 5; i < 8; i++) {
        CPU_SET(i, &cpuset);
      }
      pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);

      for (int i = 0; i < add_thread_count; i++) {
        low->threads_[low->assigned_thread_count_ + i] = (pthread_t)0;
        low->stopped_[low->assigned_thread_count_ + i] = false;
        pthread_create(&low->threads_[low->assigned_thread_count_ + i],
                       &attr,
                       SubPool::RunThread,
                       new pair<int, SubPool*>(low->assigned_thread_count_ + i,
                                               low));
      }
      low->Resize_thread_count(low->Thread_count() + add_thread_count);
      low->assigned_thread_count_ += add_thread_count;
    } else if (low->idle_thread_count_ > low->max_idle_) {
      // Need to delete some threads
      int delete_thread_count = 4;
      for (int i = 0; i < delete_thread_count; i++) {
        low->queue_.Push(pair<Header*, MessageBuffer*>(NULL, NULL));
        int deleted_thread;
        while (!low->deleted_threads_.Pop(&deleted_thread)) {
          // wait for a thread to die
        }
        pthread_join(low->threads_[deleted_thread], NULL);
        low->threads_.erase(deleted_thread);
        low->stopped_.erase(deleted_thread);
      }
      low->Resize_thread_count(low->thread_count_ - delete_thread_count);
    }

    // sleep for 1/100th of a second
    usleep(10000);
  }
  return NULL;
}

