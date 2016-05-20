// Author: Kun Ren <kun@cs.yale.edu>
// Author: Alexander Thomson <thomson@cs.yale.edu>
//

#include "common/atomic.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <atomic>
#include <pthread.h>

#include "common/mutex.h"
#include "common/utils.h"
#include "machine/message_buffer.h"

using std::atomic;

DEFINE_bool(benchmark, false, "Run benchmarks instead of unit tests.");

TEST(AtomicQueueTest, AtomicQueueCorrectnessTest) {
  AtomicQueue<int>* queue = new AtomicQueue<int>();
  for (int i = 0; i < 1000000; i++) {
    queue->Push(i);
  }
  int test;
  int i = 0;
  // Check the stuff that we pop the same as what we pushed
  while (!queue->Empty()) {
    queue->Pop(&test);
    EXPECT_EQ(test, i++);
  }
}

///////////////////////   atomic primitive benchmarks   ////////////////////////

void BenchmarkAtomicQueue() {
  AtomicQueue<MessageBuffer*>* new_atomic_queue =
    new AtomicQueue<MessageBuffer*>();
  double start = GetTime();
  for (int i = 0; i < 1000000; i++) {
    new_atomic_queue->Push(new MessageBuffer());
  }
  MessageBuffer* test = NULL;
  for (int i = 0; i < 1000000; i++) {
    new_atomic_queue->Pop(&test);
    delete test;
  }
  delete new_atomic_queue;
  LOG(ERROR) << "AtomicQueue push/pop:     "
             << (GetTime() - start) * 1000 << " ns";
}

atomic<uint32> done_;
bool start_;

// Pop thread: pop elements from the queue and do something
void* AtomicQueuePopThread(void* arg) {
  while (!start_) {
    Noop<bool>(start_);
  }
  AtomicQueue<int>* new_atomic_queue = reinterpret_cast
                                       <AtomicQueue<int>* >(arg);
  int test = 1;
  int j = 0;
  while (true) {
    // Pop an element and do something
    if (new_atomic_queue->Pop(&test)) {
      for (int i = 0; i < 10; i++) {
        test = test * 3;
        test = test % 10;
        test = (test - 1) / 2;
      }
      j++;
    } else {
      if (done_ == 0) {
        break;
      }
    }
  }
  return NULL;
}

// Push thread: push elements to the queue
void* AtomicQueuePushThread(void* arg) {
  while (!start_) {
    Noop<bool>(start_);
  }
  AtomicQueue<int>* atomic_queue = reinterpret_cast
                                          <AtomicQueue<int>* >(arg);
  for (int i = 0; i < 1000000; i++) {
    atomic_queue->Push(i);
  }
  --done_;
  return NULL;
}

void BenchmarkAtomicQueueContention(int producers, int consumers) {
  start_ = false;
  done_ = producers;

  AtomicQueue<int>* atomic_queue =
    new AtomicQueue<int>();

  // Create Pop threads and Push threads
  vector<pthread_t> producer_threads(producers);
  vector<pthread_t> consumer_threads(consumers);
  for (int i = 0; i < consumers; i++) {
    pthread_create(&consumer_threads[i],
                   NULL,
                   AtomicQueuePopThread,
                   reinterpret_cast<void*>(atomic_queue));
  }
  for (int i = 0; i < producers; i++) {
    pthread_create(&producer_threads[i],
                   NULL,
                   AtomicQueuePushThread,
                   reinterpret_cast<void*>(atomic_queue));
  }
  double start = GetTime();
  start_ = true;
  for (int i = 0; !atomic_queue->Empty(); i++) {
    // Wait for queue to be empty.
    Noop<AtomicQueue<int>*>(atomic_queue);
  }

  for (int i = 0; i < producers; i++) {
    pthread_join(producer_threads[i], NULL);
  }
  for (int i = 0; i < consumers; i++) {
    pthread_join(consumer_threads[i], NULL);
  }
  delete atomic_queue;
  LOG(ERROR) << "AtomicQueue: Concurrently push and pop " << producers <<
                " million ints (" << producers << " producer threads, " <<
                consumers << " consumer threads). Total time per push/pop: "<<
                (GetTime() - start) / (double)producers * 1000 << " ns";
}

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  if (FLAGS_benchmark) {
    BenchmarkAtomicQueue();
    BenchmarkAtomicQueueContention(1, 1);
    BenchmarkAtomicQueueContention(1, 6);
    BenchmarkAtomicQueueContention(6, 1);
    BenchmarkAtomicQueueContention(4, 4);
    BenchmarkAtomicQueueContention(10, 10);
    return 0;
  } else {
    return RUN_ALL_TESTS();
  }
}

