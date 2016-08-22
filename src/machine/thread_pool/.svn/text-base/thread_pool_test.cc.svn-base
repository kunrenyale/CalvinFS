// Author: Kun Ren <kun@cs.yale.edu>
// Author: Alexander Thomson <thomson@cs.yale.edu>
//

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <atomic>
#include <set>
#include <string>
#include <algorithm>

#include "machine/thread_pool/thread_pool.h"
#include "common/utils.h"
#include "proto/header.pb.h"
#include "machine/message_buffer.h"
#include "proto/scalar.pb.h"

using std::atomic;
using std::set;

DEFINE_bool(benchmark, false, "Run benchmarks instead of unit tests.");
DEFINE_int32(high, 10, "Number of high priority tasks.");
DEFINE_int32(low, 10, "Number of low priority tasks.");

/////////////////// MessageHandler that used by tests///////////////////////

// This MessageHandler is used by Correctness test, Delete, Create test and
// Throughput test.
class TestMessageHandler : public MessageHandler {
 public:
  TestMessageHandler() : counter_(0) {}
  virtual ~TestMessageHandler() {}
  virtual void HandleMessage(Header* header, MessageBuffer* message) {
    // Do something for about 1 microsec
    atomic<int> my_counter(0);
    for (int i = 0; i < 120; i++) {
      ++my_counter;
    }

    ++counter_;
    if (!message->empty()) {
      Lock l(&mutex_);
      completed_.insert((*message)[0].ToString());
    }
    delete header;
    delete message;
  }
  int counter() const {
    return counter_;
  }
  const set<string>& completed() { return completed_; }

 private:
  atomic<int> counter_;
  set<string> completed_;
  Mutex mutex_;
};

// This MessageHandler is used by Latency distribution test
class TestLatencyHandler : public MessageHandler {
 public:
  explicit TestLatencyHandler(int size) : counter_(0) {finsh_.resize(size);}
  virtual ~TestLatencyHandler() {}
  virtual void HandleMessage(Header* header, MessageBuffer* message) {
    // Do something for about 1 microsec
    atomic<int> my_counter(0);
    for (int i = 0; i < 120; i++) {
      ++my_counter;
    }

    ++counter_;
    // Record the finsh time of the tast.
    finsh_[header->from()] = GetTime();
    delete header;
    delete message;
  }
  int counter() const {
    return counter_;
  }

  double GetFinshTime(int index) {
    return finsh_[index];
  }

 private:
  atomic<int> counter_;
  vector<double> finsh_;
};

// This MessageHandler is used by Deadlock freedom test
class DeadlockFreedomMessageHandler : public MessageHandler {
 public:
  DeadlockFreedomMessageHandler() : counter_(0) {
    pthread_mutex_init(&mutex_, NULL);
  }
  virtual ~DeadlockFreedomMessageHandler() {}

  virtual void HandleMessage(Header* header, MessageBuffer* message) {
    if (header->rpc() == "lock") {
      AcquireMutex();
      ++counter_;
      delete message;
      delete header;
    } else if (header->rpc() == "unlock") {
      ReleaseMutex();
      ++counter_;
      delete message;
      delete header;
    } else {
      LOG(ERROR) << "Error";
    }
  }

  int counter() const {
    return counter_;
  }

  void Lock() {
    pthread_mutex_lock(&mutex_);
  }

  void Release() {
    pthread_mutex_unlock(&mutex_);
  }

  void AcquireMutex() {
    // Get lock, do something, release lock and leave
    Lock();
    atomic<int> my_counter(0);
    for (int i = 0; i < 120; i++) {
      ++my_counter;
    }
    Release();
  }

  void ReleaseMutex() {
    // Release lock, do something and leave
    Release();
    atomic<int> my_counter(0);
    for (int i = 0; i < 120; i++) {
      ++my_counter;
    }
  }

 private:
  pthread_mutex_t mutex_;
  atomic<int> counter_;
};

// This MessageHandler is used while testing different workloads
class BenchmarkMessageHandler : public MessageHandler {
 public:
  BenchmarkMessageHandler() : go_(false), started_(0), high_(0), low_(0) {}
  virtual ~BenchmarkMessageHandler() {}

  virtual void HandleMessage(Header* header, MessageBuffer* message) {
    if (header->rpc() == "loopy") {
      LoopyWorkload(header);
      delete message;
      delete header;
    } else if (header->rpc() == "short") {
      ShortWorkload(header);
      delete message;
      delete header;
    } else if (header->rpc() == "sleepy") {
      // TODO(kunren): Test sleep mostly workload
    }
  }

  int64 started() {
    return started_.load();
  }

  int64 high() {
    return high_;
  }

  int64 low() {
    return low_;
  }

  double high_duration() {
    return high_duration_;
  }

  double low_duration() {
    return low_duration_;
  }

  void set_go(bool val) {
    go_ = val;
  }

  void LoopyWorkload(Header* header) {
    ++started_;
    atomic<int64> my_counter(0);
    while (!go_.load()) {
      // wait for go signal
    }
    // increment until go signal is false
    while (go_.load()) {
      ++my_counter;
    }

    Lock l(&mutex_);
    if (header->priority() == Header::HIGH) {
      high_ += my_counter.load();
    } else {
      low_ += my_counter.load();
    }

    --started_;
  }

  void ShortWorkload(Header* header) {
    ++started_;
    while (!go_.load()) {
      // wait for go signal
    }
    double start = GetTime();
    // Do something for about 15 millisec
    atomic<int> my_counter(0);
    for (int i = 0; i < 1200000; i++) {
      ++my_counter;
    }

    Lock l(&mutex_);
    if (header->priority() == Header::HIGH) {
      high_duration_ += (GetTime() - start);
    } else {
      low_duration_ += (GetTime() - start);
    }

    --started_;
  }

 private:
  atomic<bool> go_;
  atomic<int> started_;
  Mutex mutex_;
  int64 high_;
  int64 low_;
  double high_duration_;
  double low_duration_;
};


///////////////////////////Correctness test///////////////////////////////////

// Returns a random string that does not appear in 'used'.
string UniqueRandomString(const set<string>& used, int length) {
  string s;
  do {
    s = RandomString(length);
  } while (used.count(s) != 0);
  return s;
}

TEST(ThreadPoolTest, Correctness) {
  TestMessageHandler* test_handler = new TestMessageHandler();
  ThreadPool* tp = new ThreadPool(test_handler);
  set<string> requests;
  int test_task_count = 2*10000;

  // Submit some tasks to the thread pool
  for (int j = 0; j < 100; j++) {
    for (int i = 0; i < 100; i++) {
      // high priority
      Header* header = new Header();
      header->set_priority(Header::HIGH);
      string* s = new string(UniqueRandomString(requests, 6));
      requests.insert(*s);
      tp->HandleMessage(header, new MessageBuffer(s));

      // low priority
      header = new Header();
      header->set_priority(Header::LOW);
      s = new string(UniqueRandomString(requests, 6));
      requests.insert(*s);
      tp->HandleMessage(header, new MessageBuffer(s));
    }
    usleep(10000);
  }

  // Wait for response.
  while ((int)test_handler->counter() < test_task_count) {
    usleep(10);
  }
  usleep(1000000);

  EXPECT_EQ((int)requests.size(), (int)test_handler->completed().size());

  // Check if the message handler actually got the correct message
  set<string>::const_iterator a = requests.begin();
  for (int i = 0; i < test_task_count; i++) {
    EXPECT_NE((int)test_handler->completed().count(*a), 0);
    a++;
  }
  EXPECT_EQ(a, requests.end());
  delete tp;
}

//////////////////Delete, Create test///////////////////////////////////

TEST(ThreadPoolTest, ThreadPoolDeleteCreateTest) {
  // Create two thread pool
  TestMessageHandler* test_handler_1 = new TestMessageHandler();
  ThreadPool* tp_1 = new ThreadPool(test_handler_1);
  TestMessageHandler* test_handler_2 = new TestMessageHandler();
  ThreadPool* tp_2 = new ThreadPool(test_handler_2);

  int test_task_count = 100000;

  // Delete a thread pool while it is still working
  for (int i = 0; i < test_task_count; i++) {
    Header* header = new Header();
    if (rand() % 2 == 0) {
      header->set_priority(Header::HIGH);
    } else {
      header->set_priority(Header::LOW);
    }
    tp_1->HandleMessage(header, new MessageBuffer());
  }
  for (int i = 0; i < test_task_count; i++) {
    Header* header = new Header();
    if (rand() % 2 == 0) {
      header->set_priority(Header::HIGH);
    } else {
      header->set_priority(Header::LOW);
    }
    tp_2->HandleMessage(header, new MessageBuffer());
  }
  delete tp_2;

  // Create another thread pool and submit some tasks
  TestMessageHandler* test_handler_3 = new TestMessageHandler();
  ThreadPool* tp_3 = new ThreadPool(test_handler_3);
  for (int i = 0; i < test_task_count; i++) {
    Header* header = new Header();
    if (rand() % 2 == 0) {
      header->set_priority(Header::HIGH);
    } else {
      header->set_priority(Header::LOW);
    }
    tp_3->HandleMessage(header, new MessageBuffer());
  }

  // When finish all tasks, delete all thread pools
  while (test_handler_1->counter() != test_task_count ||
         test_handler_3->counter() != test_task_count) {
    usleep(10);
  }
  delete tp_1;
  delete tp_3;
}

///////////////////Deadlock freedom test/////////////////////////////////////

TEST(ThreadPoolTest, DeadlockFreedom) {
  DeadlockFreedomMessageHandler* bmh = new DeadlockFreedomMessageHandler();
  ThreadPool* tp = new ThreadPool(bmh);

  // First Lock the mutex
  bmh->Lock();

  int lock_actions = 200;

  // Enqueue a lot of actions that try to acquire that mutex(initially, fail).
  for (int i = 0; i < lock_actions; i++) {
    Header* header = new Header();
    if (rand() % 2 == 0) {
      header->set_priority(Header::HIGH);
    } else {
      header->set_priority(Header::LOW);
    }
    header->set_rpc("lock");
    tp->HandleMessage(header, new MessageBuffer());
  }

  usleep(1000000);

  // Enqueue a new action that releases the mutex.
  Header* header = new Header();
  header->set_priority(Header::HIGH);
  header->set_rpc("unlock");
  tp->HandleMessage(header, new MessageBuffer());

  // Wait for response.
  while ((int)bmh->counter() < lock_actions + 1) {
    usleep(10);
  }
  usleep(1000000);
  delete tp;
}

////////////////////////Throughput test/////////////////////////////////////

void ThreadPoolBenchmarkThroughput() {
  TestMessageHandler* test_handler = new TestMessageHandler();
  ThreadPool* tp = new ThreadPool(test_handler);
  int test_task_count = 2000000;
  double start = GetTime();

  // Submit a lot of tasks to the thread pool
  for (int i = 0; i < test_task_count; i++) {
    Header* header = new Header();
    header->set_priority(Header::HIGH);
    tp->HandleMessage(header, new MessageBuffer());
  }

  // Wait for response.
  while (test_handler->counter() != test_task_count) {
    usleep(10);
  }
  delete tp;
  LOG(ERROR) << "------------Total throughput test------------------------";
  LOG(ERROR) << "ThreadPoolBenchmark " << " :   " << ((double)test_task_count) /
                (GetTime() - start) << " tasks / second";
}

/////////////////////////Latency distribution test/////////////////////////

void ThreadPoolLatencyDistributionTest() {
  int test_task_count = 1000000;
  TestLatencyHandler* test_handler = new TestLatencyHandler(test_task_count);
  ThreadPool* tp = new ThreadPool(test_handler);
  vector<double> begin(test_task_count);
  vector<double> latency(test_task_count);
  double start = GetTime();

  // Submit a lot of tasks to the thread pool
  for (int i = 0; i < test_task_count; i++) {
    Header* header = new Header();
    header->set_priority(Header::HIGH);
    header->set_from(i);
    begin[i] = GetTime();
    tp->HandleMessage(header, new MessageBuffer());
  }

  // Wait for response.
  while (test_handler->counter() != test_task_count) {
    usleep(10);
  }

  for (int i = 0; i < test_task_count; i++) {
    latency[i] = test_handler->GetFinshTime(i) - begin[i];
  }

  // Sort the latency vector
  sort(latency.begin(), latency.end());

  double latency_50 = latency[test_task_count / 2 + 1];
  double latency_90 = latency[(test_task_count * 9) / 10 + 1];
  double latency_99 = latency[(test_task_count * 99) / 100 + 1];
  double latency_999 = latency[(test_task_count * 99.9) / 100 + 1];

  delete tp;
  LOG(ERROR) << "------------Latency distribution test------------------------";
  LOG(ERROR) << "Throughput: " << ((double)test_task_count)
                   / (GetTime() - start) << " tasks / second. ";
  LOG(ERROR) << "50th percentile (median) latency:             "
             << latency_50 * 1000000 << "  microsecond";
  LOG(ERROR) << "90th percentile latency:                      "
             << latency_90 * 1000000 << "  microsecond";
  LOG(ERROR) << "99th percentile latency:                      "
             << latency_99 * 1000000 << "  microsecond";
  LOG(ERROR) << "99.9th percentile latency:                    "
             << latency_999 * 1000000 << "  microsecond";
}

///////////////////Benchmark different workload////////////////////////////////

void BenchmarkLoopyWorkload(int high_tasks, int low_tasks) {
  BenchmarkMessageHandler* bmh = new BenchmarkMessageHandler();
  ThreadPool* tp = new ThreadPool(bmh);
  for (int i = 0; i < high_tasks; i++) {
    Header* header = new Header();
    header->set_priority(Header::HIGH);
    header->set_rpc("loopy");
    tp->HandleMessage(header, new MessageBuffer());
  }
  for (int i = 0; i < low_tasks; i++) {
    Header* header = new Header();
    header->set_priority(Header::LOW);
    header->set_rpc("loopy");
    tp->HandleMessage(
        header,
        new MessageBuffer());
  }

  while (bmh->started() != high_tasks + low_tasks) {
    // Wait for all tasks to be running in thread pool
  }
  bmh->set_go(true);
  usleep(4000000);
  bmh->set_go(false);
  while (bmh->started() != 0) {
    // Wait for all tasks to finish
  }

  LOG(ERROR) << "---------------Benchmark Loopy Workload------------------";
  if (high_tasks != 0) {
    LOG(ERROR) << "High: "
               << ((double)bmh->high()) / ((double)high_tasks)
                      / ((double)145000000);
  }
  if (low_tasks != 0) {
    LOG(ERROR) << "Low:  "
               << ((double)bmh->low()) / ((double)low_tasks)
                      / ((double)145000000);
  }
}

void BenchmarkShortWorkload(int high_tasks, int low_tasks) {
  BenchmarkMessageHandler* bmh = new BenchmarkMessageHandler();
  ThreadPool* tp = new ThreadPool(bmh);
  for (int i = 0; i < high_tasks; i++) {
    Header* header = new Header();
    header->set_priority(Header::HIGH);
    header->set_rpc("short");
    tp->HandleMessage(header, new MessageBuffer());
  }
  for (int i = 0; i < low_tasks; i++) {
    Header* header = new Header();
    header->set_priority(Header::LOW);
    header->set_rpc("short");
    tp->HandleMessage(
        header,
        new MessageBuffer());
  }

  while (bmh->started() != high_tasks + low_tasks) {
    // Wait for all tasks to be running in thread pool
  }
  bmh->set_go(true);
  usleep(4000000);
  while (bmh->started() != 0) {
    // Wait for all tasks to finish
  }

  LOG(ERROR) << "---------------Benchmark Short Workload------------------";
  if (high_tasks != 0) {
    LOG(ERROR) << "High: "
               << ((double)17.3) / (1000*((double)bmh->high_duration())
                  / ((double)high_tasks));
  }
  if (low_tasks != 0) {
    LOG(ERROR) << "Low:  "
               << ((double)17.3) / (1000*((double)bmh->low_duration())
                  / ((double)low_tasks));
  }
}

// TODO(kunren): Have not tested sleep mostly, I currently don't know how to
//               test this work load.

int main(int argc, char **argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);

  // Set to true for benchmarking, false for testing.
  if (FLAGS_benchmark) {
    ThreadPoolBenchmarkThroughput();
    ThreadPoolLatencyDistributionTest();
    BenchmarkLoopyWorkload(FLAGS_high, FLAGS_low);
    BenchmarkShortWorkload(FLAGS_high, FLAGS_low);
    return 0;
  } else {
    return RUN_ALL_TESTS();
  }
}

