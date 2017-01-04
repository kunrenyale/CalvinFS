// Author: Alexander Thomson (thomson@cs.yale.edu)
//

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <google/profiler.h>
#include <gtest/gtest.h>

#include "common/types.h"
#include "common/utils.h"
#include "components/store/mvstore_app.h"
#include "components/scheduler/scheduler.h"
#include "machine/app/app.h"
#include "machine/app/reporter.h"
#include "machine/machine.h"
#include "proto/mvstore.pb.h"
#include "proto/tpcc.pb.h"

DEFINE_bool(benchmark, true, "Run benchmarks instead of unit tests.");

DEFINE_int32(read_delay, 0, "extra read delay (us)");
DEFINE_double(duration, 1, "Benchmark duration.");
DEFINE_bool(use_leveldb, false, "Use LevelDB instead of btree for backend.");
DEFINE_bool(gaussian, false, "Use gaussian key distribution?");
DEFINE_double(sigma, 20, "Sigma for gaussian key distribution.");
DEFINE_bool(tpcc, false, "Run TPCC New Order txns instead of Microbenchmark?");

DEFINE_string(scheduler, "Serial", "Scheduler to use.");
DEFINE_uint64(laziness, 1, "1/laziness txns are strict.");

DEFINE_int32(micro_writes, 10, "micro writeset size");

class Materialization {
 public:
  Materialization(const string& scheduler_type, bool use_leveldb)
      : scheduler_type_(scheduler_type + "Scheduler"),
        m_(0, ClusterConfig::LocalCluster(1)) {
    // Store.
    StartAppProto sap;
    sap.add_participants(0);
    if (use_leveldb) {
      sap.set_app("MVStoreApp_LevelDB");
    } else {
      sap.set_app("MVStoreApp");
    }
    sap.set_app_name("store");
    m_.AddApp(sap);

    // Scheduler.
    sap.set_app(scheduler_type_);
    sap.set_app_name("scheduler");
    m_.AddApp(sap);
  }

  ~Materialization() {
    // Stop reporter main loop before deleting other apps.
    m_.GetApp("reporter")->Stop();
  }

  void SetActionSource(Source<Action*>* requests) {
    reinterpret_cast<Scheduler*>(m_.GetApp("scheduler"))
        ->SetActionSource(requests);
  }

  void StartReporter() {
    // Reporter.
    StartAppProto sap;
    sap.add_participants(0);
    sap.set_app("Reporter");
    sap.set_app_name("reporter");
    m_.AddApp(sap);
  }

  void ReportThroughputBetween(double start, double end) {
    Reporter* r = reinterpret_cast<Reporter*>(m_.GetApp("reporter"));
    uint64 a = FromScalar<uint64>(r->QueryAtTime("store", "applied", start));
    uint64 b = FromScalar<uint64>(r->QueryAtTime("store", "applied", end));
    printf("%d %.3f  # read-delay(us) throughput", FLAGS_read_delay,
           (static_cast<double>(b) - static_cast<double>(a)) / (end - start));
  }
  void Report() {
    Reporter* r = reinterpret_cast<Reporter*>(m_.GetApp("reporter"));
    r->Stop();
    r->Query("store", "total");
    r->Query("store", "applied");
    r->Query("store", "unevaluated");
    r->Query("store", "inprogress");
    r->Query("store", "evalthreads");
    ReportThroughputBetween(FLAGS_duration * 0.4, FLAGS_duration * 0.9);
  }

 private:
  string scheduler_type_;
  Machine m_;
};

template<typename T>
class PreparedSource : public Source<T> {
 private:
  static const uint64 kTotalActions = 1000000;

 public:
  PreparedSource() : version_(0) {}
  virtual ~PreparedSource() {}

  virtual bool Get(Action** a) {
    if (version_ < kTotalActions) {
      *a = actions_[version_++];
      (*a)->set_version(version_);  // action.version() starts at 1.
      return true;
    }
    return false;
  }

 protected:
  // All subclasses' constructors should call this method.
  void Setup() {
    for (uint32 i = 0; i < kTotalActions; i++) {
      actions_.push_back(CreateElement(i));
    }
  }

  // All subclasses must define this method.
  virtual T CreateElement(int i) = 0;

  uint64 version_;
  vector<T> actions_;
};

class MicroSource : public PreparedSource<Action*> {
 private:
  static const int kReadSetSize = 20;
  static const int kWriteSetSize = 2;

 public:
  MicroSource() {
    Setup();
  }

  virtual ~MicroSource() {}

 protected:
  virtual Action* CreateElement(int i) {
    Action* a = new Action();
    a->set_action_type("MICRO");
    if (i % FLAGS_laziness == 0) {
      a->set_run_strict(true);
    }
    MVStoreMicroInput in;
    // Add 10 unique keys.
    if (FLAGS_gaussian) {
      set<int> keys;
      // Uniform inital key.
      int initial = rand() % 100000;
      keys.insert(initial);
      while (keys.size() < static_cast<uint32>(kReadSetSize)) {
        keys.insert(
            (initial + static_cast<int>(RandomGaussian(FLAGS_sigma)) + 100000)
                % 100000);
      }
      for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it) {
        in.add_keys(IntToString(*it));
      }
    } else {
      set<int> keys;
      while (keys.size() < static_cast<uint32>(kReadSetSize)) {
        keys.insert(rand() % 100000);
      }
      for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it) {
        in.add_keys(IntToString(*it));
      }
    }
    in.set_writes(kWriteSetSize);
    in.SerializeToString(a->mutable_input());
    MVStoreApp::GetActionType(*a)->ComputeRWSets(a);
    a->set_read_delay(FLAGS_read_delay);
    return a;
  }

};


class NewOrderSource : public PreparedSource<Action*> {
 private:
  static const uint32 kOrderLineCount = 10;

 public:
  NewOrderSource() {
    Setup();
  }

  virtual ~NewOrderSource() {}

 protected:
  virtual Action* CreateElement(int i) {
    Action* a = new Action();
    a->set_action_type("NO");
    if (i % FLAGS_laziness == 0) {
      a->set_run_strict(true);
    }
    NewOrderInput in;
    in.set_district(rand() % 10);
    in.set_customer(rand() % 30000);
    in.set_order(i);

    set<int> keys;

    // 1% of txns have an invalid key.
    int initial = (rand() % 100 == 0) ? 100001 : (rand() % 100000);
    keys.insert(initial);

    while (keys.size() < kOrderLineCount) {
      keys.insert(
        FLAGS_gaussian
        ? ((initial + static_cast<int>(RandomGaussian(FLAGS_sigma)) + 100000)
            % 100000)
        : (rand() % 100000));
    }

    for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it) {
      in.add_ol_item(*it);
    }
    in.SerializeToString(a->mutable_input());
    MVStoreApp::GetActionType(*a)->ComputeRWSets(a);
    a->set_read_delay(FLAGS_read_delay);
    return a;
  }
};

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  LOG(ERROR) << "Setting up DB...";
  Materialization m(FLAGS_scheduler, FLAGS_use_leveldb);

  LOG(ERROR) << "Creating workload...";
  Source<Action*>* source =
      FLAGS_tpcc
      ? reinterpret_cast<Source<Action*>*>(new NewOrderSource())
      : reinterpret_cast<Source<Action*>*>(new MicroSource());

  LOG(ERROR) << "Running for " << FLAGS_duration << " seconds...\n";
  m.StartReporter();
  m.SetActionSource(source);
  Spin(FLAGS_duration);

  m.Report();
  return 0;
}

