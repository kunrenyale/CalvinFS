// Author: Alex Thomson (thomson@cs.yale.edu)
//

#ifndef CALVIN_MACHINE_APP_REPORTER_H_
#define CALVIN_MACHINE_APP_REPORTER_H_

#include <glog/logging.h>
#include <map>

#include "common/utils.h"
#include "machine/app/app.h"
#include "machine/machine.h"
#include "proto/report.pb.h"

using std::map;

class Reporter : public App {
 public:
  explicit Reporter(int interval_in_usecs = 1000)
    : interval_(interval_in_usecs), go_(true), going_(false) {
  }
  virtual ~Reporter() {
    Stop();
  }

  virtual void HandleMessage(Header* header, MessageBuffer* message) {
    // TODO(agt): Implement RPC interface for reporting in distributed
    //            deployments.
  }

  virtual void Start() {
    // Make a list of apps to poll (to avoid having to call machine()->GetApp()
    // too much).
    vector<App*> apps;
    for (map<string, App*>::iterator it = machine()->apps_.begin();
         it != machine()->apps_.end(); ++it) {
      // No need to get reports from self.
      if (it->first != name()) {
        apps.push_back(it->second);
      }
    }

    double start_time = GetTime();

    going_ = true;
    while (go_) {
      Lock* lock = new Lock(&mutex_);
      double time = GetTime() - start_time;
      for (uint32 i = 0; i < apps.size(); i++) {
        Report* report = apps[i]->GetReport();
        if (report != NULL) {
          // Ignore empty reports.
          if (report->data_size() != 0) {
            report->set_app(apps[i]->name());
            report->set_time(time);
            reports_.push_back(report);
          } else {
            delete report;
          }
        }
      }
      delete lock;
      usleep(interval_);
    }
    going_ = false;
  }

  virtual void Stop() {
    go_ = false;
    // Wait for main loop to stop.
    while (going_) {
      Noop<bool>(going_);
    }
  }

  void Dump() {
    Lock l(&mutex_);
    for (uint32 i = 0; i < reports_.size(); i++) {
      LOG(ERROR) << reports_[i]->DebugString();
    }
  }

  void Query(string app, string quantity) {
    for (uint32 i = 0; i < reports_.size(); i++) {
      if (reports_[i]->app() == app) {
        for (int j = 0; j < reports_[i]->data_size(); j++) {
          if (reports_[i]->data(j).quantity() == quantity) {
            printf("%.4f  %s     # %s.%s\n",
                   reports_[i]->time(),
                   ShowScalar(reports_[i]->data(j).value()).c_str(),
                   reports_[i]->app().c_str(),
                   reports_[i]->data(j).quantity().c_str());
          }
        }
      }
    }
  }

  Scalar QueryAtTime(string app, string quantity, double time) {
    for (uint32 i = 0; i < reports_.size(); i++) {
      if (reports_[i]->time() >= time && reports_[i]->app() == app) {
        for (int j = 0; j < reports_[i]->data_size(); j++) {
          if (reports_[i]->data(j).quantity() == quantity) {
            return reports_[i]->data(j).value();
          }
        }
      }
    }
    LOG(FATAL) << "no result found";
    return Scalar();
  }


 private:
  // Number of microseconds to sleep between polls.
  int interval_;

  // True iff main thread SHOULD run.
  bool go_;

  // True iff main thread IS running.
  bool going_;

  // All reports collected.
  vector<Report*> reports_;

  // Mutex for atomically reading/appending to reports_;
  Mutex mutex_;
};

#endif  // CALVIN_MACHINE_APP_REPORTER_H_

