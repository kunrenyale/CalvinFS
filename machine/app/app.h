// Author: Alex Thomson (thomson@cs.yale.edu)
//
// Interface for a distributed application to run on a machine deployment.
// See (TODO) for an annotated example of a machine App.
//
// TODO(agt): Formalize some kind of convention for tracking multiple
// all machines' App instances for a given distributed application. Also,
// figure out how best to specify how an App should behave in the presence of
// failures....

#ifndef CALVIN_MACHINE_APP_APP_H_
#define CALVIN_MACHINE_APP_APP_H_

#include <glog/logging.h>
#include <google/protobuf/message.h>
#include <atomic>
#include <map>
#include <string>
#include "common/atomic.h"
#include "common/types.h"
#include "common/utils.h"
#include "common/mutex.h"
#include "machine/machine.h"
#include "machine/message_buffer.h"
#include "machine/message_handler.h"
#include "proto/header.pb.h"
#include "proto/report.pb.h"

using std::atomic;
using std::map;
using std::string;

class App : public MessageHandler {
 public:
  virtual ~App() {}

  // Returns the name identifying the App at the local machine.
  const string& name() { return name_; }

  // The app takes ownership of '*header' and '*message'. This function is
  // generally called by a thread pool's worker thread.
  virtual void HandleMessage(Header* header, MessageBuffer* message) = 0;

  // Starts running a main loop in the AppStarter thread that created/
  // registered the app. Not required to terminate. Default implementation:
  // noop (i.e. no main loop).
  virtual void Start() {}

  // If 'Start()' implements a continuous loop, Stop() must signal that loop to
  // halt and not return until it does.
  virtual void Stop() {}

  // Returns a heap-allocated status report proto for the app (of which the
  // caller takes ownership) or NULL if the app has no data to report, which
  // is the default implementation.
  virtual Report* GetReport() {
    return NULL;
  }

 protected:
  // Utility method for creating a report for this app at this timestamp but
  // with no actual data.
  Report* NewReport() {
    Report* report = new Report();
    report->set_app(name());
    report->set_time(GetTime());
    return report;
  }

  // Returns a handle on the local machine to which this App is registered.
  // This is 'protected' because it is meant to be used by subclasses
  inline Machine* machine() { return machine_; }

 private:
  friend class Machine;
  friend class AppStarter;

  // Every App must be registered with a name. Names must be unique per Machine.
  // An App's name is set when it is registered to a Machine and is immutable
  // thereafter.
  string name_;

  // Machine to which the App is registered.
  Machine* machine_;
};

class StartApp {
 public:
  virtual ~StartApp() {}
  virtual App* Go(const string& args) = 0;
};

#define REGISTER_APP(APP)                                   \
class StartApp_##APP : public StartApp {                    \
 public:                                                    \
  virtual ~StartApp_##APP() {}                              \
  virtual App* Go(const string& ARG);                       \
  static bool _UNUSED_CLASS_VARIABLE_;                      \
};                                                          \
bool StartApp_##APP::_UNUSED_CLASS_VARIABLE_ =              \
    AddStartableApp(#APP, new StartApp_##APP());            \
App* StartApp_##APP::Go(const string& ARG)

struct SAState {
  Mutex mutex_;
  map<string, StartApp*> startable_apps_;
};

SAState* GetState();

bool AddStartableApp(const string& app, StartApp* startapp);

#endif  // CALVIN_MACHINE_APP_APP_H_

