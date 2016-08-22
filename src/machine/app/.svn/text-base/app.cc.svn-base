// Author: Alex Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun@cs.yale.edu)
//

#include "machine/app/app.h"
#include <atomic>
#include <map>
#include <string>
#include "common/mutex.h"

using std::atomic;
using std::map;
using std::string;

SAState* GetState() {
  static SAState state_;
  return &state_;
}

bool AddStartableApp(const string& app, StartApp* startapp) {
  SAState* state = GetState();
  Lock l(&state->mutex_);

  // Register app at most once.
  if (state->startable_apps_.count(app) == 0) {
    state->startable_apps_[app] = startapp;
  }
  return true;
}

