// Author: Kun Ren <kun@cs.yale.edu>
//

#ifndef CALVIN_COMPONENTS_STORE_MICROBENCHMARK_H_
#define CALVIN_COMPONENTS_STORE_MICROBENCHMARK_H_

#include <string>
#include "components/store/kvstore.h"

class MicrobenchmarkStore : public Store {
 public:
  explicit MicrobenchmarkStore(KVStore* store);
  ~MicrobenchmarkStore();

  // Types of actions that MicrobenchmarkStore can interpret.
  virtual void GetRWSets(Action* action);
  virtual void Run(Action* action);
  virtual bool IsLocal(const string& path);

  KVStore* records_;
}














#endif  // CALVIN_COMPONENTS_STORE_MICROBENCHMARK_H_
