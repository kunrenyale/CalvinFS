// Author: Kun Ren <kun@cs.yale.edu>
//

#ifndef CALVIN_COMPONENTS_STORE_TPCC_H_
#define CALVIN_COMPONENTS_STORE_TPCC_H_

#include <string>
#include "components/store/kvstore.h"

class TpccStore : public Store {
 public:
  explicit TpccStore(KVStore* store);
  ~TpccStore();

  // Types of actions that MicrobenchmarkStore can interpret.
  virtual void GetRWSets(Action* action);
  virtual void Run(Action* action);

  virtual bool IsLocal(const string& path);
  virtual uint32 LookupReplicaByDir(string dir);
  virtual uint64 GetHeadMachine(uint64 machine_id);
  virtual uint32 LocalReplica();

  KVStore* records_;
}














#endif  // CALVIN_COMPONENTS_STORE_TPCC_H_
