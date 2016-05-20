// Author: Alex Thomson
//
// A ClusterConfig represents a local view of a collection of Machines.
// Constructing a ClusterConfig object does not actually deploy an application
// across the collection of physical servers (use ClusterManager for that).
//
// TODO(agt): Also need a protobuf version of this. (In fact, MachineInfo
//            should probably be a protobuf in the first place.)
//

#ifndef CALVIN_MACHINE_CLUSTER_CONFIG_H_
#define CALVIN_MACHINE_CLUSTER_CONFIG_H_

#include <map>
#include <string>

#include "common/types.h"
#include "proto/cluster_config.pb.h"

using std::map;
using std::string;

class ClusterConfig {
 public:
  // Default constructor creates a ClusterConfig consisting of no Machines.
  // Note that this is completely useless until 'FromFile()' or 'FromString()'
  // is called to populate it with information about the actual cluster
  // configuration.
  ClusterConfig() {}

  // Returns a clusterconfig with n machines numbered 0 through (n-1), all on
  // localhost, and with consecutive unique ports starting at 10000.
  static ClusterConfig LocalCluster(int n);

  // Populates a ClusterConfig using a specification consisting of zero or
  // more (newline delimited) lines of the format:
  //
  //    <machine-id>:<ip>:<port>
  //
  // The specification can either be read from a file, provided as a string,
  // or provided as a protobuf (see proto/cluster_config.proto).
  //
  // Each MachineID that appears in the specification must be unique, as must
  // each (ip, port) pair.
  void FromFile(const string& filename);
  void FromString(const string& config);
  void FromProto(const ClusterConfigProto& config);

  // ClusterConfigs can also be written out to files, strings, or protos.
  void ToFile(const string& filename);
  void ToString(string* out);
  void ToProto(ClusterConfigProto* out);

  // Returns the number of machines that appear in the config.
  inline int size() const {
    return static_cast<int>(machines_.size());
  }

  // Returns true and populates '*info' accordingly iff the config tracks a
  // machine with id 'id'.
  inline bool lookup_machine(uint64 id, MachineInfo* info) {
    if (machines_.count(id)) {
      *info = machines_[id];
      return true;
    }
    return false;
  }

  // Returns a const ref to the underlying collection of machine records.
  const map<uint64, MachineInfo>& machines() {
    return machines_;
  }

 private:
  friend class Machine;

  // Contains all machines.
  map<uint64, MachineInfo> machines_;

  // Intentionally copyable.
};

#endif  // CALVIN_MACHINE_CLUSTER_CONFIG_H_

