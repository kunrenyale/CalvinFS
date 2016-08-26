// Author: Alex Thomson

#include "machine/cluster_config.h"

#include <map>
#include <set>
#include <string>
#include <vector>

#include "common/types.h"
#include "common/utils.h"
#include "leveldb/env.h"

using std::map;
using std::set;
using std::string;

// Checks to see if a config string appears to be a valid cluster config repr.
void CheckString(const string& config) {
  // Track machine ids and host-port pairs that we have already seen to ensure
  // that none are repeated.
  set<uint64> machine_ids;
  set<string> host_port_pairs;

  // Split config string into lines.
  vector<string> entries = SplitString(config, '\n');
  for (uint32 i = 0; i < entries.size(); i++) {
    // Skip empty lines.
    if (!entries[i].empty()) {
      // Each entry should consist of three colon-delimited parts: id, host and
      // port. Parse entry and check validity of id/host/port.
      vector<string> entry = SplitString(entries[i], ':');
      CHECK(entry.size() == 3) << "bad config line: " << entries[i];
      uint64 id = static_cast<uint64>(StringToInt(entry[0]));
      string host = entry[1];
      int port = StringToInt(entry[2]);
      CHECK(static_cast<int64>(id) >= 0)
          << "bad machine id: " << static_cast<int64>(id);
      CHECK(host.size() > 0) << "empty hostname";
      CHECK(port >= 0) << "bad port: " << port;

      // Check for repeated machine ids.
      CHECK(machine_ids.count(id) == 0)
          << "repeated machine id: " << id;
      machine_ids.insert(id);

      // Check for repeated host/port pairs.
      string hostport = host + ":" + IntToString(port);
      CHECK(host_port_pairs.count(hostport) == 0)
          << "repeated host/port pair: " << hostport;
      host_port_pairs.insert(hostport);
    }
  }
}

// Checks to see if a config proto appears to be a valid cluster config repr.
void CheckProto(const ClusterConfigProto& config) {
  // Track machine ids and host-port pairs that we have already seen to ensure
  // that none are repeated.
  set<uint64> machine_ids;
  set<string> host_port_pairs;

  for (int i = 0; i < config.machines_size(); i++) {
    // Check validity of id/host/port.
    CHECK(config.machines(i).has_id()) << "missing machind id";
    CHECK(config.machines(i).has_host())<< "missing host";
    CHECK(config.machines(i).has_port())<< "missing port";
    CHECK(static_cast<int64>(config.machines(i).id()) >= 0)
        << "bad machine id: " << static_cast<int64>(config.machines(i).id());
    CHECK(config.machines(i).host().size() > 0)
        << "empty hostname";
    CHECK(config.machines(i).port() >= 0)
        << "bad port: " << config.machines(i).port();

    // Check for repeated machine ids.
    CHECK(machine_ids.count(config.machines(i).id()) == 0)
        << "repeated machine id: " << config.machines(i).id();
    machine_ids.insert(config.machines(i).id());

    // Check for repeated host/port pairs.
    string hostport = config.machines(i).host() + ":" +
                      IntToString(config.machines(i).port());
    CHECK(host_port_pairs.count(hostport) == 0)
        << "repeated host/port pair: " << hostport;
    host_port_pairs.insert(hostport);
  }
}

ClusterConfig ClusterConfig::LocalCluster(int n) {
  string s;
  for (int i = 0; i < n; i++) {
    s.append(IntToString(i));
    s.append(":localhost:");
    s.append(IntToString(20000+i));
    s.append("\n");
  }
  // remove trailing newline
  s.resize(s.size()-1);
  ClusterConfig cc;
  cc.FromString(s);
  return cc;
}

void ClusterConfig::FromFile(const string& filename) {
  string config;
  leveldb::Status s = leveldb::ReadFileToString(
      leveldb::Env::Default(),
      filename,
      &config);
  CHECK(s.ok()) << "failed to read file: " + filename;
  FromString(config);
}

void ClusterConfig::FromString(const string& config) {
  CheckString(config);

  // Clear any previous machine information.
  machines_.clear();

  // Split config string into lines.
  vector<string> entries = SplitString(config, '\n');
  for (uint32 i = 0; i < entries.size(); i++) {
    // Skip empty lines.
    if (!entries[i].empty()) {
      // Each entry should consist of three colon-delimited parts: id, host and
      // port. Parse entry and check validity of id/host/port.
      vector<string> entry = SplitString(entries[i], ':');

      // Add entry.
      uint64 id = static_cast<uint64>(StringToInt(entry[0]));
      machines_[id].set_id(id);
      machines_[id].set_host(entry[1]);
      machines_[id].set_port(StringToInt(entry[2]));
    }
  }
}

void ClusterConfig::FromProto(const ClusterConfigProto& config) {
  machines_.clear();
  for (int i = 0; i < config.machines_size(); i++) {
    machines_[config.machines(i).id()] = config.machines(i);
  }
}

void ClusterConfig::ToFile(const string& filename) {
  string config;
  ToString(&config);
  leveldb::Status s = leveldb::WriteStringToFile(
      leveldb::Env::Default(),
      filename,
      config);
  CHECK(s.ok()) << "failed to write file: " + filename;
}

void ClusterConfig::ToString(string* out) {
  // TODO(agt): Implement this?
}

void ClusterConfig::ToProto(ClusterConfigProto* out) {
  // TODO(agt): Implement this?
}
