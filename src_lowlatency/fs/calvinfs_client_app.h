// Author: Alexander Thomson <thomson@cs.yale.edu>
//         Kun  Ren <kun.ren@yale.edu>
//

#ifndef CALVIN_FS_CALVINFS_CLIENT_APP_H_
#define CALVIN_FS_CALVINFS_CLIENT_APP_H_

#include <leveldb/env.h>

#include "components/scheduler/scheduler.h"
#include "components/store/store.h"
#include "components/store/store_app.h"
#include "fs/block_log.h"
#include "fs/calvinfs.h"
#include "fs/metadata.pb.h"
#include "fs/metadata_store.h"
#include "machine/app/app.h"
#include "machine/machine.h"

using std::make_pair;

class CalvinFSClientApp : public App {
 public:
  CalvinFSClientApp()
      : capacity_(kMaxCapacity), go_(true), going_(false), reporting_(false) {
  }
  virtual ~CalvinFSClientApp() {
    go_ = false;
    while (going_.load()) {}
  }

  virtual void Start() {
    action_count_ = 0;
    for (int i = 0; i < 20000000; i++) {
      random_data_.push_back(rand() % 256);
    }

    latencies_["touch"] = new AtomicQueue<double>();
    latencies_["mkdir"] = new AtomicQueue<double>();
    latencies_["append"] = new AtomicQueue<double>();
    latencies_["copy"] = new AtomicQueue<double>();
    latencies_["rename"] = new AtomicQueue<double>();
    latencies_["ls"] = new AtomicQueue<double>();
    latencies_["cat0"] = new AtomicQueue<double>();
    latencies_["cat1"] = new AtomicQueue<double>();
    latencies_["cat10"] = new AtomicQueue<double>();
    latencies_["cat100"] = new AtomicQueue<double>();


    config_ = new CalvinFSConfigMap(machine());
    replica_ = config_->LookupReplica(machine()->machine_id());
    blocks_ = reinterpret_cast<DistributedBlockStoreApp*>(
                    machine()->GetApp("blockstore"));
    log_ = reinterpret_cast<BlockLogApp*>(machine()->GetApp("blocklog"));
    scheduler_ = reinterpret_cast<Scheduler*>(machine()->GetApp("scheduler"));
    metadata_ =
        reinterpret_cast<MetadataStore*>(
           reinterpret_cast<StoreApp*>(machine()->GetApp("metadata"))->store());

    Spin(1);

    switch(experiment) {
      case 0:
        FillExperiment();
        break;

      case 1:
        ConflictingAppendExperiment();
        break;

      case 2:
        RandomAppendExperiment();
        break;

      case 3:
        CopyExperiment();
        break;

      case 4:
        RenameExperiment();
        break;

      case 5:
        LatencyExperimentReadFile();
        break;

      case 6:
        LatencyExperimentCreateFile();
        break;

      case 7:
        LatencyExperimentAppend();
        break;

      case 8:
        LatencyExperimentMix();
        break;

      case 9:
        CrashExperiment();
        break;

    }

  }
  static const int kMaxCapacity = 100;

  virtual void HandleMessage(Header* header, MessageBuffer* message) {
    // INTERNAL metadata lookup
    if (header->rpc() == "LOOKUP") {
      machine()->SendReplyMessage(
          header,
          GetMetadataEntry(header->misc_string(0)));

    // EXTERNAL LS
    } else if (header->rpc() == "LS") {
      machine()->SendReplyMessage(header, LS(header->misc_string(0)));

    // EXTERNAL read file
    } else if (header->rpc() == "READ_FILE") {
      machine()->SendReplyMessage(header, ReadFile(header->misc_string(0)));

    // EXTERNAL file/dir creation
    } else if (header->rpc() == "CREATE_FILE") {
      machine()->SendReplyMessage(header, CreateFile(
          header->misc_string(0),
          header->misc_bool(0) ? DIR : DATA));

    // EXTERNAL file append
    } else if (header->rpc() == "APPEND") {
      machine()->SendReplyMessage(header, AppendStringToFile(
          (*message)[0],
          header->misc_string(0)));

   // EXTERNAL file copy
   } else if (header->rpc() == "COPY_FILE") {
     machine()->SendReplyMessage(header, CopyFile(
         header->misc_string(0),
         header->misc_string(1)));
     
   // EXTERNAL file copy
   } else if (header->rpc() == "RENAME_FILE") {
     machine()->SendReplyMessage(header, RenameFile(
         header->misc_string(0),
         header->misc_string(1)));

    // Callback for recording latency stats
    } else if (header->rpc() == "CB") {
      double end = GetTime();
      string category = header->misc_string(1);
      if (category == "cat") {
        if ((*message)[0] == "metadata lookup error\n") {
          latencies_["cat0"]->Push(
              end - header->misc_double(0));
          delete header;
          delete message;
          return;
        }
        MetadataEntry result;
        result.ParseFromArray((*message)[0].data(), (*message)[0].size());
        CHECK(result.has_type());

        if (result.file_parts_size() == 0) {
          category.append("0");

        } else if (result.file_parts_size() == 1) {
          category.append("1");

        } else if (result.file_parts_size() <= 10) {
          category.append("10");

        } else {
          category.append("100");
        }
      }

      latencies_[category]->Push(
          end - header->misc_double(0));

      delete header;
      delete message;

    } else {
      LOG(FATAL) << "unknown RPC: " << header->rpc();
    }
  }

  uint64 RandomBlockSize() {
    return 1000000 / (1 + rand() % 9999);
  }

  void FillExperiment() {
    Spin(1);
    metadata_->Init();
    Spin(1);
    machine()->GlobalBarrier();
    Spin(1);

    string tld("/a" + UInt64ToString(machine()->machine_id()));
    int dirs = 1000;
    int files = 10;

    // Put files into second-level dir.
    double start = GetTime();
    for (int i = 0; i < files; i++) {
      string file = "/d" + IntToString(i);
      for (int j = 0; j < dirs; j++) {
        BackgroundCreateFile(tld + "/b" + IntToString(j) + file);
      }
      LOG(ERROR) << "[" << machine()->machine_id() << "] "
                 << "Added file d" << i << " to " << dirs << " dirs";
    }
    // Wait for all operations to finish.
    while (capacity_.load() < kMaxCapacity) {
      usleep(10);
    }
    // Report.
    LOG(ERROR) << "[" << machine()->machine_id() << "] "
               << "Created " << dirs * files << " files. Elapsed time: "
               << (GetTime() - start) << " seconds";
  }

  void ConflictingAppendExperiment() {
    int files = 2;
    // Create 1k top-level files.
    if (machine()->machine_id() == 0) {
      for (int i = 0; i < files; i++) {
        BackgroundCreateFile("/f" + IntToString(i));
      }
      // Wait for all operations to finish.
      while (capacity_.load() < kMaxCapacity) {
        usleep(10);
      }
    }

    // 1k appends to random files.
    Spin(1);
    machine()->GlobalBarrier();
    Spin(1);
    double start = GetTime();
    int iterations = 5;
    for (int a = 0; a < iterations; a++) {
      for (int i = 0; i < 1000; i++) {
        // Append.
        BackgroundAppendStringToFile(
            RandomData(RandomBlockSize()),
            "/f" + IntToString(rand() % files));
      }
      LOG(ERROR) << "[" << machine()->machine_id() << "] "
                 << "CAppendExperiment progress: " << a+1 << "/" << iterations;
    }
    // Wait for all operations to finish.
    while (capacity_.load() < kMaxCapacity) {
      usleep(10);
    }
    // Report.
    LOG(ERROR) << "[" << machine()->machine_id() << "] "
               << iterations << "k conflicting appends. Elapsed time: "
               << (GetTime() - start) << " seconds";
  }
  void RandomAppendExperiment() {
    // Create M * 1k top-level files.
    for (int i = 0; i < 100; i++) {
      BackgroundCreateFile(
        "/f" + UInt64ToString(machine()->machine_id()) + "." + IntToString(i));
    }
    // Wait for all operations to finish.
    while (capacity_.load() < kMaxCapacity) {
      usleep(10);
    }

    // 1k appends to random files.
    Spin(1);
    machine()->GlobalBarrier();
    Spin(1);
    double start = GetTime();
    int iterations = 5;
    for (int a = 0; a < iterations; a++) {
      for (int i = 0; i < 1000; i++) {
        // Append.
        BackgroundAppendStringToFile(
            RandomData(RandomBlockSize()),
            "/f" + IntToString(rand() % 100));
      }
      LOG(ERROR) << "[" << machine()->machine_id() << "] "
                 << "RAppendExperiment progress: " << a+1 << "/" << iterations;
    }
    // Wait for all operations to finish.
    while (capacity_.load() < kMaxCapacity) {
      usleep(10);
    }
    // Report.
    LOG(ERROR) << "[" << machine()->machine_id() << "] "
               << iterations << "k random appends. Elapsed time: "
               << (GetTime() - start) << " seconds";
  }

  string RandomFile() {
    return "/a" + IntToString(machine()->machine_id()) +
           "/b" + IntToString(rand() % 100) + "/c";
  }

  void LatencyExperimentSetup() {
    Spin(1);
    metadata_->InitSmall();
    Spin(1);
    machine()->GlobalBarrier();
    Spin(1);

    string tld("/a" + IntToString(machine()->machine_id()));

    // Append to some files.
    for (int i = 0; i < 1000; i++) {
      while (rand() % 3 == 0) {
        BackgroundAppendStringToFile(
            RandomData(RandomBlockSize()),
            tld + "/b" + IntToString(i) + "/c");
      }
      if (i % 100 == 0) {
        LOG(ERROR) << "[" << machine()->machine_id() << "] "
                   << "LE prep progress C: " << i / 100 << "/" << 10;
      }
    }
    Spin(1);

    // Wait for all operations to finish.
    while (capacity_.load() < kMaxCapacity) {
      usleep(10);
    }
    LOG(ERROR) << "[" << machine()->machine_id() << "] LE prep complete";
  }

 
  void LatencyExperimentReadFile() {
    // Setup.
    LatencyExperimentSetup();

    Spin(1);
    machine()->GlobalBarrier();
    Spin(1);

    // Begin mix of operations.
    reporting_ = true;
    double start = GetTime();
    for (int i = 0; i < 1000; i++) {
      BackgroundReadFile(RandomFile());

      if (i % 10 == 0) {
        LOG(ERROR) << "[" << machine()->machine_id() << "] "
                   << "LE test progress: " << i / 10 << "/" << 100;
      }
    }

    // Wait for all operations to finish.
    while (capacity_.load() < kMaxCapacity) {
      usleep(10);
    }
    // Report.
    LOG(ERROR) << "[" << machine()->machine_id() << "] "
               << "ReadFile workload completed. Elapsed time: "
               << (GetTime() - start) << " seconds";

    // Write out latency reports.
    Report();
  }
 
void LatencyExperimentCreateFile() {
    // Setup.
    LatencyExperimentSetup();

    Spin(1);
    machine()->GlobalBarrier();
    Spin(1);

    // Begin mix of operations.
    reporting_ = true;
    double start = GetTime();
    for (int i = 0; i < 10000; i++) {
      BackgroundCreateFile(
            "/a" + IntToString(machine()->machine_id()) +
            "/b" + IntToString(rand() % 1000) +
            "/x" + UInt64ToString(1000 + machine()->GetGUID()));

      if (i % 100 == 0) {
        LOG(ERROR) << "[" << machine()->machine_id() << "] "
                   << "LE test progress: " << i / 100 << "/" << 100;
      }
    }
    // Wait for all operations to finish.
    while (capacity_.load() < kMaxCapacity) {
      usleep(10);
    }
    // Report.
    LOG(ERROR) << "[" << machine()->machine_id() << "] "
               << "CreateFile workload completed. Elapsed time: "
               << (GetTime() - start) << " seconds";

    // Write out latency reports.
    Report();
  }

void LatencyExperimentAppend() {
    // Setup.
    LatencyExperimentSetup();

    Spin(1);
    machine()->GlobalBarrier();
    Spin(1);

    // Begin mix of operations.
    reporting_ = true;
    double start = GetTime();
    for (int i = 0; i < 1000; i++) {
      BackgroundAppendStringToFile(RandomData(RandomBlockSize()), RandomFile());

      if (i % 10 == 0) {
        LOG(ERROR) << "[" << machine()->machine_id() << "] "
                   << "LE test progress: " << i / 10 << "/" << 100;
      }
    }
    // Wait for all operations to finish.
    while (capacity_.load() < kMaxCapacity) {
      usleep(10);
    }
    // Report.
    LOG(ERROR) << "[" << machine()->machine_id() << "] "
               << "Append workload completed. Elapsed time: "
               << (GetTime() - start) << " seconds";

    // Write out latency reports.
    Report();
  }

  void LatencyExperimentMix() {
    // Setup.
    LatencyExperimentSetup();

    Spin(1);
    machine()->GlobalBarrier();
    Spin(1);

    // Begin mix of operations.
    reporting_ = true;
    double start = GetTime();
    for (int i = 0; i < 1000; i++) {
      int seed = rand() % 100;

      // 60% read operations
      if (seed < 60) {
        BackgroundReadFile(RandomFile());

      // 10% file creation operations
      } else if (seed < 70) {
        BackgroundCreateFile(
            "/a" + IntToString(rand() % machine()->config().size()) +
            "/b" + IntToString(rand() % 100) +
            "/x" + UInt64ToString(1000 + machine()->GetGUID()));

      // 30% append operations
      } else {
        BackgroundAppendStringToFile(
            RandomData(RandomBlockSize()),
            RandomFile());
      }
      if (i % 10 == 0) {
        LOG(ERROR) << "[" << machine()->machine_id() << "] "
                   << "LE test progress: " << i / 10 << "/" << 100;
      }
    }
    // Wait for all operations to finish.
    while (capacity_.load() < kMaxCapacity) {
      usleep(10);
    }
    // Report.
    LOG(ERROR) << "[" << machine()->machine_id() << "] "
               << "Mixed workload completed. Elapsed time: "
               << (GetTime() - start) << " seconds";

    // Write out latency reports.
    Report();
  }

  void CrashExperimentSetup() {
    Spin(1);
    machine()->GlobalBarrier();
    Spin(1);

    // Create top-level dir.
    string tld("/a" + IntToString(machine()->machine_id()));
    CreateFile(tld, DIR);
    Spin(1);

    // Create subdirs.
    for (int i = 0; i < 1000; i++) {
      BackgroundCreateFile(tld + "/b" + IntToString(i), DIR);
      if (i % 10 == 0) {
        LOG(ERROR) << "[" << machine()->machine_id() << "] "
                   << "CE prep progress A: " << i / 10 << "/" << 100;
      }
    }
    Spin(1);

    // Create files.
    for (int i = 0; i < 1000; i++) {
      BackgroundCreateFile(tld + "/b" + IntToString(i) + "/c", DATA);
      if (i % 10 == 0) {
        LOG(ERROR) << "[" << machine()->machine_id() << "] "
                   << "CE prep progress B: " << i / 10 << "/" << 100;
      }
    }
    Spin(1);

    // Append to some files.
    for (int i = 0; i < 1000; i++) {
      while (rand() % 2 == 0) {
        BackgroundAppendStringToFile(
            RandomData(RandomBlockSize()),
            tld + "/b" + IntToString(i) + "/c");
      }
      if (i % 10 == 0) {
        LOG(ERROR) << "[" << machine()->machine_id() << "] "
                   << "CE prep progress C: " << i / 10 << "/" << 100;
      }
    }
    Spin(1);

    // Wait for all operations to finish.
    while (capacity_.load() < kMaxCapacity) {
      usleep(10);
    }
    LOG(ERROR) << "[" << machine()->machine_id() << "] CE prep complete";
  }

  string RandomFileC() {
    return "/a" + IntToString(rand() % machine()->config().size()) +
           "/b" + IntToString(rand() % 1000) + "/c";
  }

  void CrashExperiment() {
    // Setup.
    CrashExperimentSetup();
    
    Spin(1);
    machine()->GlobalBarrier();
    Spin(1);

    reporting_ = true;
    double start = GetTime();
    double tick = start + 1;
    int tickid = 0;
    while (GetTime() < start + 60) {
      double t = GetTime();
      if (t > tick) {
        string report("\n");
        for (auto it = latencies_.begin(); it != latencies_.end(); ++it) {
          vector<double> v;
          double d;
          while (it->second->Pop(&d)) {
            v.push_back(d);
          }
          sort(v.begin(), v.end());
          if (!v.empty()) {
            report.append(
                "[" + UInt64ToString(machine()->machine_id()) + "] " +
                IntToString(tickid) + " " +
                IntToString(v.size()) + " "
                + it->first + "-count\n");
            report.append(
                "[" + UInt64ToString(machine()->machine_id()) + "] " +
                IntToString(tickid) + " " +
                DoubleToString(v[v.size() / 2]) + " "
                + it->first + "-median\n");
            report.append(
                "[" + UInt64ToString(machine()->machine_id()) + "] " +
                IntToString(tickid) + " " +
                DoubleToString(v[v.size() * 99 / 100]) + " "
                + it->first + "-99th-percentile\n");
          }
        }
        LOG(ERROR) << report;
        
        if (tickid == 30) {
          if (replica_ == 1) {
            LOG(ERROR) << "[" + UInt64ToString(machine()->machine_id()) + "] "
                       << "KABOOM!";
            exit(0);
          } else {
            capacity_ += kMaxCapacity / 2;
          }
        }
        tick += 1;
        tickid++;
      }

      if (machine()->machine_id() % 3 == 0) {
        BackgroundReadFile(RandomFileC());

      } else if (machine()->machine_id() % 3 == 1) {
        BackgroundCreateFile(
            "/a" + IntToString(rand() % machine()->config().size()) +
            "/b" + IntToString(rand() % 100) +
            "/x" + UInt64ToString(1000 + machine()->GetGUID()));

      } else {
        BackgroundAppendStringToFile(
            RandomData(RandomBlockSize()),
            RandomFileC());
      }
    }
  }


  void CopyExperiment() {
    uint64 partitions_per_replica = config_->GetPartitionsPerReplica();
    uint64 replicas_num = config_->GetReplicas();
    vector<uint64> machines_other_replicas;      

    for (uint64 i = 0; i < partitions_per_replica * replicas_num; i++) {
      if (i/partitions_per_replica != replica_) {
        machines_other_replicas.push_back(i);
      }
    }

    uint64 size_other_machines = machines_other_replicas.size();

    Spin(1);
    metadata_->Init();
    Spin(1);
    machine()->GlobalBarrier();
    Spin(1);

    double start = GetTime();
    for (int i = 0; i < 10000; i++) {
      int seed = rand() % 100;
      
      // Copy operations inside one data center
      if (seed < 0) {
        BackgroundCopyFile("/a" + IntToString(machine()->machine_id()) + "/b" + IntToString(rand() % 1000) + "/c" + IntToString(rand() % 1000),
                           "/a" + IntToString(machine()->machine_id()) + "/b" + IntToString(rand() % 1000) + "/d" + IntToString(machine()->GetGUID()));
      } else {
      // Copy operations that cross data centers
        BackgroundCopyFile("/a" + IntToString(machine()->machine_id()) + "/b" + IntToString(rand() % 1000) + "/c" + IntToString(rand() % 1000),
                           "/a" + IntToString(machines_other_replicas[rand()%size_other_machines]) + "/b" + IntToString(rand() % 1000) + "/d" + IntToString(machine()->GetGUID()));
      }

      if (i % 1000 == 0) {
        LOG(ERROR) << "[" << machine()->machine_id() << "] "
                   << "Test progress : " << i / 1000 << "/" << 10;
      }    
    }

    // Report.
    LOG(ERROR) << "[" << machine()->machine_id() << "] "
               << "Copyed " <<  "10000 files. Elapsed time: "
               << (GetTime() - start) << " seconds";
    
  }

  void RenameExperiment() {
    uint64 partitions_per_replica = config_->GetPartitionsPerReplica();
    uint64 replicas_num = config_->GetReplicas();
    vector<uint64> machines_other_replicas;      

    for (uint64 i = 0; i < partitions_per_replica * replicas_num; i++) {
      if (i/partitions_per_replica != replica_) {
        machines_other_replicas.push_back(i);
      }
    }

    uint64 size_other_machines = machines_other_replicas.size();

    Spin(1);
    metadata_->Init();
    Spin(1);
    machine()->GlobalBarrier();
    Spin(1);

    double start = GetTime();
    for (int i = 0; i < 100; i++) {
      for (int j = 0; j < 100; j++) {
        int seed = rand() % 100;
      
        // Copy operations inside one data center
        if (seed < 0) {
          BackgroundRenameFile("/a" + IntToString(machine()->machine_id()) + "/b" + IntToString(i) + "/c" + IntToString(j),
                           "/a" + IntToString(machine()->machine_id()) + "/b" + IntToString(i+1) + "/d" + IntToString(machine()->GetGUID()));
        } else {
        // Copy operations that cross data centers
          BackgroundRenameFile("/a" + IntToString(machine()->machine_id()) + "/b" + IntToString(i) + "/c" + IntToString(j),
                           "/a" + IntToString(machines_other_replicas[rand()%size_other_machines]) + "/b" + IntToString(rand() % 1000) + "/d" + IntToString(machine()->GetGUID()));
        }      
      }

      if (i % 10 == 0) {
        LOG(ERROR) << "[" << machine()->machine_id() << "] "
                   << "Test progress : " << i / 10 << "/" << 10;
      }
    }

    // Report.
    LOG(ERROR) << "[" << machine()->machine_id() << "] "
               << "Renamed " <<  "1000 files. Elapsed time: "
               << (GetTime() - start) << " seconds";
  }

  void Report() {
    string report;
    for (auto it = latencies_.begin(); it != latencies_.end(); ++it) {
      double d;
      while (it->second->Pop(&d)) {
        report.append(it->first + " " + DoubleToString(d) + "\n");
      }
    }
    string filename =
        "/tmp/report." + UInt64ToString(machine()->machine_id());
    leveldb::Status s = leveldb::WriteStringToFile(
        leveldb::Env::Default(),
        report,
        filename);

    if (s.ok()) {
      LOG(ERROR) << "reporting latencies to " << filename;
    } else {
      LOG(ERROR) << "failed to save report: " << filename;
    }
  }

  // Caller takes ownership of returned MessageBuffers.
  // Returns serialized MetadataEntry protobuf.
  MessageBuffer* GetMetadataEntry(const Slice& path);

  // Returns client-side printable output.
  MessageBuffer* CreateFile(const Slice& path, FileType type = DATA);
  MessageBuffer* AppendStringToFile(const Slice& data, const Slice& path);
  MessageBuffer* ReadFile(const Slice& path);
  MessageBuffer* LS(const Slice& path);
  MessageBuffer* CopyFile(const Slice& from_path, const Slice& to_path);
  MessageBuffer* RenameFile(const Slice& from_path, const Slice& to_path);

  void BackgroundCreateFile(const Slice& path, FileType type = DATA) {
    Header* header = new Header();
    header->set_from(machine()->machine_id());
    header->set_to(machine()->machine_id());
    header->set_type(Header::RPC);
    header->set_app(name());
    header->set_rpc("CREATE_FILE");
    header->add_misc_bool(type == DIR);  // DIR = true, DATA = false
    header->add_misc_string(path.data(), path.size());
    if (reporting_ && rand() % 2 == 0) {
      header->set_callback_app(name());
      header->set_callback_rpc("CB");
      header->add_misc_string((type == DIR) ? "mkdir" : "touch");
      header->add_misc_double(GetTime());
    } else {
      header->set_ack_counter(reinterpret_cast<uint64>(&capacity_));
      while (capacity_.load() <= 0) {
        // Wait for some old operations to complete.
        usleep(100);
      }
      --capacity_;
    }
    machine()->SendMessage(header, new MessageBuffer());
  }

  void BackgroundAppendStringToFile(const Slice& data, const Slice& path) {
    Header* header = new Header();
    header->set_from(machine()->machine_id());
    header->set_to(machine()->machine_id());
    header->set_type(Header::RPC);
    header->set_app(name());
    header->set_rpc("APPEND");
    header->add_misc_string(path.data(), path.size());
    if (reporting_ && rand() % 2 == 0) {
      header->set_callback_app(name());
      header->set_callback_rpc("CB");
      header->add_misc_string("append");
      header->add_misc_double(GetTime());
    } else {
      header->set_ack_counter(reinterpret_cast<uint64>(&capacity_));
      while (capacity_.load() <= 0) {
        // Wait for some old operations to complete.
        usleep(100);
      }
      --capacity_;
    }
    machine()->SendMessage(header, new MessageBuffer(data));
  }

  void BackgroundReadFile(const Slice& path) {
    Header* header = new Header();
    header->set_from(machine()->machine_id());
    header->set_to(machine()->machine_id());
    header->set_type(Header::RPC);
    header->set_app(name());
    header->set_rpc("READ_FILE");
    header->add_misc_string(path.data(), path.size());
    if (reporting_ && rand() % 2 == 0) {
      header->set_callback_app(name());
      header->set_callback_rpc("CB");
      header->add_misc_string("cat");
      header->add_misc_double(GetTime());
    } else {
      header->set_ack_counter(reinterpret_cast<uint64>(&capacity_));
      while (capacity_.load() <= 0) {
        // Wait for some old operations to complete.
        usleep(100);
      }
      --capacity_;
    }
    machine()->SendMessage(header, new MessageBuffer());
  }

  void BackgroundLS(const Slice& path) {
    Header* header = new Header();
    header->set_from(machine()->machine_id());
    header->set_to(machine()->machine_id());
    header->set_type(Header::RPC);
    header->set_app(name());
    header->set_rpc("LS");
    header->add_misc_string(path.data(), path.size());
    if (reporting_ && rand() % 2 == 0) {
      header->set_callback_app(name());
      header->set_callback_rpc("CB");
      header->add_misc_string("ls");
      header->add_misc_double(GetTime());
    } else {
      header->set_ack_counter(reinterpret_cast<uint64>(&capacity_));
      while (capacity_.load() <= 0) {
        // Wait for some old operations to complete.
        usleep(100);
      }
      --capacity_;
    }
    machine()->SendMessage(header, new MessageBuffer());
  }

  void BackgroundCopyFile(const Slice& from_path, const Slice& to_path) {
    Header* header = new Header();
    header->set_from(machine()->machine_id());
    header->set_to(machine()->machine_id());
    header->set_type(Header::RPC);
    header->set_app(name());
    header->set_rpc("COPY_FILE");
    header->add_misc_string(from_path.data(), from_path.size());
    header->add_misc_string(to_path.data(), to_path.size());
    if (reporting_ && rand() % 2 == 0) {
      header->set_callback_app(name());
      header->set_callback_rpc("CB");
      header->add_misc_string("copy");
      header->add_misc_double(GetTime());
    } else {
      header->set_ack_counter(reinterpret_cast<uint64>(&capacity_));
      while (capacity_.load() <= 0) {
        // Wait for some old operations to complete.
        usleep(100);
      }
      --capacity_;
    }
    machine()->SendMessage(header, new MessageBuffer());
  }

  void BackgroundRenameFile (const Slice& from_path, const Slice& to_path) {
    Header* header = new Header();
    header->set_from(machine()->machine_id());
    header->set_to(machine()->machine_id());
    header->set_type(Header::RPC);
    header->set_app(name());
    header->set_rpc("RENAME_FILE");
    header->add_misc_string(from_path.data(), from_path.size());
    header->add_misc_string(to_path.data(), to_path.size());
    if (reporting_ && rand() % 2 == 0) {
      header->set_callback_app(name());
      header->set_callback_rpc("CB");
      header->add_misc_string("rename");
      header->add_misc_double(GetTime());
    } else {
      header->set_ack_counter(reinterpret_cast<uint64>(&capacity_));
      while (capacity_.load() <= 0) {
        // Wait for some old operations to complete.
        usleep(100);
      }
      --capacity_;
    }
    machine()->SendMessage(header, new MessageBuffer());
  }


  inline Slice RandomData(uint64 size) {
    uint64 start = rand() % (random_data_.size() - size);
    return Slice(random_data_.data() + start, size);
  }

  void set_start_time(double t) { start_time_ = t; }
  double start_time_;

  void set_experiment(int e) {experiment = e;}
  int experiment;

  atomic<int> action_count_;
  atomic<int> capacity_;

  string random_data_;

  map<string, AtomicQueue<double>*> latencies_;

  atomic<bool> go_;
  atomic<bool> going_;
  bool reporting_;

  // Configuration for this CalvinFS instance.
  CalvinFSConfigMap* config_;

  // Local replica id.
  uint64 replica_;

  // Block store.
  DistributedBlockStoreApp* blocks_;

  // BlockLogApp for appending new requests.
  BlockLogApp* log_;

  // Scheduler for getting safe version.
  Scheduler* scheduler_;

  // MetadataStore for getting RWSets.
  MetadataStore* metadata_;
};

#endif  // CALVIN_FS_CALVINFS_CLIENT_APP_H_

