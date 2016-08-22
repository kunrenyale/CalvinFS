// Author: Alexander Thomson <thomson@cs.yale.edu>
//

#ifndef CALVIN_FS_HDFS_H_
#define CALVIN_FS_HDFS_H_

#include "common/types.h"
#include "fs/fs.h"

// FileSystem type providing access to an HDFS deployment.
class HadoopFS : public FS {
 public:
  // TODO(dan,sirui): Add a constructor.

  virtual ~HadoopFS() {}
  virtual Status ReadFileToString(const string& path, string* data);
  virtual Status CreateDirectory(const string& path);
  virtual Status CreateFile(const string& path);
  virtual Status WriteStringToFile(const string& data, const string& path);
  virtual Status AppendStringToFile(const string& data, const string& path);
  virtual Status LS(const string& path, vector<string>* contents);
  virtual Status Remove(const string& path);
  virtual Status Copy(const string& from_path, const string& to_path);

 private:
  // TODO(dan,sirui): Add whatever HDFS connection info is needed to connect
  //                  and access the HDFS deployment here as class variables.
};

#endif  // CALVIN_FS_HDFS_H_

