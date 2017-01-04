// Author: Alexander Thomson <thomson@cs.yale.edu>
//
// TODO(agt,kun,danqing,sirui):
//    Extend test coverage---there are a LOT of behaviors not yet tested! In
//    addition to the obvious stuff, we should add multi-threaded test cases
//    (it's probably easiest to do so using pthreads directly rather than
//    integrating this with machine/thread_pool here).
//

#include "fs/fs.h"
#include "fs/localfs.h"
#include "fs/hdfs.h"
#include "fs/calvinfs.h"

#include <algorithm>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "fs/status.h"

class FSTest {
 public:
  // Takes ownership of fs.
  explicit FSTest(FS* fs) : fs_(fs) {}
  ~FSTest() {
    delete fs_;
  }

  void NewFSsAreEmpty() {
    vector<string> contents;
    Status s = fs_->LS("", &contents);
    EXPECT_TRUE(s.ok()) << s.ToString();
    EXPECT_TRUE(contents.empty());
  }

  void CreateNonEmptyFile() {
    // Write string to file: /foo
    EXPECT_TRUE(fs_->WriteStringToFile("Hello, world!", "/foo").ok());

    // Check that it appeared and contains the string.
    vector<string> contents;
    string s;
    EXPECT_TRUE(fs_->LS("", &contents).ok());
    EXPECT_TRUE(contents.size() == 1);
    EXPECT_TRUE(contents[0] == "foo");
    EXPECT_TRUE(fs_->ReadFileToString("/foo", &s).ok());
    EXPECT_EQ("Hello, world!", s);
  }

  void CreateDirsAndFiles() {
    // Create dir: /foo
    EXPECT_TRUE(fs_->CreateDirectory("/foo").ok());
    // Create dir: /baz
    EXPECT_TRUE(fs_->CreateDirectory("/baz").ok());

    // Check that /foo and /baz are there.
    vector<string> contents;
    EXPECT_TRUE(fs_->LS("", &contents).ok());
    EXPECT_TRUE(contents.size() == 2);
    sort(contents.begin(), contents.end());
    EXPECT_TRUE(contents[0] == "baz");
    EXPECT_TRUE(contents[1] == "foo");

    // Check that /foo and /bar are empty.
    EXPECT_TRUE(fs_->LS("/foo", &contents).ok());
    EXPECT_TRUE(contents.empty());
    EXPECT_TRUE(fs_->LS("/baz", &contents).ok());
    EXPECT_TRUE(contents.empty());

    // Create file: /spam
    EXPECT_TRUE(fs_->CreateFile("/spam").ok());

    // Check that it appeared and is empty.
    EXPECT_TRUE(fs_->LS("", &contents).ok());
    EXPECT_TRUE(contents.size() == 3);
    sort(contents.begin(), contents.end());
    EXPECT_TRUE(contents[0] == "baz");
    EXPECT_TRUE(contents[1] == "foo");
    EXPECT_TRUE(contents[2] == "spam");
    string s;
    EXPECT_TRUE(fs_->ReadFileToString("/spam", &s).ok());
    EXPECT_EQ("", s);

    // Create and append to file file: /foo/bar
    EXPECT_TRUE(fs_->CreateFile("/foo/bar").ok());
    Status st = fs_->AppendStringToFile("Hello, world!", "/foo/bar");
    EXPECT_TRUE(st.ok()) << st.ToString();

    // Check that it appeared and contains the string.
    EXPECT_TRUE(fs_->LS("/foo", &contents).ok());
    EXPECT_TRUE(contents.size() == 1);
    EXPECT_TRUE(contents[0] == "bar");
    EXPECT_TRUE(fs_->ReadFileToString("/foo/bar", &s).ok());
    EXPECT_EQ("Hello, world!", s);
  }

  void BadCommands() {
    // Create dir: /foo/bar (/foo doesn't exist)
    EXPECT_FALSE(fs_->CreateDirectory("/foo/bar").ok());

    // Create file: /foo/bar (/foo doesn't exist)
    EXPECT_FALSE(fs_->CreateFile("/foo/bar").ok());

    // Write to file: /foo/bar (/foo doesn't exist)
    EXPECT_FALSE(fs_->WriteStringToFile("asdf", "/foo/bar").ok());

    // LS nonexistant dir
    vector<string> contents;
    EXPECT_FALSE(fs_->LS("/foo", &contents).ok());

    // Read nonexistant file
    string s;
    EXPECT_FALSE(fs_->ReadFileToString("/foo", &s).ok());

    // Append to nonexistant file
    EXPECT_FALSE(fs_->AppendStringToFile("asdf", "/foo").ok());

    // TODO(agt): also check error messages
    // TODO(agt): test more bad commands
  }

 private:
  FS* fs_;
};

#define FS_TEST(FS_TYPE,NEW_FS,TEST_METHOD) \
TEST(FS_TYPE##Test, TEST_METHOD) { \
  FSTest(reinterpret_cast<FS*>(NEW_FS)).TEST_METHOD(); \
}

// Instantiate tests.
FS_TEST(LocalFS, new LocalFS(), NewFSsAreEmpty)
FS_TEST(LocalFS, new LocalFS(), CreateNonEmptyFile)
FS_TEST(LocalFS, new LocalFS(), CreateDirsAndFiles)
FS_TEST(LocalFS, new LocalFS(), BadCommands)

FS_TEST(LocalCalvinFS, new LocalCalvinFS(), NewFSsAreEmpty)
FS_TEST(LocalCalvinFS, new LocalCalvinFS(), CreateDirsAndFiles)
FS_TEST(LocalCalvinFS, new LocalCalvinFS(), BadCommands)

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

