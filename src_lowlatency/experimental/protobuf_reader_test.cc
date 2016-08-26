// Author: Alexander Thomson <thomson@cs.yale.edu>
//

#include "experimental/protobuf_reader.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <google/profiler.h>
#include <gtest/gtest.h>
#include <sys/time.h>

#include <vector>

#include "common/utils.h"
#include "proto/testing.pb.h"

using std::vector;

DEFINE_string(benchmark, "", "What benchmarks to run instead of unit tests.");

string GetRandomString(int size) {
  string s;
  for (int i = 0; i < size; i++) {
    s += rand() % 26 + 'A';
  }
  return s;
}

void RandomA(A* a, int xcount, int scount, int ssize) {
  a->Clear();
  for (int i = 0; i < xcount; i++) a->add_x(rand() % 1000000000);
  for (int i = 0; i < scount; i++) a->add_s(GetRandomString(ssize));
}

A RandomA(int xcount, int scount, int ssize) {
  A a;
  RandomA(&a, xcount, scount, ssize);
  return a;
}

void RandomB(B* b, int depth, int fanout) {
  b->Clear();
  if (depth <= 1) {
    for (int i = 0; i < fanout; i++) {
      RandomA(b->add_a(), 0, 32, 32);
    }
  } else {
    for (int i = 0; i < fanout; i++) {
      RandomB(b->add_b(), depth-1, fanout);
    }
  }
}

// TESTS
//
// TODO ASSERT_DEATH tests
  
TEST(ProtobufReaderTest, Empty) {
  string e;
  A().SerializeToString(&e);
  EXPECT_FALSE(ProtobufReader(e).ReadField<uint32>(1, NULL));
  EXPECT_FALSE(ProtobufReader(e).ReadField<string>(1, NULL));
  EXPECT_FALSE(ProtobufReader(e).ReadField<Slice>(1, NULL));
  EXPECT_FALSE(ProtobufReader(e).ReadField<ProtobufReader>(1, NULL));
}

TEST(ProtobufReaderTest, ReadFields) {
  string e;
  A a;
  a.add_x(1);
  a.add_x(5);
  a.add_s("1");
  a.add_s("5");
  a.SerializeToString(&e);

  EXPECT_FALSE(ProtobufReader(e).ReadField<uint32>(0, NULL));
  EXPECT_FALSE(ProtobufReader(e).ReadField<string>(0, NULL));
  EXPECT_FALSE(ProtobufReader(e).ReadField<Slice>(0, NULL));
  EXPECT_FALSE(ProtobufReader(e).ReadField<ProtobufReader>(0, NULL));
  EXPECT_FALSE(ProtobufReader(e).ReadRepeatedField<uint32>(0, 0, NULL));
  EXPECT_FALSE(ProtobufReader(e).ReadRepeatedField<string>(0, 0, NULL));
  EXPECT_FALSE(ProtobufReader(e).ReadRepeatedField<Slice>(0, 0, NULL));
  EXPECT_FALSE(ProtobufReader(e).ReadRepeatedField<ProtobufReader>(0, 0, NULL));

  uint32 x;
  EXPECT_TRUE(ProtobufReader(e).ReadField<uint32>(1, &x));
  EXPECT_TRUE(x == 1);
  EXPECT_TRUE(ProtobufReader(e).ReadRepeatedField<uint32>(1, 0, &x));
  EXPECT_TRUE(x == 1);
  EXPECT_TRUE(ProtobufReader(e).ReadRepeatedField<uint32>(1, 1, &x));
  EXPECT_TRUE(x == 5);
  EXPECT_FALSE(ProtobufReader(e).ReadRepeatedField<uint32>(1, 2, &x));

  string s;
  EXPECT_TRUE(ProtobufReader(e).ReadField<string>(2, &s));
  EXPECT_EQ("1", s);
  EXPECT_TRUE(ProtobufReader(e).ReadRepeatedField<string>(2, 0, &s));
  EXPECT_EQ("1", s);
  EXPECT_TRUE(ProtobufReader(e).ReadRepeatedField<string>(2, 1, &s));
  EXPECT_EQ("5", s);
  EXPECT_FALSE(ProtobufReader(e).ReadRepeatedField<string>(2, 2, &s));

  Slice sl;
  EXPECT_TRUE(ProtobufReader(e).ReadField<Slice>(2, &sl));
  EXPECT_EQ("1", sl);
  EXPECT_TRUE(ProtobufReader(e).ReadRepeatedField<Slice>(2, 0, &sl));
  EXPECT_EQ("1", sl);
  EXPECT_TRUE(ProtobufReader(e).ReadRepeatedField<Slice>(2, 1, &sl));
  EXPECT_EQ("5", sl);
  EXPECT_FALSE(ProtobufReader(e).ReadRepeatedField<Slice>(2, 2, &sl));
}

// BENCHMARKS_A

double Time() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec + tv.tv_usec/1e6;
}

void BenchmarkParseFromString_A(int xcount, int scount, int ssize) {
  int total = 1000000 / (1 + xcount * 4 + scount * (1+ssize));
  vector<string> v(total, string());
  for (int i = 0; i < total; i++) {
    RandomA(xcount, scount, ssize).SerializeToString(&v[i]);
  }

  double start = Time();
  for (int i = 0; i < total; i++) {
    A a;
    a.ParseFromString(v[i]);
  }
  double end = Time();
  LOG(ERROR) << "ParseFromString_A(" << xcount << " ints + "
             << scount << "x" << ssize << " bytes): "
             << (end - start) * 1000000000 / total << " ns";
}

template<typename StringType>  // string or Slice
void BenchmarkReadRepeatedField_A(int xcount, int scount, int ssize) {
  int total = 10000000 / (1 + xcount * 4 + scount * (1+ssize));
  vector<string> v(total, string());
  for (int i = 0; i < total; i++) {
    RandomA(xcount, scount, ssize).SerializeToString(&v[i]);
  }

  double start = Time();
  for (int i = 0; i < total; i++) {
    if (xcount + scount > 0) {
      // Choose a random field/entry.
      int index = rand() % (xcount + scount);
      if (index < xcount) {
        // int field
        uint32 x;
        ProtobufReader(v[i]).ReadRepeatedField<uint32>(1, index, &x);
      } else {
        // string field
        StringType s;
        ProtobufReader(v[i]).ReadRepeatedField<StringType>(2, index - xcount, &s);
      }
    }
  }
  double end = Time();
  LOG(ERROR) << "ReadRepeatedField_A<" << TypeName<StringType>()
             << ">(" << xcount << " ints + " << scount << "x" << ssize
             << " bytes): " << (end - start) * 1000000000 / total << " ns";
}

// BENCHMARKS_B

void BenchmarkParseFromString_B(int depth, int fanout) {
  B b;
  RandomB(&b, depth, fanout);
  string e;
  b.SerializeToString(&e);

  int total = 100000000 / e.size();
  vector<string> v(total, string());
  for (int i = 0; i < total; i++) {
    RandomB(&b, depth, fanout);
    b.SerializeToString(&v[i]);
  }

  double start = Time();
  for (int i = 0; i < total; i++) {
    B bb;
    bb.ParseFromString(v[i]);
  }
  double end = Time();
  LOG(ERROR) << "ReadRepeatedField_B(" << depth << ", " << fanout << "): "
             << e.size() << " bytes   -> " << (end - start) * 1000000000 / total
             << " ns";
}

template<typename StringType>  // string or Slice
void BenchmarkReadRepeatedField_B(int depth, int fanout) {
  B b;
  RandomB(&b, depth, fanout);
  string e;
  b.SerializeToString(&e);

  int total = 100000000 / e.size();
  vector<string> v(total, string());
  for (int i = 0; i < total; i++) {
    ProtobufReader pr(v[i]);
    ProtobufReader prr;
    for (int j = 0; j < depth - 1; j++) {
      pr.ReadRepeatedField<ProtobufReader>(1, rand() % fanout, &prr);
      pr = prr;
    }
    pr.ReadRepeatedField<ProtobufReader>(2, rand() % fanout, &prr);
    StringType s;
    prr.ReadRepeatedField<StringType>(2, rand() % 32, &s);
  }

  double start = Time();
  for (int i = 0; i < total; i++) {
    B bb;
    bb.ParseFromString(v[i]);
  }
  double end = Time();
  LOG(ERROR) << "ReadRepeatedField_B(" << depth << ", " << fanout << "): "
             << e.size() << " bytes   -> " << (end - start) * 1000000000 / total
             << " ns";
}

void BenchmarkProfileParseFromString(int total) {
  vector<string> v(total, string());
  for (int i = 0; i < total; i++) {
    RandomA(100, 20, 100).SerializeToString(&v[i]);
  }
  A a;
  ProfilerStart("proto_parse.prof");
  for (int i = 0; i < total; i++) {
    a.ParseFromString(v[i]);
  }
  ProfilerStop();
}

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  if (FLAGS_benchmark == "a") {
    BenchmarkParseFromString_A(0, 0, 0);
    BenchmarkParseFromString_A(1, 0, 0);
    BenchmarkParseFromString_A(10, 0, 0);
    BenchmarkParseFromString_A(100, 0, 0);
    BenchmarkParseFromString_A(0, 10, 10);
    BenchmarkParseFromString_A(0, 10, 100);
    BenchmarkParseFromString_A(0, 10, 1000);
    BenchmarkReadRepeatedField_A<Slice>(0, 0, 0);
    BenchmarkReadRepeatedField_A<Slice>(1, 0, 0);
    BenchmarkReadRepeatedField_A<Slice>(10, 0, 0);
    BenchmarkReadRepeatedField_A<Slice>(100, 0, 0);
    BenchmarkReadRepeatedField_A<Slice>(0, 10, 10);
    BenchmarkReadRepeatedField_A<Slice>(0, 10, 100);
    BenchmarkReadRepeatedField_A<Slice>(0, 10, 1000);
    BenchmarkReadRepeatedField_A<string>(0, 10, 10);
    BenchmarkReadRepeatedField_A<string>(0, 10, 100);
    BenchmarkReadRepeatedField_A<string>(0, 10, 1000);
    return 0;
  } else if (FLAGS_benchmark == "b") {
    BenchmarkParseFromString_B(1, 2);
    BenchmarkParseFromString_B(2, 2);
    BenchmarkParseFromString_B(3, 2);
    BenchmarkParseFromString_B(4, 2);
    BenchmarkParseFromString_B(5, 2);
    BenchmarkParseFromString_B(6, 2);
    BenchmarkReadRepeatedField_B<Slice>(1, 2);
    BenchmarkReadRepeatedField_B<Slice>(2, 2);
    BenchmarkReadRepeatedField_B<Slice>(3, 2);
    BenchmarkReadRepeatedField_B<Slice>(4, 2);
    BenchmarkReadRepeatedField_B<Slice>(5, 2);
    BenchmarkReadRepeatedField_B<Slice>(6, 2);
    BenchmarkParseFromString_B(1, 10);
    BenchmarkParseFromString_B(2, 10);
    BenchmarkParseFromString_B(3, 10);
    BenchmarkReadRepeatedField_B<Slice>(1, 10);
    BenchmarkReadRepeatedField_B<Slice>(2, 10);
    BenchmarkReadRepeatedField_B<Slice>(3, 10);
    return 0;
  } else if (FLAGS_benchmark == "c") {
    BenchmarkProfileParseFromString(1000000);
    return 0;
  }
  return RUN_ALL_TESTS();
}

