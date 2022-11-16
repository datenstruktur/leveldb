// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <utility>

#include "table/filter_block.h"

#include "gtest/gtest.h"
#include "leveldb/filter_policy.h"
#include "util/coding.h"
#include "util/hash.h"
#include "util/logging.h"

namespace leveldb {

class StringSink : public WritableFile {
 public:
  ~StringSink() override = default;

  const std::string& contents() const { return contents_; }

  Status Close() override { return Status::OK(); }
  Status Flush() override { return Status::OK(); }
  Status Sync() override { return Status::OK(); }

  Status Append(const Slice& data) override {
    contents_.append(data.data(), data.size());
    return Status::OK();
  }

 private:
  std::string contents_;
};

class StringSource : public RandomAccessFile {
 public:
  StringSource(const Slice& contents)
      : contents_(contents.data(), contents.size()) {}

  ~StringSource() override = default;

  uint64_t Size() const { return contents_.size(); }

  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    if (offset >= contents_.size()) {
      return Status::InvalidArgument("invalid Read offset");
    }
    if (offset + n > contents_.size()) {
      n = contents_.size() - offset;
    }
    std::memcpy(scratch, &contents_[offset], n);
    *result = Slice(scratch, n);
    return Status::OK();
  }

 private:
  std::string contents_;
};

// For testing: emit an array with one hash value per key
class TestHashFilter : public FilterPolicy {
 public:
  const char* Name() const override { return "TestHashFilter"; }

  void CreateFilter(const Slice* keys, int n, std::string* dst, int index) const override {
    for (int i = 0; i < n; i++) {
      uint32_t h = Hash(keys[i].data(), keys[i].size(), 1);
      PutFixed32(dst, h);
    }
  }

  bool KeyMayMatch(const Slice& key, const Slice& filter, int index) const override {
    uint32_t h = Hash(key.data(), key.size(), 1);
    for (size_t i = 0; i + 4 <= filter.size(); i += 4) {
      if (h == DecodeFixed32(filter.data() + i)) {
        return true;
      }
    }
    return false;
  }
};

class FilterBlockTest : public testing::Test {
 public:
  FilterBlockTest():policy_(NewBloomFilterPolicy(10)) {}
  //TestHashFilter policy_;
  const FilterPolicy *policy_;
};

TEST_F(FilterBlockTest, EmptyBuilder) {
  ReadOptions options;
  uint64_t offset = 0;
  StringSink sink;

  FilterBlockBuilder *builder = new FilterBlockBuilder(policy_, &sink, &offset);
  Slice block = builder->Finish();
  ASSERT_EQ("\\x00\\x00\\x00\\x00\\x0b", EscapeString(block));

  StringSource source(sink.contents());

  FilterBlockReader *reader = new FilterBlockReader(policy_, builder->Finish(), &source, options);
  ASSERT_TRUE(reader->KeyMayMatch(0, "foo"));
  ASSERT_TRUE(reader->KeyMayMatch(100000, "foo"));

}

TEST_F(FilterBlockTest, SingleChunk) {
  uint64_t offset = 0;

  ReadOptions options;
  StringSink sink;
  FilterBlockBuilder *builder = new FilterBlockBuilder(policy_, &sink, &offset);

  builder->StartBlock(100);
  builder->AddKey("foo");
  builder->AddKey("bar");
  builder->AddKey("box");
  builder->StartBlock(200);
  builder->AddKey("box");
  builder->StartBlock(300);
  builder->AddKey("hello");

  Slice block = builder->Finish();

  StringSource source(sink.contents());

  FilterBlockReader *reader = new FilterBlockReader(policy_, block, &source, options);

  ASSERT_TRUE(reader->KeyMayMatch(100, "foo"));
  ASSERT_TRUE(reader->KeyMayMatch(100, "bar"));
  ASSERT_TRUE(reader->KeyMayMatch(100, "box"));
  ASSERT_TRUE(reader->KeyMayMatch(100, "hello"));
  ASSERT_TRUE(reader->KeyMayMatch(100, "foo"));
  ASSERT_TRUE(!reader->KeyMayMatch(100, "missing"));
  ASSERT_TRUE(!reader->KeyMayMatch(100, "other"));
}

TEST_F(FilterBlockTest, MultiChunk) {
  uint64_t offset = 0;

  ReadOptions options;
  StringSink sink;
  FilterBlockBuilder *builder = new FilterBlockBuilder(policy_, &sink, &offset);

  // First filter
  builder->StartBlock(0);
  builder->AddKey("foo");
  builder->StartBlock(2000);
  builder->AddKey("bar");

  // Second filter
  builder->StartBlock(3100);
  builder->AddKey("box");

  // Third filter is empty

  // Last filter
  builder->StartBlock(9000);
  builder->AddKey("box");
  builder->AddKey("hello");

  Slice block = builder->Finish();
  StringSource source(sink.contents());
  FilterBlockReader *reader = new FilterBlockReader(policy_, block, &source, options);

  // Check first filter
  ASSERT_TRUE(reader->KeyMayMatch(0, "foo"));
  ASSERT_TRUE(reader->KeyMayMatch(2000, "bar"));
  ASSERT_TRUE(!reader->KeyMayMatch(0, "box"));
  ASSERT_TRUE(!reader->KeyMayMatch(0, "hello"));

  // Check second filter
  ASSERT_TRUE(reader->KeyMayMatch(3100, "box"));
  ASSERT_TRUE(!reader->KeyMayMatch(3100, "foo"));
  ASSERT_TRUE(!reader->KeyMayMatch(3100, "bar"));
  ASSERT_TRUE(!reader->KeyMayMatch(3100, "hello"));

  // Check third filter (empty)
  ASSERT_TRUE(!reader->KeyMayMatch(4100, "foo"));
  ASSERT_TRUE(!reader->KeyMayMatch(4100, "bar"));
  ASSERT_TRUE(!reader->KeyMayMatch(4100, "box"));
  ASSERT_TRUE(!reader->KeyMayMatch(4100, "hello"));

  // Check last filter
  ASSERT_TRUE(reader->KeyMayMatch(9000, "box"));
  ASSERT_TRUE(reader->KeyMayMatch(9000, "hello"));
  ASSERT_TRUE(!reader->KeyMayMatch(9000, "foo"));
  ASSERT_TRUE(!reader->KeyMayMatch(9000, "bar"));
}

}  // namespace leveldb