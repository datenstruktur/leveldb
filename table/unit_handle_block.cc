#include "unit_handle_block.h"

#include <iostream>
#include "leveldb/options.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
UnitHandleBlockBuilder::UnitHandleBlockBuilder(
    const FilterPolicy* policy, WritableFile* file, uint64_t* offset,
    uint64_t expiredTime, uint64_t access_frequnecy, int filter_unit_number)
    : policy_(policy),
      file_(file),
      offset_(offset),  // todo: WritableFilter offset封装一下
      expiredTime_(expiredTime),
      access_frequencey_(access_frequnecy),
      filter_unit_number_(filter_unit_number) {}

void UnitHandleBlockBuilder::WriteBitmap(const std::vector<Slice>& keys) {
  const size_t num_keys = keys.size();
  if (num_keys == 0) return;

  // 生成filter_unit_number个filter unit
  for (int i = 0; i < filter_unit_number_; i++) {
    // 生成bitmap
    policy_->CreateFilter(&keys[0], static_cast<int>(num_keys), &bitmap_, i);
    Slice bitmap(bitmap_.data(), bitmap_.size());
    // 写入磁盘，返回block handle
    WriteRawBlock(bitmap, kNoCompression, &blockHandle_);

    // handle的offset
    handle_offset_.push_back(result_.size());

    // handle转换为string
    blockHandle_.EncodeTo(&block_handle_string_);
    // 增加到字符串里
    result_.append(block_handle_string_);

    block_handle_string_.clear();
    bitmap_.clear();
  }

  file_->Sync();
}

std::string UnitHandleBlockBuilder::Finish(const std::vector<Slice>& keys) {
  // 生成block handle字符串和handle offset数组
  WriteBitmap(keys);

  // 记录数组的开始offset
  uint32_t array_offset = result_.size();

  for (int i = 0; i < handle_offset_.size(); i++) {
    // 追加入数组元素
    PutFixed32(&result_, handle_offset_[i]);
  }

  // 追加入数组开始位置
  PutFixed32(&result_, array_offset);

  // 追加入热度信息
  PutFixed64(&result_, expiredTime_);
  PutFixed64(&result_, access_frequencey_);

  return result_;
}

Status UnitHandleBlockBuilder::WriteRawBlock(const Slice& block_contents,
                                             CompressionType type,
                                             BlockHandle* handle) {
  Status s;
  handle->set_offset(*offset_);
  handle->set_size(block_contents.size());
  s = file_->Append(block_contents);
  if (s.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    s = file_->Append(Slice(trailer, kBlockTrailerSize));
    if (s.ok()) {
      *offset_ += block_contents.size() + kBlockTrailerSize;
    }
  }

  return s;
}

void UnitHandleBlockBuilder::Close() { file_->Close(); }

UnitHandleBlockReader::UnitHandleBlockReader(const FilterPolicy* policy,
                                             RandomAccessFile* file,
                                             ReadOptions options,
                                             Slice contents, int loaded_number)
    : policy_(policy),
      file_(file),
      options_(options),
      filter_unit_loaded_number_(0) {

  data_ = contents.data();
  size_t n = contents.size();

  access_frequnecy_ = DecodeFixed64(data_ + n - 8);
  expiredTime_ = DecodeFixed64(data_ + n - 8 - 8);

  array_offset_ = DecodeFixed32(data_ + n - 8 - 8 - 4);
  filter_unit_number_ = (int)((n - 8 - 8 - 4 - array_offset_) / 4);

  LoadUnits(loaded_number);
}

BlockHandle UnitHandleBlockReader::ReturnBlockHandle(int index){
  uint32_t start, end;
  BlockHandle blockHandle;
  Slice handleContent;

  start = DecodeFixed32(data_ + array_offset_ + index * 4);
  end   = DecodeFixed32(data_ + array_offset_ + index * 4 + 4);

  handleContent = Slice(data_ + start, end - start);

  blockHandle.DecodeFrom(&handleContent);

  return blockHandle;
}

Status UnitHandleBlockReader::LoadUnits(int number) {
  if (filter_unit_loaded_number_ + number > filter_unit_number_)
    return Status::Corruption("load too many units");

  for (int i = 0; i < number; i++) {
    blockHandle_ = ReturnBlockHandle(i);
    Status s = ReadBlock(file_, options_, blockHandle_, &blockContents_);
    if (s.ok()) {
      bitmaps_.push_back(blockContents_.data.ToString());
      filter_unit_loaded_number_++;
    } else {
      return s;
    }

  }
  return Status::OK();
}

void UnitHandleBlockReader::EvictUnits(int number) {
  if (bitmaps_.size() > number) {
    for (int i = 0; i < number; i++) {
      bitmaps_.pop_back();
      filter_unit_loaded_number_--;
    }
  } else {
    bitmaps_.clear();
    filter_unit_loaded_number_ = 0;
  }
}

void UnitHandleBlockReader::UpdateCurrentTime(uint64_t current_time) {
  expiredTime_ = current_time + lifeTime;
}

bool UnitHandleBlockReader::IsCold(uint64_t current_time) const {
  // 冷的话，是现在早就过了退休的时间
  // 1001 >= 1000
  return current_time >= expiredTime_;
}

bool UnitHandleBlockReader::KeyMayMatch(const Slice& key) {
  access_frequnecy_ ++;
  if(bitmaps_.empty()) return false;
  for (int i = 0; i < bitmaps_.size(); i++) {
    if (!policy_->KeyMayMatch(key, bitmaps_[i], i)) {
      return false;
    }
  }

  return true;
}

int UnitHandleBlockReader::ReturnLoadedUnitsNumber() const {
  return filter_unit_loaded_number_;
}

int UnitHandleBlockReader::ReturnUnitsNumber() const { return filter_unit_number_; }
}  // namespace leveldb