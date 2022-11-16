#ifndef STORAGE_LEVELDB_UNIT_HANDLE_BLOCK_H_
#define STORAGE_LEVELDB_UNIT_HANDLE_BLOCK_H_

#include <string>
#include <vector>

#include "table/format.h"

#include "include/leveldb/env.h"
#include "include/leveldb/filter_policy.h"
#include "include/leveldb/options.h"
#include "include/leveldb/slice.h"
#include "include/leveldb/status.h"

namespace leveldb {
class UnitHandleBlockBuilder {
 public:
  explicit UnitHandleBlockBuilder(const FilterPolicy* policy,
                                  WritableFile* file, uint64_t* offset,
                                  uint64_t expiredTime,
                                  uint64_t access_frequencey,
                                  int filter_unit_number);

  std::string Finish(const std::vector<Slice>& keys);

  void Close();

 private:
  const FilterPolicy* policy_;
  WritableFile* file_;
  uint64_t* offset_;

  uint64_t expiredTime_;
  uint64_t access_frequencey_;
  int filter_unit_number_;

  std::string result_;
  std::vector<uint32_t> handle_offset_;

  std::string block_handle_string_;
  BlockHandle blockHandle_;

  std::string bitmap_;

  void WriteBitmap(const std::vector<Slice>& keys);
  Status WriteRawBlock(const Slice& block_contents, CompressionType type,
                       BlockHandle* handle);
};

class UnitHandleBlockReader {
 public:
  /*
   * Contents:
   * [unit_handler][unit_handler][unit_handler_offset][unit_handler_offset][array_offset][expiredTime][access_frequnecy]
   */
  explicit UnitHandleBlockReader(const FilterPolicy* policy,
                                 RandomAccessFile* file, ReadOptions options,
                                 Slice contents, int loaded_number);

  Status LoadUnits(int number);
  void EvictUnits(int number);
  void UpdateCurrentTime(uint64_t current_time);
  bool IsCold(uint64_t current_time) const;
  bool KeyMayMatch(const Slice& key);
  int ReturnLoadedUnitsNumber() const;
  int ReturnUnitsNumber() const;

 private:
  const FilterPolicy* policy_;
  RandomAccessFile* file_;
  const ReadOptions options_;

  const char *data_;
  uint32_t array_offset_;


  std::vector<std::string> bitmaps_;
  uint64_t expiredTime_;

  uint64_t access_frequnecy_;
  int filter_unit_loaded_number_;
  int filter_unit_number_;

  BlockContents blockContents_;
  BlockHandle blockHandle_;

  BlockHandle ReturnBlockHandle(int index);
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UNIT_HANDLE_BLOCK_H_