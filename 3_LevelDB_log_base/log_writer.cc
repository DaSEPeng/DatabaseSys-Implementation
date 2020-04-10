// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <stdint.h>

#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

static void InitTypeCrc(uint32_t* type_crc) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc[i] = crc32c::Value(&t, 1);
  }
}

Writer::Writer(WritableFile* dest) : dest_(dest), block_offset_(0) {
  InitTypeCrc(type_crc_);
}

Writer::Writer(WritableFile* dest, uint64_t dest_length)
    : dest_(dest), block_offset_(dest_length % kBlockSize) {
  InitTypeCrc(type_crc_);
}

Writer::~Writer() = default;

// 添加一条记录,leveldb将key和value封装成了一个Slice
Status Writer::AddRecord(const Slice& slice) {
  const char* ptr = slice.data();     // key和value的值
  // seq number 8个字节  快表中的序号 
  // entry number 标识一个batch里的记录的个数

  size_t left = slice.size();         // seq number+entry number+type+keysize+key+valuesize+value

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  Status s;
  bool begin = true;  // 标识是不是一个slice的开头
  do {
    const int leftover = kBlockSize - block_offset_;   // kBlockSize=32KB，是一个固定的值  block_offset_是已经占有的空间
    assert(leftover >= 0);
    if (leftover < kHeaderSize) {  // kHeaderSize=7byte
      // Switch to a new block
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        static_assert(kHeaderSize == 7, "");
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover)); // 实际的指针已经增加了leftover这么大的长度
      }
      block_offset_ = 0; // 换一个新的block
    }

    // Invariant: we never leave < kHeaderSize bytes in a block.
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

    const size_t avail = kBlockSize - block_offset_ - kHeaderSize; // 实际能写数据的大小
    const size_t fragment_length = (left < avail) ? left : avail;  // 如果剩下要写的不够剩下的空间，就不占用剩下的空间

    RecordType type;  // 0是保留字节，标识是否损坏
    const bool end = (left == fragment_length);
    if (begin && end) {
      type = kFullType;   // 1
    } else if (begin) {
      type = kFirstType;  // 2
    } else if (end) {
      type = kLastType;   // 3
    } else {
      type = kMiddleType; // 4
    }

    s = EmitPhysicalRecord(type, ptr, fragment_length); // 真正写文件的行为
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);  // 每写一部分，就会在left减去一部分的长度
  return s;
}

Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr,
                                  size_t length) {
  assert(length <= 0xffff);  // Must fit in two bytes     长度肯定小于2个字节的，因为总共就是32KB
  assert(block_offset_ + kHeaderSize + length <= kBlockSize);

  // Format the header
  char buf[kHeaderSize];                      // 头部 7个字节
  buf[4] = static_cast<char>(length & 0xff);  // 长度的低位
  buf[5] = static_cast<char>(length >> 8);    // 长度的高位
  buf[6] = static_cast<char>(t);              // 类型1个字节

  // Compute the crc of the record type and the payload.
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, length);
  crc = crc32c::Mask(crc);  // Adjust for storage  高位和低位颠倒一下
  EncodeFixed32(buf, crc);  // 将结果存到buffer里

  // Write the header and the payload
  // 写入header和数据
  Status s = dest_->Append(Slice(buf, kHeaderSize));
  if (s.ok()) {
    s = dest_->Append(Slice(ptr, length));
    if (s.ok()) {
      s = dest_->Flush(); // 刷新缓冲区
    }
  }
  block_offset_ += kHeaderSize + length; // 多占用这些位
  return s;
}

}  // namespace log
}  // namespace leveldb
