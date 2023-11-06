//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  this->max_depth_ = max_depth;

  global_depth_ = 0;

  for (unsigned char & local_depth : local_depths_) {
    local_depth = 0;
  }

  for (int & bucket_page_id : bucket_page_ids_) {
    bucket_page_id = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  // map the least significant bit
  return hash & GetGlobalDepthMask();
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  if (bucket_idx >= (1 << GetMaxDepth())) {
    throw ExecutionException("The index exceed the max size");
  }
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  if (bucket_idx >= (1 << GetMaxDepth())) {
    throw ExecutionException("The index exceed the max size");
  }
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t { return 0; }

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t {
  return global_depth_;
}

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  if (global_depth_ == GetMaxDepth()) {
    throw ExecutionException("Global depth greater than max depth");
  }
  global_depth_++;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  if (global_depth_ == 0) {
    throw ExecutionException("Global depth less than 0");
  }
  global_depth_--;
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  for (unsigned char & local_depth : local_depths_) {
    if (local_depth == global_depth_) {
      return false;
    }
  }
  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t {
  return 1 << global_depth_;
}

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  if (local_depth > GetMaxDepth()) {
    throw ExecutionException("Local depth greater than max depth");
  }
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  if (local_depths_[bucket_idx] >= GetMaxDepth()) {
    throw ExecutionException("Local depth greater than max depth");
  }
  local_depths_[bucket_idx]++;
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  if (local_depths_[bucket_idx] == 0) {
    throw ExecutionException("Local depth Less than 0");
  }
  local_depths_[bucket_idx]--;
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t {
  return ((1 << global_depth_) - 1);
}

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  return ((1 << local_depths_[bucket_idx]) - 1);
}

auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t {
  return max_depth_;
}

}  // namespace bustub
