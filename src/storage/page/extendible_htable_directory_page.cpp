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

  for (int i = 0; i < (1 << max_depth); i++) {
    local_depths_[i] = 0;
    bucket_page_ids_[i] = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  // map the least significant bit
  return hash & GetGlobalDepthMask();
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  if (bucket_idx >= (1 << GetMaxDepth())) {
    throw ExecutionException("Directory: The index exceed the max size");
  }
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  if (bucket_idx >= (1 << GetMaxDepth())) {
    throw ExecutionException("Directory: The index exceed the max size");
  }
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  if (global_depth_ == GetMaxDepth()) {
    throw ExecutionException("Directory: Global depth greater than max depth");
  }

  // duplicate
  auto tmp = (1 << global_depth_);
  for (int i = tmp; i < 2 * tmp; i++) {
    local_depths_[i] = local_depths_[i - tmp];
    bucket_page_ids_[i] = bucket_page_ids_[i - tmp];
  }
  global_depth_++;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  if (global_depth_ == 0) {
    throw ExecutionException("Directory: Global depth less than 0");
  }

  // re-init
  auto tmp = (1 << global_depth_);
  for (int i = tmp / 2; i < tmp; i++) {
    local_depths_[i] = 0;
    bucket_page_ids_[i] = INVALID_PAGE_ID;
  }
  global_depth_--;
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  return std::all_of(local_depths_, local_depths_ + (1 << global_depth_),
                     [this](unsigned char local_depth) { return local_depth != global_depth_; });
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return 1 << global_depth_; }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  if (local_depth > GetMaxDepth()) {
    throw ExecutionException("Directory: Local depth greater than max depth");
  }
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  if (local_depths_[bucket_idx] >= GetMaxDepth()) {
    throw ExecutionException("Directory: Local depth greater than max depth");
  }
  local_depths_[bucket_idx]++;
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  if (local_depths_[bucket_idx] == 0) {
    throw ExecutionException("Directory: Local depth Less than 0");
  }
  local_depths_[bucket_idx]--;
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t { return ((1 << global_depth_) - 1); }

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  return ((1 << local_depths_[bucket_idx]) - 1);
}

auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t { return max_depth_; }

}  // namespace bustub
