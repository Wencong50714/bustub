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

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t { return 0; }

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  if (bucket_idx >= (1 << max_depth_)) {
    throw ExecutionException("The index exceed the max size");
  }
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  if (bucket_idx >= (1 << max_depth_)) {
    throw ExecutionException("The index exceed the max size");
  }
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t { return 0; }

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  if (global_depth_ == max_depth_) {
    throw ExecutionException("Global depth greater than max depth");
  }
  global_depth_++;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  if (global_depth_ == 0) {
    throw ExecutionException("Global depth less than 1");
  }
  global_depth_++;
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool { return false; }

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return 0; }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {return local_depths_[bucket_idx / 4];}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
}

}  // namespace bustub
