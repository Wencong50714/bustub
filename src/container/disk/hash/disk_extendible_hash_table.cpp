//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  // Empty Table: Only have header page
  auto header_guard = bpm_->NewPageGuarded(&header_page_id_);
  auto header_page = header_guard.template AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth_);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  uint32_t hash = Hash(key);

  auto header_page = bpm_->FetchPageBasic(header_page_id_).template AsMut<ExtendibleHTableHeaderPage>();
  auto dir_idx = header_page->HashToDirectoryIndex(hash);
  auto dir_page_id = static_cast<page_id_t>(header_page->GetDirectoryPageId(dir_idx));

  // If the page have not been allocated, create a new page and place back
  if (dir_page_id == INVALID_PAGE_ID) {
    auto dir_guard = bpm_->NewPageGuarded(&dir_page_id);
    auto dir_page = dir_guard.template AsMut<ExtendibleHTableDirectoryPage>();
    dir_page->Init(directory_max_depth_);

    header_page->SetDirectoryPageId(dir_idx, dir_page_id);
  }

  auto dir_page = bpm_->FetchPageBasic(dir_page_id).template AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_idx = dir_page->HashToBucketIndex(hash);
  auto bucket_page_id = static_cast<page_id_t>(dir_page->GetBucketPageId(bucket_idx));

  // If the page have not been allocated, create a new page and place back
  if (bucket_page_id == INVALID_PAGE_ID) {
    auto bucket_guard = bpm_->NewPageGuarded(&bucket_page_id);
    auto bucket_page = bucket_guard.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    bucket_page->Init(bucket_max_size_);

    dir_page->SetBucketPageId(bucket_idx, bucket_page_id);
  }

  auto bucket_page = bpm_->FetchPageBasic(bucket_page_id).template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  V value;
  if (bucket_page->Lookup(key, value, cmp_)) {
    result->emplace_back(value);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  uint32_t hash = Hash(key);

  auto header_page = bpm_->FetchPageBasic(header_page_id_).template AsMut<ExtendibleHTableHeaderPage>();
  auto dir_idx = header_page->HashToDirectoryIndex(hash);
  auto dir_page_id = static_cast<page_id_t>(header_page->GetDirectoryPageId(dir_idx));

  // If the page have not been allocated, create a new page and place back
  if (dir_page_id == INVALID_PAGE_ID) {
    auto dir_guard = bpm_->NewPageGuarded(&dir_page_id);
    auto dir_page = dir_guard.template AsMut<ExtendibleHTableDirectoryPage>();
    dir_page->Init(directory_max_depth_);

    header_page->SetDirectoryPageId(dir_idx, dir_page_id);
  }

  auto dir_page = bpm_->FetchPageBasic(dir_page_id).template AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_idx = dir_page->HashToBucketIndex(hash);
  auto bucket_page_id = static_cast<page_id_t>(dir_page->GetBucketPageId(bucket_idx));

  // If the page have not been allocated, create a new page and place back
  if (bucket_page_id == INVALID_PAGE_ID) {
    auto bucket_guard = bpm_->NewPageGuarded(&bucket_page_id);
    auto bucket_page = bucket_guard.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    bucket_page->Init(bucket_max_size_);

    dir_page->SetBucketPageId(bucket_idx, bucket_page_id);
  }

  auto bucket_page = bpm_->FetchPageBasic(bucket_page_id).template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  if (bucket_page->IsFull()) {
    if (dir_page->GetLocalDepth(bucket_idx) == directory_max_depth_) {
      return false; // can't split
    }

    // Prepare the rehashing items
    std::vector<std::pair<K, V>> rehash_items = {};
    rehash_items.emplace_back(std::make_pair(key, value));  // add current item
    for (uint32_t i = 0; i < bucket_page->Size(); i++) {
      rehash_items.emplace_back(bucket_page->EntryAt(i));
    }

    // Maintain the invariant of directory
    if (dir_page->GetLocalDepth(bucket_idx) == dir_page->GetGlobalDepth()) {
      dir_page->IncrGlobalDepth();  // dictionary expansion
    }

    // create a new page, and bind it
    page_id_t new_page_id = INVALID_PAGE_ID;
    auto new_bucket_guard = bpm_->NewPageGuarded(&new_page_id);
    auto new_bucket_page = new_bucket_guard.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    new_bucket_page->Init(bucket_max_size_);

    // split
    auto local_mask = dir_page->GetLocalDepthMask(bucket_idx);
    auto suffix = bucket_idx & local_mask;
    auto step = (1 << dir_page->GetLocalDepth(bucket_idx));
    bool odd = true;
    for (uint32_t i = suffix; i < (1 << dir_page->GetGlobalDepth()); i += step) {
      if (odd) {
        dir_page->SetBucketPageId(i, new_page_id);
      }
      dir_page->IncrLocalDepth(i);
      odd = !odd;
    }

    // rehash these items
    for (auto &it : rehash_items) {
      auto rehash = Hash(it.first);
      auto rehash_page_idx = static_cast<page_id_t>(dir_page->GetBucketPageId(dir_page->HashToBucketIndex(rehash)));

      if (rehash_page_idx == new_page_id) {
        // remove from old page, and add it to new
        bucket_page->Remove(it.first, cmp_);
        new_bucket_page->Insert(it.first, it.second, cmp_);
      } // Else, do nothing
    }
  } else {
    // bucket page have free space
    bucket_page->Insert(key, value, cmp_);
  }

  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  return false;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  return false;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                       ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
                                                       uint32_t new_bucket_idx, uint32_t local_depth_mask) {}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  return false;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
