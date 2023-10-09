//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size),
      disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)),
      log_manager_(log_manager),
      disk_manager_(disk_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

// Update the page table, page frame metadata and access it
auto BufferPoolManager::AssignPage(frame_id_t fid, page_id_t pid) {
  page_table_.insert({pid, fid});

  pages_[fid].page_id_ = pid;
  pages_[fid].pin_count_ = 1;
  pages_[fid].is_dirty_ = false;

  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);
  return &pages_[fid];
}

void RemoveItemFromPageTable(std::unordered_map<frame_id_t, page_id_t> &m, frame_id_t fid) {
  for (auto it = m.begin(); it != m.end(); ++it) {
    if (it->second == fid) {
      m.erase(it);
      return;
    }
  }
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  frame_id_t fid;
  // First, get page from free list
  if (!free_list_.empty()) {
    fid = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&fid)) {
      return nullptr;
    }
    // If page is dirty, write back to the disk
    if (pages_[fid].IsDirty()) {
      auto promise = disk_scheduler_->CreatePromise();
      auto f = promise.get_future();
      disk_scheduler_->Schedule({true, pages_[fid].GetData(), pages_[fid].GetPageId(), std::move(promise)});
      f.get();
    }
    RemoveItemFromPageTable(page_table_, fid);
  }
  // get new page_id
  page_id_t pid = AllocatePage();
  *page_id = pid;
  pages_[fid].ResetMemory();
  return AssignPage(fid, pid);
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  // First search for page_id in the buffer pool.
  frame_id_t fid;
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    fid = it->second;
    pages_[fid].pin_count_ += 1;
    replacer_->RecordAccess(fid);
    replacer_->SetEvictable(fid, false);
    return &pages_[fid];
  }
  if (!free_list_.empty()) {
    fid = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&fid)) {
      return nullptr;
    }
    if (pages_[fid].IsDirty()) {
      auto promise = disk_scheduler_->CreatePromise();
      auto f = promise.get_future();
      disk_scheduler_->Schedule({true, pages_[fid].GetData(), pages_[fid].GetPageId(), std::move(promise)});
      f.get();
    }
    RemoveItemFromPageTable(page_table_, fid);
  }
  // read the page from disk by scheduling a read DiskRequest with disk_scheduler_->Schedule()
  auto promise = disk_scheduler_->CreatePromise();
  auto f = promise.get_future();
  disk_scheduler_->Schedule({false, pages_[fid].GetData(), page_id, std::move(promise)});
  f.get();
  return AssignPage(fid, page_id);
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }

  auto fid = it->second;
  if (pages_[fid].pin_count_ == 0) {
    return false;
  }
  pages_[fid].pin_count_--;
  if (pages_[fid].pin_count_ == 0) {
    replacer_->SetEvictable(fid, true);
    if (!pages_[fid].is_dirty_) {
      pages_[fid].is_dirty_ = is_dirty;
    }
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    disk_manager_->WritePage(page_id, pages_[it->second].GetData());
    pages_[it->second].is_dirty_ = false;
    return true;
  }
  return false;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lock(latch_);
  for (frame_id_t i = 0; i < static_cast<int>(pool_size_); i++) {
    auto pid = pages_[i].GetPageId();
    if (pid != INVALID_PAGE_ID) {
      disk_manager_->WritePage(pid, pages_[i].GetData());
      pages_[i].is_dirty_ = false;
    }
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return true;
  }
  auto fid = it->second;
  if (pages_[fid].pin_count_ > 0) {
    return false;
  }
  replacer_->Evict(&fid);
  free_list_.emplace_back(static_cast<int>(fid));
  // reset page metadata
  pages_[fid].ResetMemory();
  pages_[fid].page_id_ = INVALID_PAGE_ID;
  pages_[fid].pin_count_ = 0;
  pages_[fid].is_dirty_ = false;
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
