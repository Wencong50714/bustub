
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
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t fid;
  bool d = false;
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
      d = true;
      auto promise = disk_scheduler_->CreatePromise();
      write_future_ = promise.get_future();
      disk_scheduler_->Schedule({true, pages_[fid].GetData(), pages_[fid].GetPageId(), std::move(promise)});
      pages_[fid].is_dirty_ = false;
    }
    page_table_.erase(pages_[fid].GetPageId());
  }
  // update metadata between schedule request and I/O operation finished
  page_id_t pid = AllocatePage();
  *page_id = pid;
  page_table_[pid] = fid;
  pages_[fid].page_id_ = pid;
  pages_[fid].pin_count_ = 1;
  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);
  if (d) {
    write_future_.get();
  }
  pages_[fid].ResetMemory();
  return &pages_[fid];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  // First search for page_id in the buffer pool.
  frame_id_t fid;
  bool d = false;
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
      d = true;
      auto promise = disk_scheduler_->CreatePromise();
      write_future_ = promise.get_future();
      disk_scheduler_->Schedule({true, pages_[fid].GetData(), pages_[fid].GetPageId(), std::move(promise)});
      pages_[fid].is_dirty_ = false;
    }
    page_table_.erase(pages_[fid].GetPageId());
  }
  if (d) {
    write_future_.get();
  }

  auto promise = disk_scheduler_->CreatePromise();
  read_future_ = promise.get_future();
  disk_scheduler_->Schedule({false, pages_[fid].GetData(), page_id, std::move(promise)});
  // update metadata between schedule request and I/O operation finished
  page_table_[page_id] = fid;
  pages_[fid].page_id_ = page_id;
  pages_[fid].pin_count_ = 1;
  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);

  read_future_.get();

  return &pages_[fid];
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

  if (--pages_[fid].pin_count_ == 0) {
    replacer_->SetEvictable(fid, true);
  }
  pages_[fid].is_dirty_ |= is_dirty;
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = page_table_.find(page_id);
  if (it != page_table_.end() && page_id != INVALID_PAGE_ID) {
    auto promise = disk_scheduler_->CreatePromise();
    auto f = promise.get_future();
    disk_scheduler_->Schedule({true, pages_[it->second].GetData(), page_id, std::move(promise)});
    f.get();
    pages_[it->second].is_dirty_ = false;
    return true;
  }
  return false;
}

void BufferPoolManager::FlushAllPages() {
  std::unordered_map<page_id_t, frame_id_t> back_page_table;
  {
    std::lock_guard<std::mutex> guard(latch_);
    back_page_table = page_table_;
  }

  for (auto &p : back_page_table) {
    FlushPage(p.first);
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
  page_table_.erase(page_id);
  replacer_->SetEvictable(fid, true);
  replacer_->Remove(fid);
  free_list_.push_back(fid);
  pages_[fid].is_dirty_ = false;
  pages_[fid].page_id_ = INVALID_PAGE_ID;
  pages_[fid].ResetMemory();
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page_p = FetchPage(page_id);
  page_p->RLatch();
  return {this, page_p};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page_p = FetchPage(page_id);
  page_p->WLatch();
  return {this, page_p};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }
}  // namespace bustub