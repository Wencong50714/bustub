//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  evictable_.reset(new bool[num_frames]);
  ks_.reset(new size_t[num_frames]);
  std::fill_n(evictable_.get(), num_frames, false);
  std::fill_n(ks_.get(), num_frames, 0);
}

auto LRUKReplacer::ScanList(std::list<frame_id_t> &l, frame_id_t *frame_id) -> bool {
  auto it = l.begin();
  for (; it != l.end(); ++it) {
    auto fid = *it;
    if (evictable_[fid]) {
      *frame_id = fid;

      l.erase(it);
      evictable_[fid] = false;
      ks_[fid] = 0;
      return true;
    }
  }
  return false;
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (less_k_list_.empty() && more_k_list_.empty()) {
    return false;
  }

  // scan less_k list to find a evict-able one
  if (ScanList(less_k_list_, frame_id) || ScanList(more_k_list_, frame_id)) {
    curr_size_--;
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);

  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }

  // if the frame is new, add to less list
  if (ks_[frame_id] == 0) {

    less_k_list_.emplace_back(frame_id);
  }

  ks_[frame_id]++;

  // if frame achieve the k_, remove from less and add to more
  if (ks_[frame_id] == k_) {
    auto it = std::find(less_k_list_.begin(), less_k_list_.end(), frame_id);
    less_k_list_.erase(it);
    more_k_list_.emplace_back(frame_id);
  }

  // if already in the more-k list, re-arrange it
  if (ks_[frame_id] > k_) {
    auto it = std::find(more_k_list_.begin(), more_k_list_.end(), frame_id);
    more_k_list_.erase(it);
    more_k_list_.emplace_back(frame_id);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  if (ks_[frame_id] == 0) {
    // frame not in the context
    throw std::exception();
  }

  // alter the curr_size
  if (set_evictable && !evictable_[frame_id]) {
    curr_size_++;
  } else if (!set_evictable && evictable_[frame_id]) {
    curr_size_--;
  }

  evictable_[frame_id] = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  if (ks_[frame_id] == 0) {
    return ;
  }

  ks_[frame_id] = 0;
  evictable_[frame_id] = false;

  auto it = std::find(less_k_list_.begin(), less_k_list_.end(), frame_id);
  if (it != less_k_list_.end()) {
    less_k_list_.erase(it);
    curr_size_--;
    return ;
  }

  it = std::find(more_k_list_.begin(), more_k_list_.end(), frame_id);
  if (it != more_k_list_.end()) {
    more_k_list_.erase(it);
    curr_size_--;
  }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
