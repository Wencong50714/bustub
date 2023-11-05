#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (page_ != nullptr) {
    bpm_->UnpinPage(PageId(), is_dirty_);
    page_ = nullptr;
  }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  // check self assign
  if (this != &that) {
    // release the resource
    Drop();

    // transfer the resource of that to this
    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;
    that.bpm_ = nullptr;
    that.page_ = nullptr;
    that.is_dirty_ = false;
  }
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); }
auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  auto read_guard = ReadPageGuard(this->bpm_, this->page_);
  this->page_->RLatch();

  this->bpm_ = nullptr;
  this->page_ = nullptr;
  this->is_dirty_ = false;
  return read_guard;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  auto write_guard = WritePageGuard(this->bpm_, this->page_);
  this->page_->WLatch();

  this->bpm_ = nullptr;
  this->page_ = nullptr;
  this->is_dirty_ = false;
  return write_guard;
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default;

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    Drop();
    guard_ = std::move(that.guard_);
  }
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.bpm_->UnpinPage(PageId(), guard_.is_dirty_);
    guard_.page_->RUnlatch();
    guard_.page_ = nullptr;
  }
}

ReadPageGuard::~ReadPageGuard() { Drop(); }

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    Drop();
    guard_ = std::move(that.guard_);
  }
  return *this;
}

void WritePageGuard::Drop() {
  if (this->guard_.page_ != nullptr) {
    guard_.bpm_->UnpinPage(PageId(), guard_.is_dirty_);
    guard_.page_->WUnlatch();
    guard_.page_ = nullptr;
  }
}

WritePageGuard::~WritePageGuard() { Drop(); }

}  // namespace bustub
