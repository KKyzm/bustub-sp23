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
  if (bpm_ == nullptr || page_ == nullptr) {
    return;
  }
  bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  Drop();
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); }

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  guard_ = std::move(that.guard_);
  dropped_ = that.dropped_;
  that.dropped_ = true;
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  Drop();
  guard_ = std::move(that.guard_);
  dropped_ = that.dropped_;
  that.dropped_ = true;
  return *this;
}

void ReadPageGuard::Drop() {
  if (dropped_) {
    return;
  }
  dropped_ = true;
  if (guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
  }
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() { Drop(); }

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  guard_ = std::move(that.guard_);
  dropped_ = that.dropped_;
  that.dropped_ = true;
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  Drop();
  guard_ = std::move(that.guard_);
  dropped_ = that.dropped_;
  that.dropped_ = true;
  return *this;
}

void WritePageGuard::Drop() {
  if (dropped_) {
    return;
  }
  dropped_ = true;
  if (guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
  }
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() { Drop(); }

}  // namespace bustub
