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
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<frame_id_t>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  auto frame_id = frame_id_t{};
  auto guard = std::scoped_lock(buffer_pool_latch_);
  if (!GetFreeFrame(&frame_id)) {
    return nullptr;
  }
  auto page = &pages_[frame_id];
  *page_id = AllocatePage();
  // update page table
  page_table_.insert({*page_id, frame_id});

  // initialize new page
  page->page_id_ = *page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;

  // sync with replacer
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  Page *page = nullptr;
  auto guard = std::scoped_lock(buffer_pool_latch_);
  if (page_table_.find(page_id) != page_table_.end()) {
    // find target page in memory pool
    auto frame_id = page_table_.at(page_id);
    page = &pages_[frame_id];
    page->pin_count_++;  // update pin count
    BUSTUB_ASSERT(page_id == page->GetPageId(), "Page table should have consistent record");
  } else {
    // didn't find target page in memory pool, get a free frame and read page from disk
    auto frame_id = frame_id_t{};
    if (GetFreeFrame(&frame_id)) {
      page = &pages_[frame_id];
      page_table_.insert({page_id, frame_id});  // update page table

      // initialize new page
      disk_manager_->ReadPage(page_id, page->GetData());
      page->page_id_ = page_id;
      page->pin_count_ = 1;
      page->is_dirty_ = false;
    }
  }

  if (page != nullptr) {
    // sync with replacer (disable eviction and record the access of this frame)
    auto frame_id = page_table_.at(page_id);
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return page;
  }
  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  auto guard = std::scoped_lock(buffer_pool_latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  auto frame_id = page_table_.at(page_id);
  auto *page = &pages_[frame_id];
  if (page->GetPinCount() == 0) {
    return false;
  }
  page->is_dirty_ = page->IsDirty() || is_dirty;
  page->pin_count_--;
  if (page->GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  auto guard = std::scoped_lock(buffer_pool_latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  auto frame_id = page_table_.at(page_id);
  auto *page = &pages_[frame_id];
  disk_manager_->WritePage(page_id, page->data_);
  page->is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (const auto &[page_id, frame_id] : page_table_) {
    FlushPage(page_id);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  auto guard = std::scoped_lock(buffer_pool_latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  auto frame_id = page_table_.at(page_id);
  auto *page = &pages_[frame_id];
  if (page->GetPinCount() > 0) {
    return false;
  }

  // update buffer pool records
  page_table_.erase(page_id);
  free_list_.push_back(frame_id);
  replacer_->Remove(frame_id);
  // reset memory and meatdata
  page->ResetMemory();
  page->is_dirty_ = false;
  page->pin_count_ = 0;
  page->page_id_ = INVALID_PAGE_ID;

  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto *page = FetchPage(page_id);
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto *page = FetchPage(page_id);
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

auto BufferPoolManager::NewPageGuardedRead(page_id_t *page_id) -> ReadPageGuard {
  auto *page = NewPage(page_id);
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuardedWrite(page_id_t *page_id) -> WritePageGuard {
  auto *page = NewPage(page_id);
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::GetFreeFrame(frame_id_t *frame_id) -> bool {
  if (!free_list_.empty()) {  // try get a free frame from free list
    *frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Evict(frame_id)) {  // try to evict a frame, if it also fails, just return false.
    return false;
  }

  auto *page = &pages_[*frame_id];
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
  }

  // update page table
  page_table_.erase(page->GetPageId());
  // reset metadata of free page
  page->is_dirty_ = false;
  page->pin_count_ = 0;
  page->page_id_ = INVALID_PAGE_ID;
  page->ResetMemory();

  return true;
}

}  // namespace bustub
