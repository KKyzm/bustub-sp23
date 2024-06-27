/**
 * index_iterator.cpp
 */
#include <cassert>
#include "common/config.h"
#include "common/macros.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

#include "storage/index/index_iterator.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t leaf_page_id, int entry_idx, BufferPoolManager *bpm)
    : page_id_(leaf_page_id), entry_idx_(entry_idx), bpm_(bpm) {
  if (leaf_page_id != INVALID_PAGE_ID) {
    guard_ = bpm_->FetchPageRead(leaf_page_id);
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(ReadPageGuard &&guard, int entry_idx, BufferPoolManager *bpm)
    : page_id_(guard.PageId()), entry_idx_(entry_idx), bpm_(bpm) {
  guard_ = std::move(guard);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  if (IsEnd()) {
    BUSTUB_ASSERT(false, "Dereference end iterator.");
  }
  auto leaf_page = guard_.As<LeafPage>();
  BUSTUB_ASSERT(leaf_page->GetSize() > entry_idx_, "");
  entry_ = MappingType{leaf_page->KeyAt(entry_idx_), leaf_page->ValueAt(entry_idx_)};
  return entry_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (IsEnd()) {
    return *this;
  }
  auto leaf_page = guard_.As<LeafPage>();
  entry_idx_++;
  if (entry_idx_ == leaf_page->GetSize()) {
    page_id_ = leaf_page->GetNextPageId();
    if (page_id_ != INVALID_PAGE_ID) {
      auto &&next_guard = bpm_->FetchPageRead(page_id_);
      guard_ = std::move(next_guard);
    }
    entry_idx_ = 0;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
