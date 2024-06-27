//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  IndexIterator();
  IndexIterator(page_id_t leaf_page_id, int entry_idx, BufferPoolManager *bpm);
  IndexIterator(ReadPageGuard &&guard, int entry_idx, BufferPoolManager *bpm);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return this->page_id_ == itr.page_id_ && this->entry_idx_ == itr.entry_idx_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool { return !this->operator==(itr); }

 private:
  page_id_t page_id_;
  int entry_idx_;
  BufferPoolManager *bpm_;

  ReadPageGuard guard_;

  MappingType entry_;
};

}  // namespace bustub
