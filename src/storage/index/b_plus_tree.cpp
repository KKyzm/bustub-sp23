#include <optional>
#include <sstream>
#include <string>
#include <type_traits>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
// #include "spdlog/spdlog.h"
#include "storage/index/b_plus_tree.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page_id = header_page_guard.As<BPlusTreeHeaderPage>()->root_page_id_;
  if (root_page_id == INVALID_PAGE_ID) {
    // don't have a root page, obviously b+tree is empty
    return true;
  }
  auto guard = bpm_->FetchPageRead(root_page_id);
  auto root_page = guard.template As<BPlusTreePage>();
  return root_page->GetSize() == 0;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  Context ctx;
  ctx.header_page_read_ = bpm_->FetchPageRead(header_page_id_);
  ctx.root_page_id_ = ctx.header_page_read_->As<BPlusTreeHeaderPage>()->root_page_id_;
  auto root_page_id = ctx.root_page_id_;
  if (root_page_id == INVALID_PAGE_ID) {
    return false;
  }
  BasicCrabbingSearch(key, &ctx);
  auto leaf_page_guard = std::move(ctx.read_set_.front());
  auto leaf_page = leaf_page_guard.template As<LeafPage>();
  bool key_found = false;
  for (int i = 0; i < leaf_page->GetSize(); i++) {
    if (comparator_(key, leaf_page->KeyAt(i)) == 0) {
      result->push_back(leaf_page->ValueAt(i));
      key_found = true;
      // break, we only support unique key
      break;
    }
  }
  return key_found;
}

/*****************************************************************************
 * INSERTION
 ******************* **********************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 *
 * Split pages only when page size is EQUAL TO max page size BEFORE insertion
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // spdlog::debug("begin to insert key {}", key.GetAsInteger());
  Context ctx;
  ctx.header_page_write_ = bpm_->FetchPageWrite(header_page_id_);
  ctx.root_page_id_ = ctx.header_page_write_->As<BPlusTreeHeaderPage>()->root_page_id_;
  // spdlog::debug("find root page = {}", ctx.root_page_id_);
  // check whether root page exists and create one if not
  auto root_page_id = ctx.root_page_id_;
  if (root_page_id == INVALID_PAGE_ID) {
    // current tree is empty, build a new tree, update root page id and insert entry
    page_id_t page_id;
    auto tmp_guard = bpm_->NewPageGuardedWrite(&page_id);
    auto new_page = tmp_guard.template AsMut<LeafPage>();
    // init leaf page and insert key value pair into it
    new_page->Init(leaf_max_size_);
    new_page->InsertAt(0, key, value);
    // set new page to root page
    SetRootPageId(&ctx, page_id);
    // spdlog::debug("root page not exists, set new root page = {}, and insert key = {}", page_id, key.GetAsInteger());
    return true;
  }

  // CrabbingSearchConservative(key, &ctx);
  CrabbingSearchPessimistic(key, &ctx, OperationType::Insertion);
  // if (!CrabbingSearchOptimistic(key, &ctx, OperationType::Insertion)) {
  //   CrabbingSearchPessimistic(key, &ctx, OperationType::Insertion);
  // }
  // take leaf page
  auto leaf_page_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto leaf_page = leaf_page_guard.template AsMut<LeafPage>();
  // check duplicate keys and record the logical order of inserted entry should be in the leaf page
  int lo = leaf_page->GetSize();  // logical order
  for (int i = 0; i < leaf_page->GetSize(); i++) {
    if (comparator_(key, leaf_page->KeyAt(i)) == 0) {
      // spdlog::error("key {} collision", key.GetAsInteger());
      return false;
    }
    if (comparator_(key, leaf_page->KeyAt(i)) < 0) {
      lo = i;
      // spdlog::debug("inserting key {} into leaf page's {}th slot", key.GetAsInteger(), lo);
      break;
    }
  }

  // if leaf page is not full, just insert entry into it, then the operation is done
  if (leaf_page->GetSize() < leaf_page->GetMaxSize()) {
    // spdlog::debug("leaf page size {} < max size {}, just insert", leaf_page->GetSize(), leaf_page->GetMaxSize());
    leaf_page->InsertAt(lo, key, value);
    return true;
  }
  // leaf page is already full, split is needed.
  BUSTUB_ASSERT(leaf_page->GetSize() == leaf_page->GetMaxSize(), "");
  // spdlog::debug("leaf page size {} == max size {}, split is needed", leaf_page->GetSize());

  // 1. Split leaf page L into two pages L1 and L2.
  // 2. Redistribute entries evenly and copy up the middle key.
  // 3. Insert an entry pointing to L2 into the parent of L.

  // get a new leaf page and init it
  page_id_t new_page_id;
  auto temp_guard = bpm_->NewPageGuardedWrite(&new_page_id);
  auto new_page = temp_guard.template AsMut<LeafPage>();
  new_page->Init(leaf_max_size_);
  new_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(new_page_id);

  // redistribute entries evenly according to the lo calculated above
  if (lo <= leaf_page->GetMaxSize() / 2) {
    for (int i = leaf_page->GetMaxSize() / 2; i < leaf_page->GetMaxSize(); i++) {
      new_page->InsertAt(i - leaf_page->GetMaxSize() / 2, leaf_page->KeyAt(i), leaf_page->ValueAt(i));
    }
    leaf_page->SetSize(leaf_page->GetMaxSize() / 2);
    leaf_page->InsertAt(lo, key, value);
  } else {
    for (int i = leaf_page->GetMaxSize() / 2 + 1; i < leaf_page->GetMaxSize(); i++) {
      new_page->InsertAt(i - (leaf_page->GetMaxSize() / 2 + 1), leaf_page->KeyAt(i), leaf_page->ValueAt(i));
    }
    leaf_page->SetSize(leaf_page->GetMaxSize() / 2 + 1);
    new_page->InsertAt(lo - (leaf_page->GetMaxSize() / 2 + 1), key, value);
  }
  // spdlog::debug("split evenly to new page = {}, key[0] = {}", new_page_id, new_page->KeyAt(0).GetAsInteger());

  // consider the situation that current leaf page is root page
  if (ctx.IsRootPage(leaf_page_guard.PageId())) {
    // BUSTUB_ASSERT(ctx.write_set_.empty(), "");
    page_id_t root_page_id;
    auto root_page_guard = bpm_->NewPageGuardedWrite(&root_page_id);
    auto root_page = root_page_guard.template AsMut<InternalPage>();
    root_page->Init(internal_max_size_);
    root_page->InsertAt(0, leaf_page->KeyAt(0), leaf_page_guard.PageId());
    root_page->InsertAt(1, new_page->KeyAt(0), new_page_id);
    SetRootPageId(&ctx, root_page_id);
    // spdlog::debug("reach root, set new root page = {}, points to {} and {}", leaf_page_guard.PageId(), new_page_id);
  } else {
    // insert an entry pointing to L2 into the parent of L
    BUSTUB_ASSERT(!ctx.write_set_.empty(), "");
    InsertIntoInternalPage(&ctx, new_page->KeyAt(0), new_page_id);
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoInternalPage(Context *ctx, const KeyType &key, page_id_t page_id) {
  // take internal page
  auto internal_page_guard = std::move(ctx->write_set_.back());
  ctx->write_set_.pop_back();
  auto internal_page = internal_page_guard.template AsMut<InternalPage>();
  // check duplicate keys and record the logical order of inserted entry should be in the leaf page
  int lo = internal_page->GetSize();  // logical order
  for (int i = 1; i < internal_page->GetSize(); i++) {
    if (comparator_(key, internal_page->KeyAt(i)) == 0) {
      throw Exception("Duplicate keys in internal page.");
    }
    if (comparator_(key, internal_page->KeyAt(i)) < 0) {
      lo = i;
      // spdlog::debug("inserting key {} , page_id = {} into internal page's {}th slot", key.GetAsInteger(), page_id,
      // lo);
      break;
    }
  }

  // if internal page is not full, just insert entry into it, then the operation is done
  if (internal_page->GetSize() < internal_page->GetMaxSize()) {
    // clang-format off
    // spdlog::debug("internal page size {} < max size {},
    // just insert", internal_page->GetSize(), internal_page->GetMaxSize());
    // clang-format on
    internal_page->InsertAt(lo, key, page_id);
    return;
  }
  // internal page is already full, split is needed.
  BUSTUB_ASSERT(internal_page->GetSize() == internal_page->GetMaxSize(), "");
  // spdlog::debug("internal page size {} == max size {}, split is needed", internal_page->GetSize());

  // get a new internal page and init it
  page_id_t new_page_id;
  auto temp_guard = bpm_->NewPageGuardedWrite(&new_page_id);
  auto new_page = temp_guard.template AsMut<InternalPage>();
  new_page->Init(internal_max_size_);
  // redistribute entries evenly according to the lo calculated above
  if (lo <= internal_page->GetMaxSize() / 2) {
    for (int i = internal_page->GetMaxSize() / 2; i < internal_page->GetMaxSize(); i++) {
      new_page->InsertAt(i - internal_page->GetMaxSize() / 2, internal_page->KeyAt(i), internal_page->ValueAt(i));
    }
    internal_page->SetSize(internal_page->GetMaxSize() / 2);
    internal_page->InsertAt(lo, key, page_id);
  } else {
    for (int i = internal_page->GetMaxSize() / 2 + 1; i < internal_page->GetMaxSize(); i++) {
      new_page->InsertAt(i - (internal_page->GetMaxSize() / 2 + 1), internal_page->KeyAt(i), internal_page->ValueAt(i));
    }
    internal_page->SetSize(internal_page->GetMaxSize() / 2 + 1);
    new_page->InsertAt(lo - (internal_page->GetMaxSize() / 2 + 1), key, page_id);
  }
  // spdlog::debug("split evenly to new page = {}, key[0] = {}", new_page_id, new_page->KeyAt(0).GetAsInteger());

  // consider the situation that current internal page is root page
  if (ctx->IsRootPage(internal_page_guard.PageId())) {
    // BUSTUB_ASSERT(ctx->write_set_.empty(), "");
    page_id_t root_page_id;
    auto root_page_guard = bpm_->NewPageGuardedWrite(&root_page_id);
    auto root_page = root_page_guard.template AsMut<InternalPage>();
    root_page->Init(internal_max_size_);
    root_page->InsertAt(0, internal_page->KeyAt(0), internal_page_guard.PageId());
    root_page->InsertAt(1, new_page->KeyAt(0), new_page_id);
    SetRootPageId(ctx, root_page_id);
    // spdlog::debug("reach root, set new root page = {}, points to {} and {}", internal_page_guard.PageId(),
    // new_page_id);
  } else {
    // insert an entry pointing to L2 into the parent of L
    BUSTUB_ASSERT(!ctx->write_set_.empty(), "");
    InsertIntoInternalPage(ctx, new_page->KeyAt(0), new_page_id);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 *
 * Redistribute or merge pages only when page size is LESS THAN min page size AFTER deletion.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // spdlog::debug("begin to remove key {}", key.ToString());
  // if current tree is empty, just return
  if (IsEmpty()) {
    // spdlog::debug("current tree empty, remove fails");
    return;
  }

  Context ctx;
  ctx.header_page_write_ = bpm_->FetchPageWrite(header_page_id_);
  ctx.root_page_id_ = ctx.header_page_write_->As<BPlusTreeHeaderPage>()->root_page_id_;
  // spdlog::debug("find root page = {}", ctx.root_page_id_);
  CrabbingSearchPessimistic(key, &ctx, OperationType::Deletion);

  // spdlog::debug(ctx.ToString());

  // take leaf page
  auto leaf_page_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto leaf_page = leaf_page_guard.template AsMut<LeafPage>();
  // find the index of entry to be removed
  int idx = 0;
  bool is_break = false;
  for (; idx < leaf_page->GetSize(); idx++) {
    if (comparator_(key, leaf_page->KeyAt(idx)) == 0) {
      is_break = true;
      break;
    }
  }

  // spdlog::debug("find key {} locate at {}th slot of leaf page {}, then delete it", key.ToString(), idx,
  // leaf_page_guard.PageId());

  if (!is_break) {
    // spdlog::debug("didn't find key {}, just return", key.ToString());
    return;
  }

  leaf_page->DeleteAt(idx);

  // if leaf page is still at least half full, the operation is done
  // ">=" or ">"
  if (leaf_page->GetSize() >= leaf_page->GetMinSize()) {
    // spdlog::debug("leaf page {} is healthy, just return", leaf_page_guard.PageId());
    return;
  }

  // if leaf page is root page, the operation is also done
  if (ctx.IsRootPage(leaf_page_guard.PageId())) {
    // spdlog::debug("leaf page {} is root, just return", leaf_page_guard.PageId());
    return;
  }

  // the size of leaf page is less than half full,
  // try redistribute first
  BUSTUB_ASSERT(!ctx.write_set_.empty(), "");
  BUSTUB_ASSERT(leaf_page->GetSize() == leaf_page->GetMinSize() - 1, "");
  auto parent_page_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();

  // spdlog::debug("leaf page {} is starving, begin to redistribute or merge it", leaf_page_guard.PageId());
  auto page_id_to_remove = RedisOrMergeChildPage<LeafPage>(&parent_page_guard, &leaf_page_guard);
  // spdlog::debug("leaf page {} is starving, begin to redistribute it", leaf_page_guard.PageId());
  // bool success = RedistributeChildPage<LeafPage>(&parent_page_guard, &leaf_page_guard);
  //
  // if (success) {
  //   spdlog::debug("redistribution successes, just return");
  //   return;
  // }
  //
  // spdlog::debug("redistribution fails, begin to merge");
  // auto page_id_to_remove = MergeChildPage(&parent_page_guard, &leaf_page_guard);

  if (page_id_to_remove != INVALID_PAGE_ID) {
    ctx.write_set_.push_back(std::move(parent_page_guard));
    RemoveFromInternalPage(&ctx, page_id_to_remove);
  }
}

INDEX_TEMPLATE_ARGUMENTS
template <typename ChildPageType>
auto BPLUSTREE_TYPE::RedisOrMergeChildPage(WritePageGuard *parent_page_guard, WritePageGuard *starving_page_guard)
    -> page_id_t {
  using ChildPageValueType = typename page_value_t<ChildPageType>::type;
  constexpr bool is_leaf_starving = std::is_same<ChildPageType, LeafPage>();

  // spdlog::debug("redistribute or merge starving page {}, parent page is {}", starving_page_guard->PageId(),
  // parent_page_guard->PageId());

  auto parent_page = parent_page_guard->template AsMut<InternalPage>();

  // spdlog::debug("parent page {} layout:\n\t {}", parent_page_guard->PageId(), parent_page->ToString());

  bool could_merge = false;
  KeyType key{};
  ChildPageValueType value{};
  WritePageGuard child_page_guard{};
  int starving_page_index = -1;
  for (int i = 0; i < parent_page->GetSize(); i++) {
    auto child_page_id = parent_page->ValueAt(i);
    if (child_page_id == starving_page_guard->PageId()) {
      starving_page_index = i;
      break;
    }
    child_page_guard = bpm_->FetchPageWrite(child_page_id);
    auto *child_page = child_page_guard.template AsMut<ChildPageType>();

    if (could_merge) {
      auto tmp_key = child_page->KeyAt(child_page->GetSize() - 1);
      auto tmp_value = child_page->ValueAt(child_page->GetSize() - 1);
      child_page->DeleteAt(child_page->GetSize() - 1);
      child_page->InsertAt(0, key, value);
      parent_page->SetKeyAt(i, child_page->KeyAt(0));  // update parent page
      key = tmp_key;
      value = tmp_value;
      continue;
    }
    if (child_page->GetSize() > child_page->GetMinSize()) {
      could_merge = true;
      key = child_page->KeyAt(child_page->GetSize() - 1);
      value = child_page->ValueAt(child_page->GetSize() - 1);
      child_page->DeleteAt(child_page->GetSize() - 1);
    } else {
      BUSTUB_ASSERT(child_page->GetSize() == child_page->GetMinSize(), "");
    }
  }

  BUSTUB_ASSERT(starving_page_index >= 0, "");

  auto starving_page = starving_page_guard->AsMut<ChildPageType>();

  // case 1: redistribute from left side
  if (could_merge) {
    BUSTUB_ASSERT(starving_page_index > 0, "");
    starving_page->InsertAt(0, key, value);
    parent_page->SetKeyAt(starving_page_index, starving_page->KeyAt(0));  // update parent page
    return INVALID_PAGE_ID;
  }

  // case 2: merge starving page to left sibling page
  if (starving_page_index > 0) {
    auto *left_sibling_page = child_page_guard.template AsMut<ChildPageType>();
    for (int i = 0; i < starving_page->GetSize(); i++) {
      left_sibling_page->InsertAt(left_sibling_page->GetSize(), starving_page->KeyAt(i), starving_page->ValueAt(i));
    }
    if (is_leaf_starving) {
      // HACK: ugly way, but I have no better ideas now
      reinterpret_cast<LeafPage *>(left_sibling_page)
          ->SetNextPageId(reinterpret_cast<LeafPage *>(starving_page)->GetNextPageId());
    }
    return starving_page_guard->PageId();
  }

  auto right_sibling_page_id = parent_page->ValueAt(1);
  auto right_sibling_page_guard = bpm_->FetchPageWrite(right_sibling_page_id);
  auto right_sibling_page = right_sibling_page_guard.template AsMut<ChildPageType>();

  // case 3: redistribute with right sibling page
  if (right_sibling_page->GetSize() > right_sibling_page->GetMinSize()) {
    auto key = right_sibling_page->KeyAt(0);
    auto value = right_sibling_page->ValueAt(0);
    right_sibling_page->DeleteAt(0);
    parent_page->SetKeyAt(1, right_sibling_page->KeyAt(0));  // update parent page
    starving_page->InsertAt(starving_page->GetSize(), key, value);
    return INVALID_PAGE_ID;
  }

  // case 4: merge starving page to right sibling page
  for (int i = 0; i < right_sibling_page->GetSize(); i++) {
    starving_page->InsertAt(starving_page->GetSize(), right_sibling_page->KeyAt(i), right_sibling_page->ValueAt(i));
  }
  if (is_leaf_starving) {
    // HACK: ugly way, but I have no better ideas now
    reinterpret_cast<LeafPage *>(starving_page)
        ->SetNextPageId(reinterpret_cast<LeafPage *>(right_sibling_page)->GetNextPageId());
  }
  return right_sibling_page_guard.PageId();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromInternalPage(Context *ctx, page_id_t page_id) {
  auto internal_page_guard = std::move(ctx->write_set_.back());
  ctx->write_set_.pop_back();
  auto internal_page = internal_page_guard.template AsMut<InternalPage>();
  // find the index of entry to be removed and remove it
  int idx = 0;
  for (; idx < internal_page->GetSize(); idx++) {
    if (page_id == internal_page->ValueAt(idx)) {
      break;
    }
  }
  internal_page->DeleteAt(idx);
  bpm_->DeletePage(page_id);

  if (internal_page->GetSize() >= internal_page->GetMinSize()) {
    return;
  }

  if (ctx->IsRootPage(internal_page_guard.PageId())) {
    if (internal_page->GetSize() == 1) {
      // if current internal page is root page and it only has one entry left,
      // then delete current page and set its unique child page as root page
      SetRootPageId(ctx, internal_page->ValueAt(0));
      bpm_->DeletePage(ctx->root_page_id_);
      ctx->root_page_id_ = internal_page->ValueAt(0);
    }
    return;
  }

  BUSTUB_ASSERT(!ctx->write_set_.empty(), "");
  BUSTUB_ASSERT(internal_page->GetSize() == internal_page->GetMinSize() - 1, "");
  auto parent_page_guard = std::move(ctx->write_set_.back());
  ctx->write_set_.pop_back();

  auto page_id_to_remove = RedisOrMergeChildPage<InternalPage>(&parent_page_guard, &internal_page_guard);
  // bool success = RedistributeChildPage<InternalPage>(&parent_page_guard, &internal_page_guard);
  // if (success) {
  //   return;
  // }
  //
  // // redistribution fails, try merge
  // auto page_id_to_remove = MergeChildPage(&parent_page_guard, &internal_page_guard);

  if (page_id_to_remove != INVALID_PAGE_ID) {
    ctx->write_set_.push_back(std::move(parent_page_guard));
    RemoveFromInternalPage(ctx, page_id_to_remove);
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  Context ctx;
  ctx.read_set_.emplace_back();  // redundant item to make while loop works correctly
  auto page_id = GetRootPageId();
  while (!IsLeafPage(page_id)) {
    ctx.read_set_.push_back(bpm_->FetchPageRead(page_id));
    ctx.read_set_.pop_front();
    auto page = ctx.read_set_.back().As<InternalPage>();
    page_id = page->ValueAt(0);
  }

  return INDEXITERATOR_TYPE(page_id, 0, bpm_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  Context ctx;
  ctx.header_page_write_ = bpm_->FetchPageWrite(header_page_id_);
  ctx.root_page_id_ = ctx.header_page_write_->As<BPlusTreeHeaderPage>()->root_page_id_;
  BasicCrabbingSearch(key, &ctx);
  auto leaf_page_guard = std::move(ctx.read_set_.front());
  auto leaf_page = leaf_page_guard.template As<LeafPage>();
  auto size = leaf_page->GetSize();
  for (int i = 0; i < size; i++) {
    if (comparator_(key, leaf_page->KeyAt(i)) == 0) {
      return INDEXITERATOR_TYPE(std::move(leaf_page_guard), i, bpm_);
    }
  }
  return End();
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(INVALID_PAGE_ID, 0, bpm_); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() const -> page_id_t {
  auto guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_;
}

/**
 * @brief set page_id as the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SetRootPageId(Context *ctx, page_id_t page_id) {
  BUSTUB_ASSERT(ctx->header_page_write_ != std::nullopt, "");
  auto header_page = ctx->header_page_write_->AsMut<BPlusTreeHeaderPage>();
  header_page->root_page_id_ = page_id;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << '\n';
    std::cout << '\n';

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << '\n';

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << '\n';
    std::cout << '\n';
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << '\n';
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << '\n';
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsLeafPage(page_id_t page_id) const -> bool {
  auto guard = bpm_->FetchPageRead(page_id);
  auto page = guard.As<BPlusTreePage>();
  return page->IsLeafPage();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindChild(const InternalPage *page, const KeyType &key) -> page_id_t {
  auto size = page->GetSize();
  // traversal the internal page to find correct child page id
  for (int i = 1; i < size; i++) {  // start from index 1, ignore the first key (index 0)
    if (comparator_(key, page->KeyAt(i)) < 0) {
      return page->ValueAt(i - 1);
    }
  }
  // if key don't less than any guide key, the last value is the correct child page id
  return page->ValueAt(size - 1);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BasicCrabbingSearch(const KeyType &key, Context *ctx) {
  auto page_id = ctx->root_page_id_;
  // start from root page, search down the B+tree until we reach leaf page
  while (!IsLeafPage(page_id)) {
    // latch coupling
    ctx->read_set_.push_back(bpm_->FetchPageRead(page_id));
    if (ctx->read_set_.size() > 1) {
      ctx->read_set_.pop_front();
    } else {
      ctx->header_page_read_ = std::nullopt;
    }
    auto page = ctx->read_set_.back().As<InternalPage>();
    page_id = FindChild(page, key);
  }
  // push leaf page
  ctx->read_set_.push_back(bpm_->FetchPageRead(page_id));
  if (ctx->read_set_.size() > 1) {
    ctx->read_set_.pop_front();
  }
  BUSTUB_ASSERT(ctx->read_set_.size() == 1, "");
}

// NOT USED NOW: caller should make sure that this thread should only keep read lock on header page and discard write
// lock before using this optimistic way, so if this search fails, caller should recheck
// root_page_id's status and do appropriate operations, since the other threads may change the tree during the interval
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CrabbingSearchOptimistic(const KeyType &key, Context *ctx, OperationType op_type) -> bool {
  auto page_id = ctx->root_page_id_;
  // start from root page, search down the B+tree until we reach leaf page
  while (!IsLeafPage(page_id)) {
    // latch coupling
    ctx->read_set_.push_back(bpm_->FetchPageRead(page_id));
    if (ctx->read_set_.size() > 1) {
      ctx->read_set_.pop_front();
    } else {
      ctx->header_page_read_ = std::nullopt;
    }
    auto page = ctx->read_set_.back().As<InternalPage>();
    page_id = FindChild(page, key);
  }
  // push leaf page
  ctx->write_set_.push_back(bpm_->FetchPageWrite(page_id));
  ctx->read_set_.pop_front();
  auto page = ctx->write_set_.back().As<BPlusTreePage>();
  if ((op_type == OperationType::Insertion && page->GetSize() < page->GetMaxSize()) ||
      (op_type == OperationType::Deletion && page->GetSize() > page->GetMinSize())) {
    BUSTUB_ASSERT(ctx->read_set_.empty(), "");
    BUSTUB_ASSERT(ctx->write_set_.size() == 1, "");
    return true;
  }
  ctx->write_set_.pop_back();
  BUSTUB_ASSERT(ctx->read_set_.empty(), "");
  BUSTUB_ASSERT(ctx->write_set_.empty(), "");
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CrabbingSearchPessimistic(const KeyType &key, Context *ctx, OperationType op_type) {
  // spdlog::debug("find root page = {}", ctx->root_page_id_);
  auto page_id = ctx->root_page_id_;
  // start from root page, search down the B+tree until we reach leaf page
  while (true) {
    // latch coupling
    ctx->write_set_.push_back(bpm_->FetchPageWrite(page_id));
    auto page = ctx->write_set_.back().As<BPlusTreePage>();
    // check if page is safe, if the page is safe, release latches on all its ancestors
    if (ctx->IsRootPage(page_id)) {
      if ((op_type == OperationType::Insertion && page->GetSize() < page->GetMaxSize()) ||
          (op_type == OperationType::Deletion && page->GetSize() > 2)) {
        ctx->header_page_write_ = std::nullopt;
      }
    } else if ((op_type == OperationType::Insertion && page->GetSize() < page->GetMaxSize()) ||
               (op_type == OperationType::Deletion && page->GetSize() > page->GetMinSize())) {
      while (ctx->write_set_.size() > 1) {
        ctx->write_set_.pop_front();
      }
    }
    if (page->IsLeafPage()) {
      break;
    }
    page_id = FindChild(reinterpret_cast<const InternalPage *>(page), key);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CrabbingSearchConservative(const KeyType &key, Context *ctx) {
  // spdlog::debug("conservatively find leaf page with target key = {}", key.GetAsInteger());
  auto page_id = ctx->root_page_id_;
  // start from root page, search down the B+tree until we reach leaf page
  while (true) {
    ctx->write_set_.push_back(bpm_->FetchPageWrite(page_id));
    // spdlog::debug("pushed back page = {}", page_id);
    auto page = ctx->write_set_.back().As<BPlusTreePage>();
    if (page->IsLeafPage()) {
      break;
    }
    page_id = FindChild(reinterpret_cast<const InternalPage *>(page), key);
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
