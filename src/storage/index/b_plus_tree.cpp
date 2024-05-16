#include <optional>
#include <sstream>
#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
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
  auto root_page_id = GetRootPageId();
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
  if (GetRootPageId() == INVALID_PAGE_ID) {
    return false;
  }
  Context ctx;
  ctx.root_page_id_ = GetRootPageId();
  BasicCrabbingSearch(key, &ctx);
  if (ctx.read_set_.empty()) {
    return false;
  }
  auto guard = std::move(ctx.read_set_.front());
  auto leaf_page = guard.template As<LeafPage>();
  auto size = leaf_page->GetSize();
  bool key_found = false;
  for (int i = 0; i < size; i++) {
    if (comparator_(key, leaf_page->KeyAt(i)) == 0) {
      result->push_back(leaf_page->ValueAt(i));
      key_found = true;
    }
  }
  return key_found;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  auto root_page_id = GetRootPageId();
  if (root_page_id == INVALID_PAGE_ID) {
    // current tree is empty, start new tree, update root page id and insert entry
    page_id_t page_id;
    auto tmp_guard = bpm_->NewPageGuardedWrite(&page_id);
    auto new_page = tmp_guard.template AsMut<LeafPage>();
    // init leaf page and insert key value pair into it
    new_page->Init(leaf_max_size_);
    new_page->InsertAt(0, key, value);
    // set new page to root page
    SetRootPageId(page_id);
    return true;
  }

  Context ctx;
  ctx.root_page_id_ = root_page_id;
  if (!CrabbingSearchOptimistic(key, &ctx, OperationType::Insertion)) {
    BUSTUB_ASSERT(ctx.read_set_.empty(), "");
    BUSTUB_ASSERT(ctx.write_set_.empty(), "");
    CrabbingSearchPessimistic(key, &ctx, OperationType::Insertion);
  }
  // take leaf page
  auto leaf_page_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto leaf_page = leaf_page_guard.template AsMut<LeafPage>();
  // check duplicate keys and
  // record the logical order of inserted entry should be in the leaf page
  int lo = leaf_page->GetSize();  // logical order
  for (int i = 0; i < leaf_page->GetSize(); i++) {
    if (comparator_(key, leaf_page->KeyAt(i)) == 0) {
      return false;
    }
    if (comparator_(key, leaf_page->KeyAt(i)) < 0) {
      lo = i;
      break;
    }
  }

  // if leaf page is not full, just insert entry into it, then the operation is done
  if (leaf_page->GetSize() < leaf_page->GetMaxSize()) {
    BUSTUB_ASSERT(ctx.write_set_.empty(), "");
    leaf_page->InsertAt(lo, key, value);
    return true;
  }
  // leaf page is already full, split is needed.
  BUSTUB_ASSERT(leaf_page->GetSize() == leaf_page->GetMaxSize(), "");

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
  // consider the situation that current leaf page is root page
  const KeyType &middle_key = new_page->KeyAt(0);
  if (ctx.IsRootPage(leaf_page_guard.PageId())) {
    BUSTUB_ASSERT(ctx.write_set_.empty(), "");
    page_id_t root_page_id;
    auto root_page_guard = bpm_->NewPageGuardedWrite(&root_page_id);
    auto root_page = root_page_guard.template AsMut<InternalPage>();
    root_page->Init(internal_max_size_);
    root_page->InsertAt(0, KeyType{}, leaf_page_guard.PageId());
    root_page->InsertAt(1, middle_key, new_page_id);
    SetRootPageId(root_page_id);
  } else {
    // insert an entry pointing to L2 into the parent of L
    BUSTUB_ASSERT(!ctx.write_set_.empty(), "");
    InsertIntoInternalPage(&ctx, middle_key, new_page_id);
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoInternalPage(Context *ctx, const KeyType &key, page_id_t page_id) {
  // take internal page
  auto internal_page_guard = std::move(ctx->write_set_.back());
  ctx->write_set_.pop_back();
  auto internal_page = internal_page_guard.template AsMut<InternalPage>();
  // check duplicate keys and
  // record the logical order of inserted entry should be in the leaf page
  int lo = internal_page->GetSize();  // logical order
  for (int i = 1; i < internal_page->GetSize(); i++) {
    if (comparator_(key, internal_page->KeyAt(i)) == 0) {
      throw Exception("Duplicate keys in internal page.");
    }
    if (comparator_(key, internal_page->KeyAt(i)) < 0) {
      lo = i;
      break;
    }
  }

  // if internal page is not full, just insert entry into it, then the operation is done
  if (internal_page->GetSize() < internal_page->GetMaxSize()) {
    BUSTUB_ASSERT(ctx->write_set_.empty(), "");
    internal_page->InsertAt(lo, key, page_id);
    return;
  }

  // internal page is already full, split is needed.
  BUSTUB_ASSERT(internal_page->GetSize() == internal_page->GetMaxSize(), "");
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
  // insert an entry pointing to L2 into the parent of L
  const KeyType &middle_key = new_page->KeyAt(0);
  // consider the situation that current internal page is root page
  if (ctx->IsRootPage(internal_page_guard.PageId())) {
    BUSTUB_ASSERT(ctx->write_set_.empty(), "");
    page_id_t root_page_id;
    auto root_page_guard = bpm_->NewPageGuardedWrite(&root_page_id);
    auto root_page = root_page_guard.template AsMut<InternalPage>();
    root_page->Init(internal_max_size_);
    root_page->InsertAt(0, KeyType{}, internal_page_guard.PageId());
    root_page->InsertAt(1, middle_key, new_page_id);
    SetRootPageId(root_page_id);
  } else {
    BUSTUB_ASSERT(!ctx->write_set_.empty(), "");
    InsertIntoInternalPage(ctx, middle_key, new_page_id);
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
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
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
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

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
void BPLUSTREE_TYPE::SetRootPageId(page_id_t page_id) {
  auto guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = guard.AsMut<BPlusTreeHeaderPage>();
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
  auto page_id = INVALID_PAGE_ID;
  auto size = page->GetSize();
  // traversal the internal page to find correct child page id
  for (int i = 1; i < size; i++) {  // start from index 1, ignore the first key (index 0)
    if (comparator_(key, page->KeyAt(i)) < 0) {
      page_id = page->ValueAt(i - 1);
      break;
    }
  }
  // if key don't less than any guide key, the last value is the correct child page id
  page_id = (page_id == INVALID_PAGE_ID) ? page->ValueAt(size - 1) : page_id;
  return page_id;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BasicCrabbingSearch(const KeyType &key, Context *ctx) {
  ctx->read_set_.emplace_back();  // redundant item to make while loop works correctly
  auto page_id = ctx->root_page_id_;
  // start from root page, search down the B+tree until we reach leaf page
  while (!IsLeafPage(page_id)) {
    // latch coupling
    ctx->read_set_.push_back(bpm_->FetchPageRead(page_id));
    ctx->read_set_.pop_front();
    auto page = ctx->read_set_.back().As<InternalPage>();
    page_id = FindChild(page, key);
  }
  // push leaf page
  ctx->read_set_.push_back(bpm_->FetchPageRead(page_id));
  ctx->read_set_.pop_front();
  BUSTUB_ASSERT(ctx->read_set_.size() == 1, "");
  BUSTUB_ASSERT(ctx->write_set_.empty(), "");
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CrabbingSearchOptimistic(const KeyType &key, Context *ctx, OperationType op_type) -> bool {
  ctx->read_set_.emplace_back();  // redundant item to make while loop works correctly
  auto page_id = ctx->root_page_id_;
  // start from root page, search down the B+tree until we reach leaf page
  while (!IsLeafPage(page_id)) {
    // latch coupling
    ctx->read_set_.push_back(bpm_->FetchPageRead(page_id));
    ctx->read_set_.pop_front();
    auto page = ctx->read_set_.back().As<InternalPage>();
    page_id = FindChild(page, key);
  }
  // push leaf page
  ctx->write_set_.push_back(bpm_->FetchPageWrite(page_id));
  ctx->read_set_.pop_front();
  BUSTUB_ASSERT(ctx->read_set_.empty(), "");
  BUSTUB_ASSERT(ctx->write_set_.size() == 1, "");
  auto page = ctx->write_set_.back().As<BPlusTreePage>();
  if ((op_type == OperationType::Insertion && page->GetSize() < page->GetMaxSize()) ||
      (op_type == OperationType::Deletion && page->GetSize() > page->GetMinSize())) {
    return true;
  }
  ctx->write_set_.pop_back();
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CrabbingSearchPessimistic(const KeyType &key, Context *ctx, OperationType op_type) {
  auto page_id = ctx->root_page_id_;
  // start from root page, search down the B+tree until we reach leaf page
  while (true) {
    // latch coupling
    ctx->write_set_.push_back(bpm_->FetchPageWrite(page_id));
    auto page = ctx->write_set_.back().As<BPlusTreePage>();
    // check if page is safe, if the page is safe, release latches on all its ancestors
    if ((op_type == OperationType::Insertion && page->GetSize() < page->GetMaxSize()) ||
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
  BUSTUB_ASSERT(ctx->read_set_.empty(), "");
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
