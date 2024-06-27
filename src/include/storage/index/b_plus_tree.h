/**
 * b_plus_tree.h
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
#pragma once

#include <algorithm>
#include <deque>
#include <iostream>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

struct PrintableBPlusTree;

/**
 * @brief Definition of the Context class.
 *
 * Hint: This class is designed to help you keep track of the pages
 * that you're modifying or accessing.
 */
class Context {
 public:
  // When you insert into / remove from the B+ tree, store the write guard of header page here.
  // Remember to drop the header page guard and set it to nullopt when you want to unlock all.
  std::optional<WritePageGuard> header_page_write_{std::nullopt};

  // When you insert read from the B+ tree, store the read guard of header page here.
  std::optional<ReadPageGuard> header_page_read_{std::nullopt};

  // Save the root page id here so that it's easier to know if the current page is the root page.
  page_id_t root_page_id_{INVALID_PAGE_ID};

  // Store the write guards of the pages that you're modifying here.
  std::deque<WritePageGuard> write_set_;

  // You may want to use this when getting value, but not necessary.
  std::deque<ReadPageGuard> read_set_;

  auto IsRootPage(page_id_t page_id) -> bool { return page_id == root_page_id_; }

  auto ToString() -> std::string {
    std::string out = "Context info:\n\t";
    if (header_page_write_.has_value()) {
      out += fmt::format("head page {} with write guard\n\t", header_page_write_->PageId());
    }
    if (header_page_read_.has_value()) {
      out += fmt::format("head page {} with read guard\n\t", header_page_read_->PageId());
    }
    out += "root page ";
    if (root_page_id_ == INVALID_PAGE_ID) {
      out += "INVALID_PAGE_ID\n\t";
    } else {
      out += std::to_string(root_page_id_) + "\n\t";
    }
    out += "write page guard set (page ids):\t";
    for (auto &guard : write_set_) {
      out += std::to_string(guard.PageId()) += " ";
    }
    out += "\n\t";
    out += "read page guard set (page ids):\t";
    for (auto &guard : read_set_) {
      out += std::to_string(guard.PageId()) += " ";
    }

    return out;
  }
};

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

// Main class providing the API for the Interactive B+ Tree.
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  explicit BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                     const KeyComparator &comparator, int leaf_max_size = LEAF_PAGE_SIZE,
                     int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *txn = nullptr) -> bool;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *txn);

  // Return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn = nullptr) -> bool;

  // Return the page id of the root node
  auto GetRootPageId() const -> page_id_t;

  void SetRootPageId(Context *ctx, page_id_t page_id);

  // Index iterator
  auto Begin() -> INDEXITERATOR_TYPE;

  auto End() -> INDEXITERATOR_TYPE;

  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;

  // Print the B+ tree
  void Print(BufferPoolManager *bpm);

  // Draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  /**
   * @brief draw a B+ tree, below is a printed
   * B+ tree(3 max leaf, 4 max internal) after inserting key:
   *  {1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 18, 19, 20}
   *
   *                               (25)
   *                 (9,17,19)                          (33)
   *  (1,5)    (9,13)    (17,18)    (19,20,21)    (25,29)    (33,37)
   *
   * @return std::string
   */
  auto DrawBPlusTree() -> std::string;

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *txn = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *txn = nullptr);

 private:
  /* Debug Routines for FREE!! */
  void ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out);

  void PrintTree(page_id_t page_id, const BPlusTreePage *page);

  auto IsLeafPage(page_id_t page_id) const -> bool;

  /**
   * @brief Convert A B+ tree into a Printable B+ tree
   *
   * @param root_id
   * @return PrintableNode
   */
  auto ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree;

  /**
   * @brief Find child page that key should locate at.
   *
   * @param page The InternalPage to traverse through to find corrent child page.
   * @param key
   * @return page_id_t Page id of found page.
   */
  auto FindChild(const InternalPage *page, const KeyType &key) -> page_id_t;

  /**
   * @brief Get the leaf page as ReadPageGuard using Basic Latch Crabbing Protocal.
   * Start at the root and go down, repeatedly acquire READ latch on the child and then unlatch parent.
   *
   * @param key Target key to search
   * @param[out] ctx Context that keeps track of the pages that is accessing.
   */
  void BasicCrabbingSearch(const KeyType &key, Context *ctx);

  // define operation type in B+tree
  enum class OperationType { Insertion, Deletion };

  /**
   * @brief Get the leaf page as WritePageGuard optimisticly using Improved Latch Crabbing Protocal.
   * Set READ latches as BasicCrabbingSearch do, go to leaf, and set WRITE latch on leaf. If the leaf is not safe,
   * release all previous latches and return false.
   *
   * @param key Target key to search
   * @param[out] ctx Context that keeps track of the pages that is accessing.
   *
   * @return Leaf page is safe or not
   */
  [[deprecated("Optimistic way is not complete, use pessimistic way instead.")]] auto CrabbingSearchOptimistic(
      const KeyType &key, Context *ctx, OperationType op_type) -> bool;

  /**
   * @brief Get the leaf page as WritePageGuard pessimisticly using Improved Latch Crabbing Protocal.
   * Start at the root and go down, obtaining X WRITE latches as needed. Once the child is latched, check if it is safe.
   * If the child is safe, release latches on all its ancestors.
   *
   * @param key Target key to search
   * @param[out] ctx Context that keeps track of the pages that is accessing.
   */
  void CrabbingSearchPessimistic(const KeyType &key, Context *ctx, OperationType op_type);

  void CrabbingSearchConservative(const KeyType &key, Context *ctx);

  /**
   * @brief Insert a new key-value pair into Internal Page
   *
   * @param ctx Pointer to Context object contains page guard of the internal page to be inserted
   * @param key
   * @param page_id
   */
  void InsertIntoInternalPage(Context *ctx, const KeyType &key, page_id_t page_id);

  // tools to deduce value type for given page type
  template <typename T>
  struct page_value_t;

  // for LeafPage, value type is specified in template parameter
  template <>
  struct page_value_t<LeafPage> {
    using type = ValueType;
  };

  // for InternalPage, value type is page_id_t
  template <>
  struct page_value_t<InternalPage> {
    using type = page_id_t;
  };

  /**
   * @brief Redistribute OR Merge leaf pages to make all of them at least half full.
   *
   * @param parent_page_guard Pointer to page guard of parent page of the starving leaf page
   * @starving_page_guard Pointer to page guard of the child page that is not half full
   */
  template <typename ChildPageType>
  auto RedisOrMergeChildPage(WritePageGuard *parent_page_guard, WritePageGuard *starving_page_guard) -> page_id_t;

  /**
   * @brief Remove an entry from internal page that points to given page.
   *
   * @param ctx Pointer to Context object contains page guard of the internal page to remove from
   * @param page_id
   */
  void RemoveFromInternalPage(Context *ctx, page_id_t page_id);

  // member variable
  std::string index_name_;
  BufferPoolManager *bpm_;
  KeyComparator comparator_;
  std::vector<std::string> log;  // NOLINT
  int leaf_max_size_;
  int internal_max_size_;
  page_id_t header_page_id_;
};

/**
 * @brief for test only. PrintableBPlusTree is a printalbe B+ tree.
 * We first convert B+ tree into a printable B+ tree and the print it.
 */
struct PrintableBPlusTree {
  int size_;
  std::string keys_;
  std::vector<PrintableBPlusTree> children_;

  /**
   * @brief BFS traverse a printable B+ tree and print it into
   * into out_buf
   *
   * @param out_buf
   */
  void Print(std::ostream &out_buf) {
    std::vector<PrintableBPlusTree *> que = {this};
    while (!que.empty()) {
      std::vector<PrintableBPlusTree *> new_que;

      for (auto &t : que) {
        int padding = (t->size_ - t->keys_.size()) / 2;
        out_buf << std::string(padding, ' ');
        out_buf << t->keys_;
        out_buf << std::string(padding, ' ');

        for (auto &c : t->children_) {
          new_que.push_back(&c);
        }
      }
      out_buf << "\n";
      que = new_que;
    }
  }
};

}  // namespace bustub
