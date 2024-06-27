//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_sequential_scale_test.cpp
//
// Identification: test/storage/b_plus_tree_sequential_scale_test.cpp
//
// Copyright (c) 2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdio>
#include <random>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

/**
 * This test should be passing with your Checkpoint 1 submission.
 */
TEST(BPlusTreeTests, InsertScaleTest) {  // NOLINT
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(30, disk_manager.get());

  // create and fetch header_page
  page_id_t page_id;
  auto *header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  int64_t scale = 5000;
  std::vector<int64_t> keys;
  for (int64_t key = 1; key < scale; key++) {
    keys.push_back(key);
  }

  // randomized the insertion order
  auto rng = std::default_random_engine{};
  std::shuffle(keys.begin(), keys.end(), rng);
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    ASSERT_EQ(tree.GetValue(index_key, &rids), true);
    ASSERT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    ASSERT_EQ(rids[0].GetSlotNum(), value);
  }

  // iterator through the b+tree
  int64_t start_key = *std::min_element(keys.begin(), keys.end());
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key & 0xFFFFFFFF);
    current_key = current_key + 1;
  }
  EXPECT_EQ(current_key, keys.size() + 1);

  start_key = 2550;
  current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key & 0xFFFFFFFF);
    current_key = current_key + 1;
  }
  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete bpm;
}

TEST(BPlusTreeTests, DeleteScaleTest) {  // NOLINT
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(30, disk_manager.get());

  // create and fetch header_page
  page_id_t page_id;
  auto *header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  constexpr int64_t SCALE = 200;
  std::vector<int64_t> keys;
  for (int64_t key = 1; key <= SCALE; key++) {
    keys.push_back(key);
  }

  // randomized the insertion order
  auto rng = std::default_random_engine{};
  std::shuffle(keys.begin(), keys.end(), rng);
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  tree.Draw(bpm, "bpt-before-deletion.dot");

  // do some search after insertion
  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    ASSERT_EQ(tree.GetValue(index_key, &rids), true);
    ASSERT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    ASSERT_EQ(rids[0].GetSlotNum(), value);
  }

  // delete some keys from b+tree
  std::vector<int64_t> remove_keys = {keys.begin(), keys.begin() + 100};
  // delete some keys repeatedly
  // remove_keys.insert(remove_keys.end(), keys.begin(), keys.begin() + 50);
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    // LOG_INFO("begin to remove key %s", std::to_string(key).c_str());
    tree.Remove(index_key, transaction);
  }

  tree.Draw(bpm, "bpt-after-deletion.dot");

  // do some search after deletion
  int64_t size = 0;
  bool is_present;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);
    if (!is_present) {
      EXPECT_NE(std::find(remove_keys.begin(), remove_keys.end(), key), remove_keys.end());
    } else {
      EXPECT_EQ(rids.size(), 1);
      EXPECT_EQ(rids[0].GetPageId(), 0);
      EXPECT_EQ(rids[0].GetSlotNum(), key);
      size = size + 1;
    }
  }

  // iterate through the b+tree
  for (auto key : remove_keys) {
    auto rm_iter = std::remove_if(keys.begin(), keys.end(), [=](auto k) { return k == key; });
    keys.erase(rm_iter, keys.end());
  }
  std::sort(keys.begin(), keys.end());

  int key_index = 0;
  index_key.SetFromInteger(keys.at(key_index));
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    auto current_key = keys.at(key_index);
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key & 0xFFFFFFFF);
    key_index++;
  }
  EXPECT_EQ(key_index, keys.size());

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete bpm;
}
}  // namespace bustub
