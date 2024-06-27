//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_concurrent_test.cpp
//
// Identification: test/storage/b_plus_tree_concurrent_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <spdlog/logger.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <chrono>  // NOLINT
#include <cstddef>
#include <cstdio>
#include <functional>
#include <random>
#include <thread>  // NOLINT

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

// helper function to launch multiple threads
template <typename... Args>
void LaunchParallelTest(uint64_t num_threads, Args &&...args) {
  std::vector<std::thread> thread_group;

  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::thread(args..., thread_itr));
  }

  // Join the threads with the main thread
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }
}

// helper function to insert
void InsertHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);
  for (auto key : keys) {
    int64_t value = key;
    rid.Set(value >> 32, value);
    index_key.SetFromInteger(key);
    tree->Insert(index_key, rid, transaction);
  }
  delete transaction;
}

// helper function to seperate insert
void InsertHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                       int total_threads, __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);
  for (auto key : keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      int64_t value = key & 0xFFFFFFFF;
      rid.Set(static_cast<int32_t>(key >> 32), value);
      index_key.SetFromInteger(key);
      tree->Insert(index_key, rid, transaction);
    }
  }
  delete transaction;
}

// helper function to delete
void DeleteHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &remove_keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  // create transaction
  auto *transaction = new Transaction(0);
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree->Remove(index_key, transaction);
  }
  delete transaction;
}

// helper function to seperate delete
void DeleteHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree,
                       const std::vector<int64_t> &remove_keys, int total_threads,
                       __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<8> index_key;
  // create transaction
  auto *transaction = new Transaction(0);
  for (auto key : remove_keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      index_key.SetFromInteger(key);
      tree->Remove(index_key, transaction);
    }
  }
  delete transaction;
}

void LookupHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                  uint64_t tid, __attribute__((unused)) uint64_t thread_itr = 0) {
  auto *transaction = new Transaction(static_cast<txn_id_t>(tid));
  GenericKey<8> index_key;
  RID rid;
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    std::vector<RID> result;
    bool res = tree->GetValue(index_key, &result, transaction);
    ASSERT_EQ(res, true);
    ASSERT_EQ(result.size(), 1);
    ASSERT_EQ(result[0], rid);
  }
  delete transaction;
}

TEST(BPlusTreeConcurrentTest, SetLogger) {
  // init logger
  static auto local_file_logger = spdlog::basic_logger_mt("basic_logger", "concurrent.log");
  local_file_logger->set_pattern("[%H:%M:%S %z] [%^%L%$] [thread %t] %v");
  local_file_logger->set_level(spdlog::level::debug);
  static auto null_logger = spdlog::basic_logger_mt("null_logger", "/dev/null");

  // specify logger
  spdlog::set_default_logger(local_file_logger);
  // spdlog::set_default_logger(null_logger);
}

TEST(BPlusTreeConcurrentTest, InsertTest1) {
  spdlog::info("INSERT TEST 1 logging");

  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator, 2, 3);
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 100;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  LaunchParallelTest(5, InsertHelper, &tree, keys);

  std::vector<RID> rids;
  GenericKey<8> index_key;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete bpm;
}

TEST(BPlusTreeConcurrentTest, InsertTest2) {
  spdlog::debug("INSERT TEST 2 logging");

  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator, 2, 3);
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 100;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  LaunchParallelTest(5, InsertHelperSplit, &tree, keys, 2);

  std::vector<RID> rids;
  GenericKey<8> index_key;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete bpm;
}

TEST(BPlusTreeConcurrentTest, DeleteTest1) {
  spdlog::debug("DELETE TEST 1 logging");

  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());

  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator, 2, 3);
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 100;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  InsertHelper(&tree, keys);

  auto rng = std::default_random_engine{};
  std::shuffle(keys.begin(), keys.end(), rng);
  std::vector<int64_t> remove_keys = {keys.begin(), keys.begin() + 20};

  std::sort(keys.begin() + 20, keys.end());

  LaunchParallelTest(5, DeleteHelper, &tree, remove_keys);

  int64_t key_index = 22;
  index_key.SetFromInteger(keys.at(key_index));

  std::vector<RID> rids;
  tree.GetValue(index_key, &rids);
  EXPECT_EQ(rids.size(), 1);

  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    int64_t current_key = keys.at(key_index);
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    key_index = key_index + 1;
  }

  EXPECT_EQ(key_index, 99);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete bpm;
}

TEST(BPlusTreeConcurrentTest, DeleteTest2) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator);
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 100;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  InsertHelper(&tree, keys);

  auto rng = std::default_random_engine{};
  std::shuffle(keys.begin(), keys.end(), rng);
  std::vector<int64_t> remove_keys = {keys.begin(), keys.begin() + 20};

  std::sort(keys.begin() + 20, keys.end());
  LaunchParallelTest(5, DeleteHelperSplit, &tree, remove_keys, 5);

  int64_t key_index = 22;
  index_key.SetFromInteger(keys.at(key_index));

  std::vector<RID> rids;
  tree.GetValue(index_key, &rids);
  EXPECT_EQ(rids.size(), 1);

  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    int64_t current_key = keys.at(key_index);
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    key_index = key_index + 1;
  }

  EXPECT_EQ(key_index, 99);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete bpm;
}

TEST(BPlusTreeConcurrentTest, DeleteTest3) {
  static const size_t SCALE_FACTOR = 100000;
  static const size_t NUM_THREAD = 2;
  static const size_t LRU_K_SIZE = 4;
  static const size_t BUSTUB_BPM_SIZE = 256;
  static const size_t KEY_MODIFY_RANGE = 2048;

  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(BUSTUB_BPM_SIZE, disk_manager.get(), LRU_K_SIZE);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator);

  // keys to Insert
  std::vector<int64_t> keys;
  for (size_t key = 1; key <= SCALE_FACTOR; key++) {
    keys.push_back(key);
  }
  InsertHelper(&tree, keys);

  tree.Draw(bpm, "scale-before-deletion");
  return;

  std::vector<std::thread> threads;

  for (size_t thread_id = 0; thread_id < NUM_THREAD; thread_id++) {
    threads.emplace_back(std::thread([thread_id, &tree] {
      size_t key_start = SCALE_FACTOR / NUM_THREAD * thread_id;
      size_t key_end = SCALE_FACTOR / NUM_THREAD * (thread_id + 1);
      spdlog::debug("key range: [{}, {}]", key_start, key_end);

      size_t while_cnt = 0;
      while (while_cnt++ < 30) {
        static size_t offset = 0;
        offset += 10;
        bustub::GenericKey<8> index_key;
        size_t cnt = 0;
        for (auto key = key_start + offset; key < key_end && cnt < KEY_MODIFY_RANGE; key++, cnt++) {
          spdlog::debug("start from key {}", key_start + offset);
          index_key.SetFromInteger(key);
          tree.Remove(index_key, nullptr);
        }
      }
    }));
  }

  for (auto &thread : threads) {
    thread.join();
  }

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete bpm;
}

TEST(BPlusTreeConcurrentTest, MixTest1) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  // first, populate index
  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  InsertHelper(&tree, keys);

  // concurrent insert
  keys.clear();
  for (int i = 6; i <= 10; i++) {
    keys.push_back(i);
  }
  LaunchParallelTest(1, InsertHelper, &tree, keys);
  // concurrent delete
  std::vector<int64_t> remove_keys = {1, 4, 3, 5, 6};
  LaunchParallelTest(1, DeleteHelper, &tree, remove_keys);

  int64_t start_key = 2;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    size = size + 1;
  }

  EXPECT_EQ(size, 5);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete bpm;
}

TEST(BPlusTreeConcurrentTest, MixTest2) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());

  // create and fetch header_page
  page_id_t page_id;
  auto *header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator);

  // Add perserved_keys
  std::vector<int64_t> perserved_keys;
  std::vector<int64_t> dynamic_keys;
  int64_t total_keys = 50;
  int64_t sieve = 5;
  for (int64_t i = 1; i <= total_keys; i++) {
    if (i % sieve == 0) {
      perserved_keys.push_back(i);
    } else {
      dynamic_keys.push_back(i);
    }
  }
  InsertHelper(&tree, perserved_keys, 1);
  // Check there are 1000 keys in there
  size_t size;

  auto insert_task = [&](int tid) { InsertHelper(&tree, dynamic_keys, tid); };
  auto delete_task = [&](int tid) { DeleteHelper(&tree, dynamic_keys, tid); };
  auto lookup_task = [&](int tid) { LookupHelper(&tree, perserved_keys, tid); };

  std::vector<std::thread> threads;
  std::vector<std::function<void(int)>> tasks;
  tasks.emplace_back(insert_task);
  tasks.emplace_back(delete_task);
  tasks.emplace_back(lookup_task);

  size_t num_threads = 10;
  for (size_t i = 0; i < num_threads; i++) {
    threads.emplace_back(std::thread{tasks[i % tasks.size()], i});
  }
  for (size_t i = 0; i < num_threads; i++) {
    threads[i].join();
  }

  // Check all reserved keys exist
  size = 0;

  for (auto iter = tree.Begin(); iter != tree.End(); ++iter) {
    const auto &pair = *iter;
    if ((pair.first).ToString() % sieve == 0) {
      size++;
    }
  }

  ASSERT_EQ(size, perserved_keys.size());

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete bpm;
}

TEST(BPlusTreeConcurrentTest, MixTest3) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());

  // create and fetch header_page
  page_id_t page_id;
  auto *header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 2, 3);

  // Add perserved_keys
  std::vector<int64_t> perserved_keys;
  std::vector<int64_t> dynamic_keys;
  int64_t total_keys = 1000;
  int64_t sieve = 5;
  for (int64_t i = 1; i <= total_keys; i++) {
    if (i % sieve == 0) {
      perserved_keys.push_back(i);
    } else {
      dynamic_keys.push_back(i);
    }
  }
  InsertHelper(&tree, perserved_keys, 1);
  // Check there are 1000 keys in there
  size_t size;

  auto insert_task = [&](int tid) { InsertHelper(&tree, dynamic_keys, tid); };
  auto delete_task = [&](int tid) { DeleteHelper(&tree, dynamic_keys, tid); };
  auto lookup_task = [&](int tid) { LookupHelper(&tree, perserved_keys, tid); };

  std::vector<std::thread> threads;
  std::vector<std::function<void(int)>> tasks;
  tasks.emplace_back(insert_task);
  tasks.emplace_back(delete_task);
  tasks.emplace_back(lookup_task);

  size_t num_threads = 6;
  for (size_t i = 0; i < num_threads; i++) {
    threads.emplace_back(std::thread{tasks[i % tasks.size()], i});
  }
  for (size_t i = 0; i < num_threads; i++) {
    threads[i].join();
  }

  // Check all reserved keys exist
  size = 0;

  for (auto iter = tree.Begin(); iter != tree.End(); ++iter) {
    const auto &pair = *iter;
    if ((pair.first).ToString() % sieve == 0) {
      size++;
    }
  }

  ASSERT_EQ(size, perserved_keys.size());

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete bpm;
}

}  // namespace bustub
