/**
 * deadlock_detection_test.cpp
 */

#include <random>
#include <thread>  // NOLINT

#include <spdlog/spdlog.h>
#include "common/config.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"
#include "gtest/gtest.h"
#include "spdlog/sinks/basic_file_sink.h"

namespace bustub {
TEST(LockManagerDeadlockDetectionTest, SimpleEdgeTest) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  lock_mgr.txn_manager_ = &txn_mgr;

  lock_mgr.AddEdge(1, 0);
  lock_mgr.AddEdge(2, 1);
  auto edges = lock_mgr.GetEdgeList();
  EXPECT_EQ(2, edges.size());
}

TEST(LockManagerDeadlockDetectionTest, EdgeTest) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  lock_mgr.txn_manager_ = &txn_mgr;

  const int num_nodes = 100;
  const int num_edges = num_nodes / 2;
  const int seed = 15445;
  std::srand(seed);

  // Create txn ids and shuffle
  std::vector<txn_id_t> txn_ids;
  txn_ids.reserve(num_nodes);
  for (int i = 0; i < num_nodes; i++) {
    txn_ids.push_back(i);
  }
  EXPECT_EQ(num_nodes, txn_ids.size());
  auto rng = std::default_random_engine{};
  std::shuffle(txn_ids.begin(), txn_ids.end(), rng);
  EXPECT_EQ(num_nodes, txn_ids.size());

  // Create edges by pairing adjacent txn_ids
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  for (int i = 0; i < num_nodes; i += 2) {
    EXPECT_EQ(i / 2, lock_mgr.GetEdgeList().size());
    auto t1 = txn_ids[i];
    auto t2 = txn_ids[i + 1];
    lock_mgr.AddEdge(t1, t2);
    edges.emplace_back(t1, t2);
    EXPECT_EQ((i / 2) + 1, lock_mgr.GetEdgeList().size());
  }

  auto lock_mgr_edges = lock_mgr.GetEdgeList();
  EXPECT_EQ(num_edges, lock_mgr_edges.size());
  EXPECT_EQ(num_edges, edges.size());

  std::sort(lock_mgr_edges.begin(), lock_mgr_edges.end());
  std::sort(edges.begin(), edges.end());

  for (int i = 0; i < num_edges; i++) {
    EXPECT_EQ(edges[i], lock_mgr_edges[i]);
  }
}

TEST(LockManagerDeadlockDetectionTest, HasCycleSimpleTest1) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  lock_mgr.txn_manager_ = &txn_mgr;

  lock_mgr.AddEdge(1, 0);
  lock_mgr.AddEdge(2, 1);
  lock_mgr.AddEdge(3, 1);
  lock_mgr.AddEdge(4, 1);
  lock_mgr.AddEdge(5, 1);

  auto txn_id = txn_id_t{};
  auto has_cycle = lock_mgr.HasCycle(&txn_id);
  EXPECT_EQ(false, has_cycle);
}

TEST(LockManagerDeadlockDetectionTest, HasCycleSimpleTest2) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  lock_mgr.txn_manager_ = &txn_mgr;

  lock_mgr.AddEdge(1, 0);
  lock_mgr.AddEdge(2, 5);
  lock_mgr.AddEdge(3, 5);
  lock_mgr.AddEdge(4, 3);
  lock_mgr.AddEdge(5, 4);

  auto txn_id = txn_id_t{};
  auto has_cycle = lock_mgr.HasCycle(&txn_id);
  EXPECT_EQ(true, has_cycle);
  EXPECT_EQ(5, txn_id);
}

TEST(LockManagerDeadlockDetectionTest, HasCycleTest) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  lock_mgr.txn_manager_ = &txn_mgr;

  lock_mgr.AddEdge(2, 1);
  lock_mgr.AddEdge(3, 2);
  // Although one txn will not wait for multiple txns, but for test purpose, we don't consider this here.
  lock_mgr.AddEdge(1, 3);
  lock_mgr.AddEdge(1, 12);
  lock_mgr.AddEdge(5, 12);
  lock_mgr.AddEdge(6, 5);
  lock_mgr.AddEdge(9, 6);
  lock_mgr.AddEdge(4, 9);
  lock_mgr.AddEdge(12, 9);
  lock_mgr.AddEdge(11, 4);
  lock_mgr.AddEdge(9, 11);

  lock_mgr.AddEdge(20, 6);
  lock_mgr.AddEdge(12, 20);

  auto txn_id = txn_id_t{};

  auto has_cycle = lock_mgr.HasCycle(&txn_id);
  EXPECT_EQ(true, has_cycle);
  EXPECT_EQ(3, txn_id);
  lock_mgr.RemoveEdge(3, 2);
  lock_mgr.RemoveEdge(1, 3);

  has_cycle = lock_mgr.HasCycle(&txn_id);
  EXPECT_EQ(true, has_cycle);
  EXPECT_EQ(11, txn_id);
  lock_mgr.RemoveEdge(11, 4);
  lock_mgr.RemoveEdge(9, 11);

  has_cycle = lock_mgr.HasCycle(&txn_id);
  EXPECT_EQ(true, has_cycle);
  EXPECT_EQ(12, txn_id);
  lock_mgr.RemoveEdge(1, 12);
  lock_mgr.RemoveEdge(5, 12);
  lock_mgr.RemoveEdge(12, 20);
  lock_mgr.RemoveEdge(12, 9);

  has_cycle = lock_mgr.HasCycle(&txn_id);
  EXPECT_EQ(false, has_cycle);
}

TEST(LockManagerDeadlockDetectionTest, SetLogger) {
  // init logger
  static auto local_file_logger = spdlog::basic_logger_mt("basic_logger", "deadlock.log");
  local_file_logger->set_pattern("[%H:%M:%S %z] [%!] [thread %t] %v");
  local_file_logger->set_level(spdlog::level::debug);
  local_file_logger->flush_on(spdlog::level::debug);
  static auto null_logger = spdlog::basic_logger_mt("null_logger", "/dev/null");

  // specify logger
  spdlog::set_default_logger(local_file_logger);
  // spdlog::set_default_logger(null_logger);
}

TEST(LockManagerDeadlockDetectionTest, BasicDeadlockDetectionTest) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  lock_mgr.txn_manager_ = &txn_mgr;
  lock_mgr.StartDeadlockDetection();

  table_oid_t toid{0};
  RID rid0{0, 0};
  RID rid1{1, 1};
  auto *txn0 = txn_mgr.Begin();
  auto *txn1 = txn_mgr.Begin();
  EXPECT_EQ(0, txn0->GetTransactionId());
  EXPECT_EQ(1, txn1->GetTransactionId());

  std::thread t0([&] {
    // Lock and sleep
    bool res = lock_mgr.LockTable(txn0, LockManager::LockMode::INTENTION_EXCLUSIVE, toid);
    EXPECT_EQ(true, res);
    res = lock_mgr.LockRow(txn0, LockManager::LockMode::EXCLUSIVE, toid, rid0);
    EXPECT_EQ(true, res);
    EXPECT_EQ(TransactionState::GROWING, txn1->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // This will block
    res = lock_mgr.LockRow(txn0, LockManager::LockMode::EXCLUSIVE, toid, rid1);
    EXPECT_EQ(true, res);

    lock_mgr.UnlockRow(txn0, toid, rid1);
    lock_mgr.UnlockRow(txn0, toid, rid0);
    lock_mgr.UnlockTable(txn0, toid);

    txn_mgr.Commit(txn0);
    EXPECT_EQ(TransactionState::COMMITTED, txn0->GetState());
  });

  std::thread t1([&] {
    // Sleep so T0 can take necessary locks
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    bool res = lock_mgr.LockTable(txn1, LockManager::LockMode::INTENTION_EXCLUSIVE, toid);
    EXPECT_EQ(res, true);

    res = lock_mgr.LockRow(txn1, LockManager::LockMode::EXCLUSIVE, toid, rid1);
    EXPECT_EQ(TransactionState::GROWING, txn1->GetState());

    // This will block
    res = lock_mgr.LockRow(txn1, LockManager::LockMode::EXCLUSIVE, toid, rid0);
    EXPECT_EQ(res, false);

    EXPECT_EQ(TransactionState::ABORTED, txn1->GetState());
    txn_mgr.Abort(txn1);
  });

  // Sleep for enough time to break cycle
  std::this_thread::sleep_for(cycle_detection_interval * 2);

  t0.join();
  t1.join();

  delete txn0;
  delete txn1;
}
}  // namespace bustub
