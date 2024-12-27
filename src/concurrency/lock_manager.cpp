//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <algorithm>
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>

#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  TxnTakeLockCheck(txn, lock_mode);

  std::unique_lock<std::mutex> table_latch{table_lock_map_latch_};
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_.insert({oid, std::make_shared<LockRequestQueue>()});
  }
  auto &lock_request_queue = *table_lock_map_.at(oid);
  table_latch.unlock();

  std::unique_lock<std::mutex> request_queue_latch{lock_request_queue.latch_};
  auto &lock_requests = lock_request_queue.request_queue_;
  auto upgrade_info = GetUpgradeInfo(txn, lock_mode, lock_request_queue);

  if (upgrade_info.IsUpgradeRequest()) {
    if (upgrade_info.pre_upgrade_lock_mode_ == lock_mode) {
      return true;
    }
    // lock upgrade is allowed, release original granted lock and set upgrading_
    auto pre_upgrade_request_ptr = *upgrade_info.pre_upgrade_lock_request_;
    delete pre_upgrade_request_ptr;
    lock_requests.erase(upgrade_info.pre_upgrade_lock_request_);
    UpdateTxnLockTable(txn, upgrade_info.pre_upgrade_lock_mode_, std::nullopt, oid);
    lock_request_queue.upgrading_ = txn->GetTransactionId();
  }

  // grant lock if possible and add current request to queue
  auto request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
  bool compatible = true;
  for (auto lock_request : lock_requests) {
    if (lock_request->granted_ && !AreLocksCompatible(lock_request->lock_mode_, lock_mode)) {
      compatible = false;
      break;
    }
  }
  if (upgrade_info.IsUpgradeRequest()) {
    lock_requests.push_front(request);
  } else {
    lock_requests.push_back(request);
  }
  if (compatible) {
    request->granted_ = true;
  } else if (upgrade_info.IsUpgradeRequest()) {
    GrantNewLocksIfPossible(&lock_request_queue);
    lock_request_queue.cv_.notify_all();
  }

  // wait
  lock_request_queue.cv_.wait(
      request_queue_latch, [&]() -> bool { return request->granted_ || txn->GetState() == TransactionState::ABORTED; });
  if (txn->GetState() == TransactionState::ABORTED) {
    // do some clean work
    if (upgrade_info.IsUpgradeRequest()) {
      lock_request_queue.upgrading_ = INVALID_TXN_ID;
    }
    lock_requests.remove(request);
    delete request;
    GrantNewLocksIfPossible(&lock_request_queue);
    lock_request_queue.cv_.notify_all();
    return false;
  }

  if (upgrade_info.IsUpgradeRequest()) {
    lock_request_queue.upgrading_ = INVALID_TXN_ID;
  }
  UpdateTxnLockTable(txn, std::nullopt, lock_mode, oid);

  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  std::unique_lock<std::mutex> table_latch{table_lock_map_latch_};
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto &lock_request_queue = *table_lock_map_[oid];
  table_latch.unlock();

  std::unique_lock<std::mutex> request_queue_latch{lock_request_queue.latch_};
  auto &lock_requests = lock_request_queue.request_queue_;

  auto iter = lock_requests.begin();
  for (; iter != lock_requests.end(); iter++) {
    if ((*iter)->txn_id_ == txn->GetTransactionId()) {
      break;
    }
  }

  {  // checks
    if (iter == lock_requests.end() || !(*iter)->granted_) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    }

    auto shared_row_lock_set = txn->GetSharedRowLockSet();
    auto exclus_row_lock_set = txn->GetExclusiveRowLockSet();
    if ((shared_row_lock_set->find(oid) != shared_row_lock_set->end() && !shared_row_lock_set->at(oid).empty()) ||
        (exclus_row_lock_set->find(oid) != exclus_row_lock_set->end() && !exclus_row_lock_set->at(oid).empty())) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
    }
  }  // checks

  UpdateTxnLockTable(txn, (*iter)->lock_mode_, std::nullopt, oid);
  UpdateTxnState(txn, (*iter)->lock_mode_);

  // remove the request record
  auto request = *iter;
  delete request;
  lock_requests.erase(iter);

  GrantNewLocksIfPossible(&lock_request_queue);

  request_queue_latch.unlock();
  lock_request_queue.cv_.notify_all();

  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  //  {}}}", txn->GetTransactionId(), LockModeStr(lock_mode), oid, rid.GetPageId(), rid.GetSlotNum());

  // row locking should not support Intention locks
  if (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::INTENTION_EXCLUSIVE ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  TxnTakeLockCheck(txn, lock_mode);

  CheckAppropriateLockOnTable(txn, oid, lock_mode);

  std::unique_lock<std::mutex> row_latch{row_lock_map_latch_};
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_.insert({rid, std::make_shared<LockRequestQueue>()});
  }
  auto &lock_request_queue = *row_lock_map_.at(rid);
  row_latch.unlock();

  std::unique_lock<std::mutex> request_queue_latch{lock_request_queue.latch_};
  auto &lock_requests = lock_request_queue.request_queue_;
  auto upgrade_info = GetUpgradeInfo(txn, lock_mode, lock_request_queue);

  if (upgrade_info.IsUpgradeRequest()) {
    if (upgrade_info.pre_upgrade_lock_mode_ == lock_mode) {
      return true;
    }
    // lock upgrade is allowed, release original granted lock and set upgrading_
    auto pre_upgrade_request_ptr = *upgrade_info.pre_upgrade_lock_request_;
    delete pre_upgrade_request_ptr;
    lock_requests.erase(upgrade_info.pre_upgrade_lock_request_);
    UpdateTxnLockRow(txn, upgrade_info.pre_upgrade_lock_mode_, std::nullopt, oid, rid);
    lock_request_queue.upgrading_ = txn->GetTransactionId();
  }

  // grant lock if possible and add current request to queue
  auto request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
  bool compatible = true;
  for (auto lock_request : lock_requests) {
    if (lock_request->granted_ && !AreLocksCompatible(lock_request->lock_mode_, lock_mode)) {
      compatible = false;
      break;
    }
  }
  if (upgrade_info.IsUpgradeRequest()) {
    lock_requests.push_front(request);
  } else {
    lock_requests.push_back(request);
  }
  if (compatible) {
    request->granted_ = true;
  } else if (upgrade_info.IsUpgradeRequest()) {
    GrantNewLocksIfPossible(&lock_request_queue);
    lock_request_queue.cv_.notify_all();
  }

  lock_request_queue.cv_.wait(
      request_queue_latch, [&]() -> bool { return request->granted_ || txn->GetState() == TransactionState::ABORTED; });
  if (txn->GetState() == TransactionState::ABORTED) {
    // do some clean work
    if (upgrade_info.IsUpgradeRequest()) {
      lock_request_queue.upgrading_ = INVALID_TXN_ID;
    }
    lock_requests.remove(request);
    delete request;
    GrantNewLocksIfPossible(&lock_request_queue);
    lock_request_queue.cv_.notify_all();
    return false;
  }

  if (upgrade_info.IsUpgradeRequest()) {
    lock_request_queue.upgrading_ = INVALID_TXN_ID;
  }
  UpdateTxnLockRow(txn, std::nullopt, lock_mode, oid, rid);

  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  //  txn->GetTransactionId(), oid, rid.GetPageId(), rid.GetSlotNum());

  std::unique_lock<std::mutex> row_latch{row_lock_map_latch_};
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto &lock_request_queue = *row_lock_map_[rid];
  row_latch.unlock();

  std::unique_lock<std::mutex> request_queue_latch{lock_request_queue.latch_};
  auto &lock_requests = lock_request_queue.request_queue_;

  auto iter = lock_requests.begin();
  for (; iter != lock_requests.end(); iter++) {
    if ((*iter)->txn_id_ == txn->GetTransactionId()) {
      break;
    }
  }

  // check
  if (iter == lock_requests.end()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  UpdateTxnLockRow(txn, (*iter)->lock_mode_, std::nullopt, oid, rid);
  // if force is set to true, bypasses all 2PL checks as if the tuple is not locked
  if (!force) {
    UpdateTxnState(txn, (*iter)->lock_mode_);
  }

  // remove the request record
  auto request = *iter;
  delete request;
  lock_requests.erase(iter);

  GrantNewLocksIfPossible(&lock_request_queue);

  request_queue_latch.unlock();
  lock_request_queue.cv_.notify_all();

  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
  //
  std::unique_lock<std::mutex> table_latch{table_lock_map_latch_};
  std::unique_lock<std::mutex> row_latch{row_lock_map_latch_};
  for (const auto &[_, lock_request_queue_ptr] : table_lock_map_) {
    std::lock_guard<std::mutex> request_queue_latch{lock_request_queue_ptr->latch_};
    for (auto request : lock_request_queue_ptr->request_queue_) {
      delete request;
    }
  }
  for (const auto &[_, lock_request_queue_ptr] : row_lock_map_) {
    std::lock_guard<std::mutex> request_queue_latch{lock_request_queue_ptr->latch_};
    for (auto request : lock_request_queue_ptr->request_queue_) {
      delete request;
    }
  }
}

auto LockManager::AreLocksCompatible(LockMode holds, LockMode wants) -> bool {
  if (holds == LockMode::INTENTION_SHARED) {
    return wants != LockMode::EXCLUSIVE;
  }
  if (holds == LockMode::INTENTION_EXCLUSIVE) {
    return wants == LockMode::INTENTION_SHARED || wants == LockMode::INTENTION_EXCLUSIVE;
  }
  if (holds == LockMode::SHARED) {
    return wants == LockMode::INTENTION_SHARED || wants == LockMode::SHARED;
  }
  if (holds == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    return wants == LockMode::INTENTION_SHARED;
  }
  return false;
}

void LockManager::TxnTakeLockCheck(Transaction *txn, LockMode lock_mode) {
  // isolation level related checks
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
      (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
       lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
  }

  if (txn->GetState() == TransactionState::SHRINKING &&
      !(txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
        (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED))) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
}

auto LockManager::UpgradeCompatible(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
  if (curr_lock_mode == LockMode::INTENTION_SHARED) {
    return true;
  }

  if (curr_lock_mode == LockMode::SHARED) {
    return requested_lock_mode == LockMode::SHARED || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE ||
           requested_lock_mode == LockMode::EXCLUSIVE;
  }

  if (curr_lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    return requested_lock_mode == LockMode::INTENTION_EXCLUSIVE ||
           requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE || requested_lock_mode == LockMode::EXCLUSIVE;
  }

  if (curr_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    return requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE || requested_lock_mode == LockMode::EXCLUSIVE;
  }

  if (curr_lock_mode == LockMode::EXCLUSIVE) {
    return requested_lock_mode == LockMode::EXCLUSIVE;
  }

  return false;
}

void LockManager::GrantNewLocksIfPossible(LockRequestQueue *lock_request_queue) {
  auto &lock_requests = lock_request_queue->request_queue_;
  // if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
  //   auto upgrading_request_iter = std::find_if(lock_requests.begin(), lock_requests.end(), [&](LockRequest *r) ->
  //   bool {
  //     return r->txn_id_ == lock_request_queue->upgrading_;
  //   });
  //   BUSTUB_ASSERT(upgrading_request_iter != lock_requests.end(), "");
  //   std::rotate(lock_requests.begin(), upgrading_request_iter, upgrading_request_iter++);
  // }

  std::vector<LockRequest *> granted_requests;
  std::vector<LockRequest *> ungranted_requests;
  for (auto request : lock_requests) {
    if (request->granted_) {
      granted_requests.push_back(request);
    } else {
      ungranted_requests.push_back(request);
    }
  }

  // grant new locks
  for (auto ungranted : ungranted_requests) {
    bool could_grant_lock = true;
    for (auto granted : granted_requests) {
      if (!AreLocksCompatible(granted->lock_mode_, ungranted->lock_mode_)) {
        could_grant_lock = false;
        break;
      }
    }
    if (could_grant_lock) {
      ungranted->granted_ = true;
      granted_requests.push_back(ungranted);
    }
  }
}

auto LockManager::GetTxnLockTableByLockMode(Transaction *txn, LockMode lock_mode)
    -> std::shared_ptr<std::unordered_set<table_oid_t>> {
  if (lock_mode == LockMode::SHARED) {
    return txn->GetSharedTableLockSet();
  }
  if (lock_mode == LockMode::EXCLUSIVE) {
    return txn->GetExclusiveTableLockSet();
  }
  if (lock_mode == LockMode::INTENTION_SHARED) {
    return txn->GetIntentionSharedTableLockSet();
  }
  if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    return txn->GetIntentionExclusiveTableLockSet();
  }
  if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    return txn->GetSharedIntentionExclusiveTableLockSet();
  }
  return nullptr;
}

auto LockManager::GetTxnLockRowByLockMode(Transaction *txn, LockMode lock_mode)
    -> std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> {
  if (lock_mode == LockMode::SHARED) {
    return txn->GetSharedRowLockSet();
  }
  if (lock_mode == LockMode::EXCLUSIVE) {
    return txn->GetExclusiveRowLockSet();
  }
  return nullptr;
}

void LockManager::UpdateTxnLockTable(Transaction *txn, std::optional<LockMode> released_lock_mode,
                                     std::optional<LockMode> granted_lock_mode, const table_oid_t &oid) {
  if (released_lock_mode.has_value()) {
    auto lock_table = GetTxnLockTableByLockMode(txn, released_lock_mode.value());
    lock_table->erase(oid);
  }

  if (granted_lock_mode.has_value()) {
    auto lock_table = GetTxnLockTableByLockMode(txn, granted_lock_mode.value());
    lock_table->insert(oid);
  }
}

void LockManager::UpdateTxnLockRow(Transaction *txn, std::optional<LockMode> released_lock_mode,
                                   std::optional<LockMode> granted_lock_mode, const table_oid_t &oid, const RID &rid) {
  if (released_lock_mode.has_value()) {
    auto lock_row = GetTxnLockRowByLockMode(txn, released_lock_mode.value());
    lock_row->at(oid).erase(rid);
  }

  if (granted_lock_mode.has_value()) {
    auto lock_row = GetTxnLockRowByLockMode(txn, granted_lock_mode.value());
    (*lock_row)[oid].insert(rid);
  }
}

void LockManager::UpdateTxnState(Transaction *txn, LockMode released_lock_mode) {
  /*
   * TRANSACTION STATE UPDATE
   *    Unlock should update the transaction state appropriately (depending upon the ISOLATION LEVEL)
   *    Only unlocking S or X locks changes transaction state.
   */
  if (released_lock_mode != LockMode::SHARED && released_lock_mode != LockMode::EXCLUSIVE) {
    return;
  }
  /*
   *    REPEATABLE_READ:
   *        Unlocking S/X locks should set the transaction state to SHRINKING
   *
   *    READ_COMMITTED:
   *        Unlocking X locks should set the transaction state to SHRINKING.
   *        Unlocking S locks does not affect transaction state.
   *
   *   READ_UNCOMMITTED:
   *        Unlocking X locks should set the transaction state to SHRINKING.
   *        S locks are not permitted under READ_UNCOMMITTED.
   *            The behaviour upon unlocking an S lock under this isolation level is undefined.
   */
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ || released_lock_mode == LockMode::EXCLUSIVE) {
    txn->SetState(TransactionState::SHRINKING);
  }
}

void LockManager::CheckAppropriateLockOnTable(Transaction *txn, const table_oid_t &oid, LockMode row_lock_mode) {
  // transaction should have an appropriate lock on the table which the row belongs to
  const auto &s_locked_tables = txn->GetSharedTableLockSet();
  const auto &is_locked_tables = txn->GetIntentionSharedTableLockSet();
  const auto &x_locked_tables = txn->GetExclusiveTableLockSet();
  const auto &ix_locked_tables = txn->GetIntentionExclusiveTableLockSet();
  const auto &six_locked_tables = txn->GetSharedIntentionExclusiveTableLockSet();

  if (row_lock_mode == LockMode::SHARED) {
    if (s_locked_tables->find(oid) == s_locked_tables->end() &&
        is_locked_tables->find(oid) == is_locked_tables->end() &&
        x_locked_tables->find(oid) == x_locked_tables->end() &&
        ix_locked_tables->find(oid) == ix_locked_tables->end() &&
        six_locked_tables->find(oid) == six_locked_tables->end()) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  } else {
    if (x_locked_tables->find(oid) == x_locked_tables->end() &&
        ix_locked_tables->find(oid) == ix_locked_tables->end() &&
        six_locked_tables->find(oid) == six_locked_tables->end()) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }
}

auto LockManager::GetUpgradeInfo(Transaction *txn, LockMode requested_lock_mode,
                                 const LockRequestQueue &lock_request_queue) -> LockUpgradeInfo {
  auto &lock_requests = lock_request_queue.request_queue_;

  std::optional<LockMode> pre_upgrade_locked_mode;
  auto pre_upgrade_lock_request = lock_requests.end();

  for (auto iter = lock_requests.begin(); iter != lock_requests.end(); iter++) {
    if ((*iter)->txn_id_ == txn->GetTransactionId()) {
      pre_upgrade_locked_mode = (*iter)->lock_mode_;
      pre_upgrade_lock_request = iter;
      break;
    }
  }

  if (pre_upgrade_locked_mode.has_value()) {
    BUSTUB_ASSERT((*pre_upgrade_lock_request)->granted_, "Upgrade lock should have original lock granted. ");
    if (lock_request_queue.upgrading_ != INVALID_TXN_ID) {
      BUSTUB_ASSERT(lock_request_queue.upgrading_ != txn->GetTransactionId(), "");
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }
    if (!UpgradeCompatible(pre_upgrade_locked_mode.value(), requested_lock_mode)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }
  }

  return {pre_upgrade_locked_mode, pre_upgrade_lock_request};
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  std::lock_guard<std::mutex> waits_for_latch{waits_for_latch_};
  auto &waits_for_t2 = waits_for_[t2];
  if (std::find(waits_for_t2.begin(), waits_for_t2.end(), t1) == waits_for_t2.end()) {
    waits_for_t2.push_back(t1);
  }
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  std::lock_guard<std::mutex> waits_for_latch{waits_for_latch_};
  auto &waits_for_t2 = waits_for_[t2];
  waits_for_t2.erase(std::remove(waits_for_t2.begin(), waits_for_t2.end(), t1), waits_for_t2.end());
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  struct DFSTraversalUnit {
    txn_id_t txn_id_;
    size_t depth_;
  };
  std::lock_guard<std::mutex> waits_for_latch{waits_for_latch_};

  auto txn_ids = std::vector<txn_id_t>{};
  for (const auto &[txn_id, _] : waits_for_) {
    txn_ids.push_back(txn_id);
  }
  std::sort(txn_ids.begin(), txn_ids.end());  // for deterministic traversal order
  auto visited = std::unordered_set<txn_id_t>{};
  for (auto txn_id_start : txn_ids) {
    if (visited.find(txn_id_start) != visited.end()) {
      continue;
    }

    auto cycle = std::vector<txn_id_t>{};
    auto dfs_stack = std::deque<DFSTraversalUnit>{};
    dfs_stack.push_back({txn_id_start, 0});
    // find cricle
    while (!dfs_stack.empty()) {
      auto traversal_unit = dfs_stack.back();
      dfs_stack.pop_back();

      BUSTUB_ASSERT(cycle.size() >= traversal_unit.depth_, "");
      if (cycle.size() > traversal_unit.depth_) {
        cycle.resize(traversal_unit.depth_);
      }
      cycle.push_back(traversal_unit.txn_id_);

      visited.insert(traversal_unit.txn_id_);
      auto &waiting_txn_ids = waits_for_[traversal_unit.txn_id_];
      sort(waiting_txn_ids.begin(), waiting_txn_ids.end(), std::greater<>{});
      for (auto waiting_txn_id : waiting_txn_ids) {
        if (visited.find(waiting_txn_id) == visited.end()) {
          dfs_stack.push_back({waiting_txn_id, traversal_unit.depth_ + 1});
        } else if (std::find(cycle.begin(), cycle.end(), waiting_txn_id) != cycle.end()) {
          // cycle found!
          auto cycle_begin_iter = std::find(cycle.begin(), cycle.end(), waiting_txn_id);
          auto max_iter = std::max_element(cycle_begin_iter, cycle.end());
          *txn_id = *max_iter;
          return true;
        }
      }
    }
  }

  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::lock_guard<std::mutex> waits_for_latch{waits_for_latch_};
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (const auto &[t2, waits_for_t2] : waits_for_) {
    for (auto &t1 : waits_for_t2) {
      edges.emplace_back(t1, t2);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);

    waits_for_.clear();
    std::unique_lock<std::mutex> table_latch{table_lock_map_latch_};
    std::unique_lock<std::mutex> row_latch{row_lock_map_latch_};
    auto f_add_edges = [&](const std::shared_ptr<LockRequestQueue> &lock_request_queue_ptr) {
      std::lock_guard<std::mutex> request_queue_latch{lock_request_queue_ptr->latch_};
      std::vector<LockRequest *> granted_requests;
      std::vector<LockRequest *> ungranted_requests;
      for (auto request : lock_request_queue_ptr->request_queue_) {
        if (request->granted_) {
          granted_requests.push_back(request);
        } else {
          ungranted_requests.push_back(request);
        }
      }
      for (auto ungranted : ungranted_requests) {
        for (auto granted : granted_requests) {
          if (!AreLocksCompatible(granted->lock_mode_, ungranted->lock_mode_)) {
            AddEdge(ungranted->txn_id_, granted->txn_id_);
          }
        }
      }
    };

    for (const auto &[_, lock_request_queue_ptr] : table_lock_map_) {
      f_add_edges(lock_request_queue_ptr);
    }
    for (const auto &[_, lock_request_queue_ptr] : row_lock_map_) {
      f_add_edges(lock_request_queue_ptr);
    }

    row_latch.unlock();
    table_latch.unlock();

    auto txn_id = txn_id_t{};
    while (HasCycle(&txn_id)) {
      for (auto waiting_txn_id : waits_for_[txn_id]) {
        RemoveEdge(waiting_txn_id, txn_id);
      }
      auto txn = txn_manager_->GetTransaction(txn_id);
      txn_manager_->Abort(txn);
    }

    table_latch.lock();
    row_latch.lock();
    for (const auto &[_, lock_request_queue_ptr] : table_lock_map_) {
      lock_request_queue_ptr->cv_.notify_all();
    }
    for (const auto &[_, lock_request_queue_ptr] : row_lock_map_) {
      lock_request_queue_ptr->cv_.notify_all();
    }
  }
}

}  // namespace bustub
