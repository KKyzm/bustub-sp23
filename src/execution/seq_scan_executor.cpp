//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  auto table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  table_iterator_ = std::make_unique<TableIterator>(table_info->table_->MakeEagerIterator());
  BUSTUB_ASSERT(table_iterator_ != nullptr, "Table iterator should not be nullptr.");
  if (GetExecutorContext()->IsDelete()) {
    // If the current operation is delete (by checking executor context IsDelete(), which will be set to true for DELETE
    // and UPDATE), you should assume all tuples scanned will be deleted
    bool success = GetExecutorContext()->GetLockManager()->LockTable(
        GetExecutorContext()->GetTransaction(), LockManager::LockMode::EXCLUSIVE, plan_->GetTableOid());
    if (!success) {
      throw ExecutionException("Failed to lock table in EXCLUSIVE mode.");
    }
  } else if (GetExecutorContext()->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    bool success = GetExecutorContext()->GetLockManager()->LockTable(
        GetExecutorContext()->GetTransaction(), LockManager::LockMode::INTENTION_SHARED, plan_->GetTableOid());
    if (!success) {
      throw ExecutionException("Failed to lock table in INTENTION_SHARED mode.");
    }
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    if (table_iterator_->IsEnd()) {
      return false;
    }
    auto next_rid = table_iterator_->GetRID();
    auto txn = GetExecutorContext()->GetTransaction();

    // determine lock_mode and whether lock/unlock is necessary
    bool should_lock = false;
    bool should_unlock = false;
    auto lock_mode = LockManager::LockMode::SHARED;
    bool is_delete = GetExecutorContext()->IsDelete();
    auto iso_level = txn->GetIsolationLevel();
    if (iso_level == IsolationLevel::REPEATABLE_READ) {
      should_lock = true;
      should_unlock = false;
    }
    if (iso_level == IsolationLevel::READ_COMMITTED) {
      should_lock = true;
      should_unlock = true;
    }
    if (is_delete) {
      should_lock = true;
      should_unlock = false;
      lock_mode = LockManager::LockMode::EXCLUSIVE;
    }

    if (should_lock) {
      // lock tuple
      bool success = GetExecutorContext()->GetLockManager()->LockRow(txn, lock_mode, plan_->GetTableOid(), next_rid);
      if (!success) {
        throw ExecutionException("Failed to lock row.");
      }
    }

    auto f_unlock_tuple = [&](bool force = false) {
      bool success = GetExecutorContext()->GetLockManager()->UnlockRow(txn, plan_->GetTableOid(), next_rid, force);
      if (!success) {
        throw ExecutionException("Failed to unlock row.");
      }
    };

    auto next_tuple_with_metadata = table_iterator_->GetTuple();
    ++(*table_iterator_);

    // check whether the tuple could be read
    bool is_valid_tuple = false;
    if (!next_tuple_with_metadata.first.is_deleted_) {
      is_valid_tuple = true;
      if (plan_->filter_predicate_ != nullptr) {
        // if you have implemented filter pushdown to scan, check the predicate
        auto value = plan_->filter_predicate_->Evaluate(&next_tuple_with_metadata.second, GetOutputSchema());
        assert(!value.IsNull());
        is_valid_tuple = value.GetAs<bool>();
      }
    }

    if (is_valid_tuple) {
      if (should_unlock) {
        f_unlock_tuple();
      }
      *tuple = next_tuple_with_metadata.second;
      *rid = next_rid;
      return true;
    }
    // else the tuple should not be read, force unlock
    f_unlock_tuple(true);
  }
}

}  // namespace bustub
