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
  auto txn = GetExecutorContext()->GetTransaction();
  auto lock_mgr = GetExecutorContext()->GetLockManager();
  auto table_oid = plan_->GetTableOid();
  auto table_info = GetExecutorContext()->GetCatalog()->GetTable(table_oid);

  table_iterator_ = std::make_unique<TableIterator>(table_info->table_->MakeEagerIterator());
  BUSTUB_ASSERT(table_iterator_ != nullptr, "Table iterator should not be nullptr.");
  // take a table lock
  if (GetExecutorContext()->IsDelete()) {
    bool already_locked = txn->IsTableExclusiveLocked(table_oid) ||
                          txn->IsTableSharedIntentionExclusiveLocked(table_oid) ||
                          txn->IsTableIntentionExclusiveLocked(table_oid);
    if (!already_locked) {
      bool success = lock_mgr->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, table_oid);
      if (!success) {
        throw ExecutionException("Failed to lock table in INTENTION_EXCLUSIVE mode.");
      }
    }
  } else if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    bool already_locked = txn->IsTableExclusiveLocked(table_oid) ||
                          txn->IsTableSharedIntentionExclusiveLocked(table_oid) ||
                          txn->IsTableIntentionExclusiveLocked(table_oid) || txn->IsTableSharedLocked(table_oid) ||
                          txn->IsTableIntentionSharedLocked(table_oid);

    if (!already_locked) {
      bool success = lock_mgr->LockTable(txn, LockManager::LockMode::INTENTION_SHARED, table_oid);
      if (!success) {
        throw ExecutionException("Failed to lock table in INTENTION_SHARED mode.");
      }
    }
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto txn = GetExecutorContext()->GetTransaction();
  auto lock_mgr = GetExecutorContext()->GetLockManager();
  auto table_oid = plan_->GetTableOid();
  while (true) {
    if (table_iterator_->IsEnd()) {
      return false;
    }
    auto next_rid = table_iterator_->GetRID();

    // determine lock_mode and whether lock/unlock is necessary
    bool should_lock = false;
    bool should_unlock = false;
    auto lock_mode = LockManager::LockMode::SHARED;
    auto iso_level = txn->GetIsolationLevel();
    bool is_delete = GetExecutorContext()->IsDelete();
    bool already_exclusive_locked = txn->IsRowExclusiveLocked(table_oid, next_rid);
    if (iso_level == IsolationLevel::REPEATABLE_READ) {
      should_lock = true;
      should_unlock = false;
    } else if (iso_level == IsolationLevel::READ_COMMITTED) {
      should_lock = true;
      should_unlock = true;
    }
    if (is_delete) {
      should_lock = true;
      should_unlock = false;
      lock_mode = LockManager::LockMode::EXCLUSIVE;
    }
    if (already_exclusive_locked) {
      should_lock = false;
      should_unlock = false;
    }

    if (should_lock) {
      // lock tuple
      bool success = lock_mgr->LockRow(txn, lock_mode, table_oid, next_rid);
      if (!success) {
        throw ExecutionException("Failed to lock row.");
      }
    }

    auto f_unlock_tuple = [&](bool force = false) {
      bool success = lock_mgr->UnlockRow(txn, table_oid, next_rid, force);
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
    if (should_lock) {  // only when the tuple is locked just now.
      f_unlock_tuple(true);
    }
  }
}

}  // namespace bustub
