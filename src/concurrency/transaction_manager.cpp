//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "storage/table/table_heap.h"
namespace bustub {

void TransactionManager::Commit(Transaction *txn) {
  // Release all the locks.
  ReleaseLocks(txn);

  txn->SetState(TransactionState::COMMITTED);
}

void TransactionManager::Abort(Transaction *txn) {
  // NOTE: only consider INSERT and DELETE operation here
  auto table_write_set = txn->GetWriteSet();
  for (const auto &table_write_record : *table_write_set) {
    auto meta = table_write_record.table_heap_->GetTupleMeta(table_write_record.rid_);
    meta.is_deleted_ = !meta.is_deleted_;
    table_write_record.table_heap_->UpdateTupleMeta(meta, table_write_record.rid_);
  }

  auto index_write_set = txn->GetIndexWriteSet();
  for (const auto &index_write_record : *index_write_set) {
    auto index_info = index_write_record.catalog_->GetIndex(index_write_record.index_oid_);
    if (index_write_record.wtype_ == WType::DELETE) {
      index_info->index_->InsertEntry(index_write_record.tuple_, index_write_record.rid_, nullptr);
    } else if (index_write_record.wtype_ == WType::INSERT) {
      index_info->index_->DeleteEntry(index_write_record.tuple_, index_write_record.rid_, nullptr);
    }
  }

  ReleaseLocks(txn);

  txn->SetState(TransactionState::ABORTED);
}

void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub
