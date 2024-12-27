//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdint>
#include <memory>
#include <vector>
#include "common/config.h"
#include "concurrency/transaction.h"
#include "storage/table/tuple.h"

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  auto txn = GetExecutorContext()->GetTransaction();
  auto lock_mgr = GetExecutorContext()->GetLockManager();
  auto table_oid = plan_->TableOid();
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
  table_indexes_ = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_);

  bool already_locked = txn->IsTableExclusiveLocked(table_oid) ||
                        txn->IsTableSharedIntentionExclusiveLocked(table_oid) ||
                        txn->IsTableIntentionExclusiveLocked(table_oid);
  if (!already_locked) {
    bool success = lock_mgr->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, table_oid);
    if (!success) {
      throw ExecutionException("Failed to lock table in INTENTION_EXCLUSIVE mode.");
    }
    child_executor_->Init();
  }
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_executed_) {
    return false;
  }

  is_executed_ = true;

  Tuple child_tuple{};
  RID child_tuple_rid{};

  int64_t count = 0;
  while (true) {
    const auto status = child_executor_->Next(&child_tuple, &child_tuple_rid);

    if (!status) {
      break;
    }

    const TupleMeta mark_as_new_inserted = {
        .insert_txn_id_ = INVALID_TXN_ID,
        .delete_txn_id_ = INVALID_TXN_ID,
        .is_deleted_ = false,
    };
    const auto rid_inserted =
        table_info_->table_->InsertTuple(mark_as_new_inserted, child_tuple, GetExecutorContext()->GetLockManager(),
                                         GetExecutorContext()->GetTransaction(), plan_->TableOid());
    if (!rid_inserted.has_value()) {
      throw ExecutionException("Failed to insert tuple into table heap.");
    }
    // maintain table write set
    auto table_write_record = TableWriteRecord{
        plan_->TableOid(),
        rid_inserted.value(),
        table_info_->table_.get(),
    };
    GetExecutorContext()->GetTransaction()->AppendTableWriteRecord(table_write_record);

    // update indexes
    for (const auto index_info : table_indexes_) {
      std::vector<Value> values_of_key_attrs{};
      for (const auto i : index_info->index_->GetKeyAttrs()) {
        values_of_key_attrs.push_back(child_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
      }
      Tuple tuple_of_key_attrs = {values_of_key_attrs, index_info->index_->GetKeySchema()};

      const auto status = index_info->index_->InsertEntry(tuple_of_key_attrs, rid_inserted.value(), nullptr);
      if (!status) {
        throw ExecutionException("Failed to insert entry into index.");
      }
      // maintain index write set
      auto index_write_record = IndexWriteRecord{
          rid_inserted.value(), plan_->TableOid(),      WType::INSERT,
          tuple_of_key_attrs,   index_info->index_oid_, GetExecutorContext()->GetCatalog(),
      };
      GetExecutorContext()->GetTransaction()->AppendIndexWriteRecord(index_write_record);
    }

    count++;
  }

  *tuple = Tuple({{plan_->OutputSchema().GetColumn(0).GetType(), count}}, &plan_->OutputSchema());

  return true;
}

}  // namespace bustub
