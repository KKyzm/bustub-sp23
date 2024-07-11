//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include "storage/table/tuple.h"

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
  table_indexes_ = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
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

    const TupleMeta mark_as_delete = {
        .insert_txn_id_ = INVALID_TXN_ID, .delete_txn_id_ = INVALID_TXN_ID, .is_deleted_ = true};
    table_info_->table_->UpdateTupleMeta(mark_as_delete, child_tuple_rid);

    // update indexes
    for (const auto index_info : table_indexes_) {
      std::vector<Value> values_of_key_attrs{};
      for (const auto i : index_info->index_->GetKeyAttrs()) {
        values_of_key_attrs.push_back(child_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
      }
      Tuple tuple_of_key_attrs = {values_of_key_attrs, index_info->index_->GetKeySchema()};
      index_info->index_->DeleteEntry(tuple_of_key_attrs, child_tuple_rid, nullptr);
    }

    // Compute expressions
    std::vector<Value> values{};
    values.reserve(plan_->target_expressions_.size());
    for (const auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }
    const auto tuple_insert = Tuple{values, &child_executor_->GetOutputSchema()};

    const TupleMeta mark_as_new_inserted = {
        .insert_txn_id_ = INVALID_TXN_ID, .delete_txn_id_ = INVALID_TXN_ID, .is_deleted_ = false};
    const auto rid_inserted = table_info_->table_->InsertTuple(mark_as_new_inserted, tuple_insert);
    if (!rid_inserted.has_value()) {
      throw ExecutionException("Failed to insert tuple into table heap.");
    }

    // update indexes
    for (const auto index_info : table_indexes_) {
      std::vector<Value> values_of_key_attrs{};
      for (const auto i : index_info->index_->GetKeyAttrs()) {
        values_of_key_attrs.push_back(tuple_insert.GetValue(&child_executor_->GetOutputSchema(), i));
      }
      Tuple tuple_of_key_attrs = {values_of_key_attrs, index_info->index_->GetKeySchema()};

      const auto status = index_info->index_->InsertEntry(tuple_of_key_attrs, rid_inserted.value(), nullptr);
      if (!status) {
        throw ExecutionException("Failed to insert entry into index.");
      }
    }

    count++;
  }

  *tuple = Tuple({{plan_->OutputSchema().GetColumn(0).GetType(), count}}, &GetOutputSchema());

  return true;
}

}  // namespace bustub
