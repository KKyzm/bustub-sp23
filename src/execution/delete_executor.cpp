//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
  table_indexes_ = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
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

    count++;
  }

  *tuple = Tuple({{plan_->OutputSchema().GetColumn(0).GetType(), count}}, &plan_->OutputSchema());

  return true;
}

}  // namespace bustub
