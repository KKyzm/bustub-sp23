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
#include "common/macros.h"
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  auto table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  table_iterator_ = std::make_unique<TableIterator>(table_info->table_->MakeIterator());
  BUSTUB_ASSERT(table_iterator_ != nullptr, "Table iterator should not be nullptr.");
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    if (table_iterator_->IsEnd()) {
      return false;
    }
    auto next_rid = table_iterator_->GetRID();
    auto next_tuple_with_metadata = table_iterator_->GetTuple();
    ++(*table_iterator_);
    if (!next_tuple_with_metadata.first.is_deleted_) {
      *tuple = next_tuple_with_metadata.second;
      *rid = next_rid;
      return true;
    }
  }
}

}  // namespace bustub
