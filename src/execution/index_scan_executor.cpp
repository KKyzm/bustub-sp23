//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  index_info_ = GetExecutorContext()->GetCatalog()->GetIndex(plan_->GetIndexOid());
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(index_info_->table_name_);
  tree_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get());
  index_iterator_ = tree_->GetBeginIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    if (index_iterator_.IsEnd()) {
      return false;
    }
    auto next_rid = (*index_iterator_).second;
    auto next_tuple_with_metadata = table_info_->table_->GetTuple(next_rid);

    ++index_iterator_;
    if (!next_tuple_with_metadata.first.is_deleted_) {
      *tuple = next_tuple_with_metadata.second;
      *rid = next_rid;
      return true;
    }
  }
}

}  // namespace bustub
