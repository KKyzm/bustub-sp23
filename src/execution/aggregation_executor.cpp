//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>
#include "storage/table/tuple.h"

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_executor_->Init();

  Tuple child_tuple{};
  RID child_tuple_rid{};
  aht_.Clear();
  while (child_executor_->Next(&child_tuple, &child_tuple_rid)) {
    std::vector<Value> elems_group_by{};
    std::vector<Value> elems_aggr{};
    for (const auto &expr : plan_->GetGroupBys()) {
      elems_group_by.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }
    for (const auto &expr : plan_->GetAggregates()) {
      elems_aggr.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }

    aht_.InsertCombine({elems_group_by}, {elems_aggr});
  }
  aht_iterator_ = aht_.Begin();
  is_executed_ = false;
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    if (!is_executed_) {
      if (plan_->GetGroupBys().empty()) {
        *tuple = Tuple{aht_.GenerateInitialAggregateValue().aggregates_, &plan_->OutputSchema()};
        is_executed_ = true;
        return true;
      }
    }
    return false;
  }

  is_executed_ = true;
  std::vector<Value> values = aht_iterator_.Key().group_bys_;
  const auto &aggregates = aht_iterator_.Val().aggregates_;
  values.insert(values.end(), aggregates.begin(), aggregates.end());
  ++aht_iterator_;

  *tuple = Tuple{values, &plan_->OutputSchema()};
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
