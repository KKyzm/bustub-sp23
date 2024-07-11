//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_executor_(std::move(left_child)),
      right_child_executor_(std::move(right_child)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_child_executor_->Init();
  right_child_executor_->Init();

  Tuple right_tuple;
  RID tmp_rid;

  right_jht_.clear();
  left_executor_status_ = left_child_executor_->Next(&left_tuple_, &tmp_rid);

  while (right_child_executor_->Next(&right_tuple, &tmp_rid)) {
    std::vector<Value> values{};
    for (const auto &expr : plan_->RightJoinKeyExpressions()) {
      values.push_back(expr->Evaluate(&right_tuple, right_child_executor_->GetOutputSchema()));
    }
    right_jht_[{values}].push_back(right_tuple);
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  RID tmp_rid;
  while (true) {
    if (!left_executor_status_) {
      return false;
    }

    // if scanning_tuples_iterator_ is not ready, initialize it through current or next left tuple.
    if (!scanning_tuples_iterator_.has_value()) {
      std::vector<Value> values{};
      for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
        values.push_back(expr->Evaluate(&left_tuple_, left_child_executor_->GetOutputSchema()));
      }
      if (right_jht_.find({values}) != right_jht_.end()) {
        const auto &right_tuples_to_join = right_jht_.at({values});
        scanning_tuples_iterator_ = right_tuples_to_join.cbegin();
        end_of_scanning_tuples_ = right_tuples_to_join.cend();
      } else if (plan_->GetJoinType() != JoinType::INNER) {
        std::vector<Value> values;
        for (uint32_t i = 0; i < plan_->GetLeftPlan()->OutputSchema().GetColumnCount(); i++) {
          values.push_back(left_tuple_.GetValue(&plan_->GetLeftPlan()->OutputSchema(), i));
        }
        for (uint32_t i = 0; i < plan_->GetRightPlan()->OutputSchema().GetColumnCount(); i++) {
          values.push_back(
              ValueFactory::GetNullValueByType(plan_->GetRightPlan()->OutputSchema().GetColumn(i).GetType()));
        }
        *tuple = Tuple{values, &GetOutputSchema()};
        left_executor_status_ = left_child_executor_->Next(&left_tuple_, &tmp_rid);
        return true;
        continue;
      }
    }

    // at this point, both left tuple and scanning_tuples_iterator_ are ready.
    if (scanning_tuples_iterator_ != end_of_scanning_tuples_) {  // find joinable right tuple
      Tuple right_tuple = *(scanning_tuples_iterator_.value());
      std::vector<Value> values;
      for (uint32_t i = 0; i < plan_->GetLeftPlan()->OutputSchema().GetColumnCount(); i++) {
        values.push_back(left_tuple_.GetValue(&plan_->GetLeftPlan()->OutputSchema(), i));
      }
      for (uint32_t i = 0; i < plan_->GetRightPlan()->OutputSchema().GetColumnCount(); i++) {
        values.push_back(right_tuple.GetValue(&plan_->GetRightPlan()->OutputSchema(), i));
      }
      *tuple = Tuple{values, &GetOutputSchema()};

      ++scanning_tuples_iterator_.value();
      return true;
    }

    // reset right tuples' iterator to std::nullopt
    scanning_tuples_iterator_.reset();
    end_of_scanning_tuples_.reset();

    left_executor_status_ = left_child_executor_->Next(&left_tuple_, &tmp_rid);
  }
}

}  // namespace bustub
