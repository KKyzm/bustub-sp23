//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <cstdint>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_executor_(std::move(left_executor)),
      right_child_executor_(std::move(right_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_child_executor_->Init();
  right_child_executor_->Init();

  RID tmp_rid;
  left_executor_status_ = left_child_executor_->Next(&left_tuple_, &tmp_rid);
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    if (!left_executor_status_) {
      return false;
    }

    Tuple right_tuple;
    RID tmp_rid;
    // for current left tuple, iterate through right tuples to find a joinable pair
    while (right_child_executor_->Next(&right_tuple, &tmp_rid)) {
      if (CouldJoin(&left_tuple_, &right_tuple)) {
        std::vector<Value> values;
        for (uint32_t i = 0; i < plan_->GetLeftPlan()->OutputSchema().GetColumnCount(); i++) {
          values.push_back(left_tuple_.GetValue(&plan_->GetLeftPlan()->OutputSchema(), i));
        }
        for (uint32_t i = 0; i < plan_->GetRightPlan()->OutputSchema().GetColumnCount(); i++) {
          values.push_back(right_tuple.GetValue(&plan_->GetRightPlan()->OutputSchema(), i));
        }
        *tuple = Tuple{values, &GetOutputSchema()};

        current_left_tuple_matched_ = true;
        return true;
      }
    }

    // reset right tuples' iterator to beginning
    right_child_executor_->Init();

    // if current left tuple has never been joined, and join type is INNER,
    // then a right tuple with all NULL values should be joined and emited
    if (!current_left_tuple_matched_ && plan_->GetJoinType() != JoinType::INNER) {
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
    }

    left_executor_status_ = left_child_executor_->Next(&left_tuple_, &tmp_rid);
    current_left_tuple_matched_ = false;
  }
}

auto NestedLoopJoinExecutor::CouldJoin(const Tuple *left_tuple, const Tuple *right_tuple) -> bool {
  Value result = plan_->Predicate()->EvaluateJoin(left_tuple, plan_->GetLeftPlan()->OutputSchema(), right_tuple,
                                                  plan_->GetRightPlan()->OutputSchema());
  BUSTUB_ASSERT(result.GetTypeId() == BOOLEAN, "Does EvaluateJoin() return a Boolean value?");
  return result.CompareEquals(ValueFactory::GetBooleanValue(true)) == CmpBool::CmpTrue;
}

}  // namespace bustub
