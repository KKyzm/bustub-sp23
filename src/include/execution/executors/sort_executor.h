//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_executor.h
//
// Identification: src/include/execution/executors/sort_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "binder/bound_order_by.h"
#include "catalog/schema.h"
#include "common/macros.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"
#include "type/type.h"

namespace bustub {
struct OrderByComparator {
  OrderByComparator(const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_bys,
                    const Schema *tuple_schema)
      : order_bys_(order_bys), tuple_schema_(tuple_schema) {}

  auto operator()(const Tuple &lhs, const Tuple &rhs) -> bool {
    for (const auto &[order_by_type, order_by_expr] : order_bys_) {
      if (order_by_type == OrderByType::INVALID) {
        continue;
      }

      Value lhs_val = order_by_expr->Evaluate(&lhs, *tuple_schema_);
      Value rhs_val = order_by_expr->Evaluate(&rhs, *tuple_schema_);
      BUSTUB_ASSERT(lhs_val.CheckComparable(rhs_val), "Values in OrderByComparator should be comparable.");

      if (lhs_val.CompareEquals(rhs_val) == CmpBool::CmpTrue) {
        continue;
      }
      if (order_by_type == OrderByType::ASC || order_by_type == OrderByType::DEFAULT) {
        return lhs_val.CompareLessThan(rhs_val) == CmpBool::CmpTrue;
      }
      BUSTUB_ASSERT(order_by_type == OrderByType::DESC, "Order by type could only be DESC at this point.");
      return lhs_val.CompareGreaterThan(rhs_val) == CmpBool::CmpTrue;
    }

    return true;
  }

  const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_bys_;
  const Schema *tuple_schema_;
};

/**
 * The SortExecutor executor executes a sort.
 */
class SortExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SortExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sort plan to be executed
   */
  SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the sort */
  void Init() override;

  /**
   * Yield the next tuple from the sort.
   * @param[out] tuple The next tuple produced by the sort
   * @param[out] rid The next tuple RID produced by the sort
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;
  /** The child executor to obtain value from */
  std::unique_ptr<AbstractExecutor> child_executor_;

  std::vector<Tuple> tuples_;

  std::vector<Tuple>::const_iterator tuples_iterator_;
};
}  // namespace bustub
