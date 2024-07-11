#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    const auto &nlj_predicate = nlj_plan.Predicate();

    std::vector<AbstractExpressionRef> left_key_expressions;
    std::vector<AbstractExpressionRef> right_key_expressions;
    GetKeyExprFromPredicate(nlj_predicate, left_key_expressions, right_key_expressions);
    BUSTUB_ASSERT(left_key_expressions.size() == right_key_expressions.size(),
                  "Length of key expressions should be same.");

    if (!left_key_expressions.empty()) {
      return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                nlj_plan.GetRightPlan(), left_key_expressions, right_key_expressions,
                                                nlj_plan.GetJoinType());
    }
  }

  return optimized_plan;
}

void Optimizer::GetKeyExprFromPredicate(const AbstractExpressionRef &predicate,
                                        std::vector<AbstractExpressionRef> &left_key_expressions,
                                        std::vector<AbstractExpressionRef> &right_key_expressions) {
  auto handle_comparison_expression = [&](const ComparisonExpression *cmp_expr) {
    if (cmp_expr->comp_type_ != ComparisonType::Equal) {
      return;
    }
    BUSTUB_ASSERT(cmp_expr->GetChildren().size() == 2, "ComparisonExpression should have 2 children.");
    auto left_col_expr = dynamic_cast<ColumnValueExpression *>(cmp_expr->GetChildAt(0).get());
    auto right_col_expr = dynamic_cast<ColumnValueExpression *>(cmp_expr->GetChildAt(1).get());
    if (left_col_expr != nullptr && right_col_expr != nullptr) {
      if (left_col_expr->GetTupleIdx() == 0) {
        left_key_expressions.push_back(cmp_expr->GetChildAt(0));
        right_key_expressions.push_back(cmp_expr->GetChildAt(1));
      } else {
        left_key_expressions.push_back(cmp_expr->GetChildAt(1));
        right_key_expressions.push_back(cmp_expr->GetChildAt(0));
      }
    }
  };

  std::function<void(const LogicExpression *)> handle_logic_expression = [&](const LogicExpression *logic_expr) {
    if (logic_expr->logic_type_ != LogicType::And) {
      return;
    }
    BUSTUB_ASSERT(logic_expr->GetChildren().size() == 2, "LogicExpression should have 2 children.");
    auto left_cmp_expr = dynamic_cast<ComparisonExpression *>(logic_expr->GetChildAt(0).get());
    auto right_cmp_expr = dynamic_cast<ComparisonExpression *>(logic_expr->GetChildAt(1).get());
    if (left_cmp_expr != nullptr && right_cmp_expr != nullptr) {
      handle_comparison_expression(left_cmp_expr);
      handle_comparison_expression(right_cmp_expr);
      return;
    }

    BUSTUB_ASSERT(false, "Unimplemented");
    // only if when all logic type is AND, key expressions could be added into vectors
  };

  if (auto cmp_expr = dynamic_cast<ComparisonExpression *>(predicate.get()); cmp_expr != nullptr) {
    handle_comparison_expression(cmp_expr);
    return;
  }

  if (auto logic_expr = dynamic_cast<LogicExpression *>(predicate.get()); logic_expr != nullptr) {
    handle_logic_expression(logic_expr);
  }
}

}  // namespace bustub
