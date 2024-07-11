#include "execution/executors/sort_executor.h"
#include "binder/bound_order_by.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();

  tuples_.clear();

  Tuple tuple;
  RID tmp_rid;
  while (child_executor_->Next(&tuple, &tmp_rid)) {
    tuples_.push_back(tuple);
  }

  OrderByComparator front_of{plan_->GetOrderBy(), &child_executor_->GetOutputSchema()};
  std::sort(tuples_.begin(), tuples_.end(), front_of);

  tuples_iterator_ = tuples_.cbegin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (tuples_iterator_ == tuples_.cend()) {
    return false;
  }

  *tuple = *tuples_iterator_;
  ++tuples_iterator_;
  return true;
}

}  // namespace bustub
