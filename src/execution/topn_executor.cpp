#include "execution/executors/topn_executor.h"
#include <algorithm>
#include "common/macros.h"
#include "execution/executors/sort_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();

  top_entries_.clear();

  Tuple tuple;
  RID tmp_rid;

  OrderByComparator front_of{plan_->GetOrderBy(), &child_executor_->GetOutputSchema()};

  // make a min heap
  while (child_executor_->Next(&tuple, &tmp_rid)) {
    if (top_entries_.size() < plan_->GetN()) {
      top_entries_.push_back(tuple);
      std::push_heap(top_entries_.begin(), top_entries_.end(), front_of);
    } else if (front_of(tuple, top_entries_[0])) {
      std::pop_heap(top_entries_.begin(), top_entries_.end(), front_of);
      top_entries_.pop_back();
      top_entries_.push_back(tuple);
      std::push_heap(top_entries_.begin(), top_entries_.end(), front_of);
    }
  }

  BUSTUB_ASSERT(top_entries_.size() <= plan_->GetN(), "Size of top_entries_ should NOT exceed N.");

  auto back_of = [&](const Tuple &lhs, const Tuple &rhs) -> bool { return !front_of(lhs, rhs); };

  std::make_heap(top_entries_.begin(), top_entries_.end(), back_of);
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (top_entries_.empty()) {
    return false;
  }

  OrderByComparator front_of{plan_->GetOrderBy(), &child_executor_->GetOutputSchema()};
  auto back_of = [&](const Tuple &lhs, const Tuple &rhs) -> bool { return !front_of(lhs, rhs); };
  std::pop_heap(top_entries_.begin(), top_entries_.end(), back_of);
  *tuple = top_entries_.back();
  top_entries_.pop_back();
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return top_entries_.size(); };

}  // namespace bustub
