#include "execution/executors/sort_executor.h"

namespace bustub {

bool compare(Tuple a, Tuple b) {

  return false;
}

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();

  Tuple child_tuple{};
  RID rid;

  while(child_executor_->Next(&child_tuple, &rid)) {
    tuples_.emplace_back(child_tuple);
  }

  std::sort(
      tuples_.begin(), tuples_.end(),
      [order_bys = plan_->order_bys_, schema = child_executor_->GetOutputSchema()](const Tuple &tuple_a, const Tuple &tuple_b) {
        for (const auto &order_key : order_bys) {
          switch (order_key.first) {
            case OrderByType::INVALID:
            case OrderByType::DEFAULT:
            case OrderByType::ASC:
              if (static_cast<bool>(order_key.second->Evaluate(&tuple_a, schema)
                                        .CompareLessThan(order_key.second->Evaluate(&tuple_b, schema)))) {
                return true;
              } else if (static_cast<bool>(order_key.second->Evaluate(&tuple_a, schema)
                                               .CompareGreaterThan(order_key.second->Evaluate(&tuple_b, schema)))) {
                return false;
              }
              break;
            case OrderByType::DESC:
              if (static_cast<bool>(order_key.second->Evaluate(&tuple_a, schema)
                                        .CompareGreaterThan(order_key.second->Evaluate(&tuple_b, schema)))) {
                return true;
              } else if (static_cast<bool>(order_key.second->Evaluate(&tuple_a, schema)
                                               .CompareLessThan(order_key.second->Evaluate(&tuple_b, schema)))) {
                return false;
              }
              break;
          }
        }
        return false;
      });
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (it_ == tuples_.size()) {
    return false;
  }

  *tuple = tuples_[it_++];
  return true;
}

}  // namespace bustub
