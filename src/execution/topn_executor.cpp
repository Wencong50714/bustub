#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();

  // Define a less cmp function
  auto cmp = [order_bys = plan_->order_bys_, schema = child_executor_->GetOutputSchema()](const Tuple &a, const Tuple &b) {
    for (const auto &order_key : order_bys) {
      switch (order_key.first) {
        case OrderByType::INVALID:
        case OrderByType::DEFAULT:
        case OrderByType::ASC:
          // For ASC, less one should have higher priority
          if (static_cast<bool>(
                  order_key.second->Evaluate(&a, schema).CompareLessThan(order_key.second->Evaluate(&b, schema)))) {
            return true;
          } else if (static_cast<bool>(order_key.second->Evaluate(&a, schema)
                                           .CompareGreaterThan(order_key.second->Evaluate(&b, schema)))) {
            return false;
          }
          break;
        case OrderByType::DESC:
          // For DESC, greater one should have higher priority
          if (static_cast<bool>(
                  order_key.second->Evaluate(&a, schema).CompareGreaterThan(order_key.second->Evaluate(&b, schema)))) {
            return true;
          } else if (static_cast<bool>(order_key.second->Evaluate(&a, schema)
                                           .CompareLessThan(order_key.second->Evaluate(&b, schema)))) {
            return false;
          }
          break;
      }
    }
    return false;
  };

  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> pq(cmp);

  Tuple child_tuple{};
  RID rid{};

  while (child_executor_->Next(&child_tuple, &rid)) {
    pq.push(child_tuple);

    if (pq.size() == plan_->GetN()) {
      pq.pop();
    } else {
      cnt_++;
    }
  }

  for (size_t i = 0; i < pq.size(); i++) {
    auto t = pq.top();
    pq.pop();
    tuples_stacks_.push(t);
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (tuples_stacks_.empty()) {
    return false;
  }

  *tuple = tuples_stacks_.top();
  tuples_stacks_.pop();
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t {
    return cnt_;
};

}  // namespace bustub
