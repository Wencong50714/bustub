#include "execution/executors/window_function_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
  child_executor_->Init();

  std::vector<Tuple> child_tuples{};
  Tuple tmp_tuple{};
  RID rid;

  while (child_executor_->Next(&tmp_tuple, &rid)) {
    child_tuples.emplace_back(tmp_tuple);
  }

  // 1. sort
  bool sorted_flag = false;
  const auto &order_by = plan_->window_functions_.begin()->second.order_by_;
  if (!order_by.empty()) {
    sorted_flag = true;
    auto cmp = [order_bys = order_by, schema = child_executor_->GetOutputSchema()](const Tuple &a, const Tuple &b) {
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
            if (static_cast<bool>(order_key.second->Evaluate(&a, schema)
                                      .CompareGreaterThan(order_key.second->Evaluate(&b, schema)))) {
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
    std::sort(child_tuples.begin(), child_tuples.end(), cmp);
  }

  // Handle Rank Case specially
  if (plan_->window_functions_.begin()->second.type_ == WindowFunctionType::Rank) {
    Value last_value{};
    int rank = 0;
    int true_rank = 0;

    for (const auto &tuple : child_tuples) {
      true_rank++;
      auto cur_value = tuple.GetValue(&child_executor_->GetOutputSchema(), 0);

      if (last_value.IsNull() || cur_value.CompareEquals(last_value) != CmpBool::CmpTrue) {
        rank = true_rank;
      }

      last_value = cur_value;

      std::vector<Value> ret_values{};
      ret_values.emplace_back(cur_value);
      ret_values.emplace_back(ValueFactory::GetIntegerValue(rank));

      tuples_.emplace_back(ret_values, &GetOutputSchema());
    }
    return;
  }

  // 2. Partition
  std::map<hash_t, std::vector<Tuple>> partition_map{};
  const auto &part = plan_->window_functions_.begin()->second.partition_by_;
  if (!part.empty()) {
    for (const auto &tuple : child_tuples) {
      auto value = part.at(0)->Evaluate(&tuple, child_executor_->GetOutputSchema());
      partition_map[HashUtil::HashValue(&value)].emplace_back(tuple);
    }
  }

  // 3. Iter
  for (const auto &tuple : child_tuples) {
    std::vector<Value> values{};

    for (const auto &win_func : plan_->window_functions_) {
      const std::vector<Tuple> *limited_tuples = &child_tuples;

      Value init_value = GenerateInitialWindowsValue(win_func.second.type_);

      // Determine the scope
      if (!win_func.second.partition_by_.empty()) {
        auto value = part.at(0)->Evaluate(&tuple, child_executor_->GetOutputSchema());
        limited_tuples = &partition_map[HashUtil::HashValue(&value)];
      }

      // combine value
      for (const auto &limited_tuple : *limited_tuples) {
        auto value = win_func.second.function_->Evaluate(&limited_tuple, child_executor_->GetOutputSchema());
        CombineValue(win_func.second.type_, init_value, value);

        if (sorted_flag && limited_tuple.GetRid() == tuple.GetRid()) {
          break;
        }
      }

      values.emplace_back(init_value);
    }
    tuples_.emplace_back(values, &GetOutputSchema());
  }
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (it_ == tuples_.size()) {
    return false;
  }

  *tuple = tuples_[it_++];
  return true;
}
}  // namespace bustub
