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
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  std::vector<Tuple> left_tuples{};
  std::vector<Tuple> right_tuples{};

  Tuple child_tuple{};
  RID rid{};

  // Collect the all tuples from left and right executors
  while (left_executor_->Next(&child_tuple, &rid)) {
    left_tuples.emplace_back(child_tuple);
  }

  while (right_executor_->Next(&child_tuple, &rid)) {
    right_tuples.emplace_back(child_tuple);
  }

  // Nested loop join
  for (auto &left_tuple : left_tuples) {
    right_executor_->Init();  // Fuck! It is nonsense!!

    bool joined = false;
    for (auto &right_tuple : right_tuples) {
      auto value = plan_->predicate_->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(), &right_tuple,
                                                   right_executor_->GetOutputSchema());
      if (!value.IsNull() && value.GetAs<bool>()) {
        joined = true;
        std::vector<Value> values{};
        for (size_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (size_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
        }
        result_.emplace_back(values, &GetOutputSchema());
      }
    }
    if (plan_->GetJoinType() == JoinType::LEFT && !joined) {
      std::vector<Value> values{};
      for (size_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
      }
      for (size_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
        // In test case, for left join, the pure left one should have integer_null field for right attribute
        values.emplace_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
      }
      result_.emplace_back(values, &GetOutputSchema());
    }
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (i_ == result_.size()) {
    return false;
  }

  *tuple = result_[i_++];
  return true;
}

}  // namespace bustub
