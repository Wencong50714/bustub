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

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();

  Tuple child_tuple{};
  RID rid{};

  // Build hash table by right key
  while (right_child_->Next(&child_tuple, &rid)) {
    HJKey keys = MakeHJKey(&child_tuple, false);
    hj_table_[keys].push_back(child_tuple);
  }

  while (left_child_->Next(&child_tuple, &rid)) {
    HJKey keys = MakeHJKey(&child_tuple, true);

    bool is_joined = false;
    if (hj_table_.find(keys) != hj_table_.end()) {
      for (const auto &right_tuple : hj_table_[keys]) {
        if (IsEqui(&child_tuple, &right_tuple)) {
          is_joined = true;
          auto values = CombineTwoTuples(&child_tuple, &right_tuple);
          result_tuples_.emplace_back(values, &GetOutputSchema());
        }
      }
    }

    if (!is_joined && plan_->GetJoinType() == JoinType::LEFT) {
      auto values = CombineTwoTuples(&child_tuple, nullptr);
      result_tuples_.emplace_back(values, &GetOutputSchema());
    }
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (it_ == result_tuples_.size()) {
    return false;
  }

  *tuple = result_tuples_[it_++];
  return true;
}

}  // namespace bustub
