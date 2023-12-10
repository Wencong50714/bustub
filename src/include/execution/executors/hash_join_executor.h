//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** @return whether two tuple is equal in the predicate */
  auto IsEqui(const Tuple *left, const Tuple *right) -> bool {
    for (size_t i = 0; i < plan_->LeftJoinKeyExpressions().size(); i++) {
      Value left_v = plan_->LeftJoinKeyExpressions()[i]->Evaluate(left, left_child_->GetOutputSchema());
      Value right_v = plan_->RightJoinKeyExpressions()[i]->Evaluate(right, right_child_->GetOutputSchema());

      if (left_v.CompareEquals(right_v) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }

  auto CombineTwoTuples(const Tuple *left, const Tuple *right) -> std::vector<Value> {
    std::vector<Value> ret{};
    for (size_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
      ret.emplace_back(left->GetValue(&left_child_->GetOutputSchema(), i));
    }

    if (right == nullptr) {
      for (size_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
        ret.emplace_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));  // In test case, return integer_null
      }
    } else {
      for (size_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
        ret.emplace_back(right->GetValue(&right_child_->GetOutputSchema(), i));
      }
    }

    return ret;
  }

  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  std::unordered_map<hash_t, std::vector<Tuple>> hj_table_{};

  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;

  std::vector<Tuple> result_tuples_{};
  size_t it_{};
};

}  // namespace bustub
