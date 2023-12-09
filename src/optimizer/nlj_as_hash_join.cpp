#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
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

auto RecursivePredicate(AbstractExpression *expr, std::vector<AbstractExpressionRef> *left_key_expr,
                        std::vector<AbstractExpressionRef> *right_key_expr) -> bool {
  const auto &cmp_expr = dynamic_cast<const ComparisonExpression*>(expr);

  if (cmp_expr != nullptr && cmp_expr->comp_type_ == ComparisonType::Equal) {
    BUSTUB_ASSERT(cmp_expr->GetChildren().size() == 2, "Cmp expr must have 2 children\n");
    const auto &left_child = dynamic_cast<const ColumnValueExpression &>(*cmp_expr->GetChildAt(0));
    const auto &right_child = dynamic_cast<const ColumnValueExpression &>(*cmp_expr->GetChildAt(1));

    auto left_id = left_child.GetTupleIdx();

    if (left_id == 0) {
      left_key_expr->emplace_back(std::make_shared<ColumnValueExpression>(left_child));
      right_key_expr->emplace_back(std::make_shared<ColumnValueExpression>(right_child));
    } else {
      left_key_expr->emplace_back(std::make_shared<ColumnValueExpression>(right_child));
      right_key_expr->emplace_back(std::make_shared<ColumnValueExpression>(left_child));
    }
    return true;
  }

  const auto &logic_expr = dynamic_cast<const LogicExpression*>(expr);
  if (logic_expr == nullptr || logic_expr->logic_type_ != LogicType::And) {
    return false;
  }

  BUSTUB_ASSERT(logic_expr->GetChildren().size() == 2, "Logic expr must have 2 children\n");
  return RecursivePredicate(logic_expr->GetChildAt(0).get(), left_key_expr, right_key_expr)
      && RecursivePredicate(logic_expr->GetChildAt(1).get(), left_key_expr, right_key_expr);
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);

    if (nlj_plan.predicate_.get() == nullptr) {
      return optimized_plan;
    }

    std::vector<AbstractExpressionRef> left_key_expressions{};
    std::vector<AbstractExpressionRef> right_key_expressions{};

    if (!RecursivePredicate(nlj_plan.predicate_.get(), &left_key_expressions, &right_key_expressions)) {
      return optimized_plan;
    }

    return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
                            left_key_expressions, right_key_expressions, nlj_plan.GetJoinType());
  }
  return optimized_plan;
}

}  // namespace bustub
