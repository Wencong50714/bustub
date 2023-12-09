#include "execution/expressions/column_value_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeMergeFilterScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);

    if (seq_scan.filter_predicate_ == nullptr) {
      return optimized_plan;
    }

    const auto *table_info = catalog_.GetTable(seq_scan.GetTableOid());
    const auto indices = catalog_.GetTableIndexes(table_info->name_);

    const auto *cv_expr = dynamic_cast<ColumnValueExpression *>(seq_scan.filter_predicate_.get()->children_[0].get());
    if (cv_expr == nullptr) {
      return optimized_plan;
    }

    // check the filtering columns from the predicate
    auto column_idx = cv_expr->GetColIdx();
    for (const auto *index : indices) {
      const auto &columns = index->key_schema_.GetColumns();

      for (const auto& column : columns) {
        if (column.GetName() == table_info->schema_.GetColumn(column_idx).GetName()) {
          const auto const_expr = dynamic_cast<ConstantValueExpression *>(seq_scan.filter_predicate_.get()->children_[1].get());
          return std::make_shared<IndexScanPlanNode>(optimized_plan->output_schema_, table_info->oid_,
                                                     index->index_oid_, seq_scan.filter_predicate_, const_expr);
        }
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
