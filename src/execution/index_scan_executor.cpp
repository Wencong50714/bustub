//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  auto *index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info->index_.get());

  Value v = plan_->pred_key_->val_;
  std::vector<Value> values{v};
  htable_->ScanKey(Tuple(values, index_info->index_->GetKeySchema()), &rids_, exec_ctx_->GetTransaction());
  rid_iter_ = rids_.begin();  // Get the result in rids_ for later use
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);

  while (rid_iter_ != rids_.end()) {
    auto [metadata, tuple_data] = table_info->table_->GetTuple(*rid_iter_);
    auto value = plan_->filter_predicate_->Evaluate(&tuple_data, GetOutputSchema());

    if (!metadata.is_deleted_ && !value.IsNull() && value.GetAs<bool>()) {
      *tuple = Tuple(tuple_data);
      *rid = *rid_iter_;
      ++rid_iter_;
      return true;
    }

    ++rid_iter_;
  }
  return false;
}

}  // namespace bustub
