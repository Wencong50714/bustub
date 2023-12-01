//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  table_iter_ = std::make_unique<TableIterator>(table_info->table_->MakeIterator());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  // Loop until a valid tuple is found or end of table is reached
  while (!table_iter_->IsEnd()) {
    auto [metadata, tuple_data] = table_iter_->GetTuple();

    // If the tuple is not deleted, set the output tuple and RID, then return true
    if (!metadata.is_deleted_) {
      *tuple = Tuple(tuple_data);
      *rid = table_iter_->GetRID();
      ++(*table_iter_);
      return true;
    }

    ++(*table_iter_);
  }
  return false;
}


}  // namespace bustub
