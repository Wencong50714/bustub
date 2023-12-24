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
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  table_iter_ = std::make_unique<TableIterator>(table_info->table_->MakeIterator());

  if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::SNAPSHOT_ISOLATION) {
    ts_ = exec_ctx_->GetTransaction()->GetReadTs();
    txn_mgr_ = exec_ctx_->GetTransactionManager();
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::SNAPSHOT_ISOLATION) {
    while (!table_iter_->IsEnd()) {
      auto [metadata, tuple_data] = table_iter_->GetTuple();

      std::vector<UndoLog> undo_logs{};
      bool find_end = false;

      auto undo_link = txn_mgr_->GetUndoLink(table_iter_->GetRID()).value();
      while (undo_link.IsValid()) {
        auto undo_log = txn_mgr_->GetUndoLog(undo_link);

        if (undo_log.ts_ >= ts_) {
          undo_logs.push_back(undo_log);
          find_end = (undo_log.ts_ == ts_);
        } else {
          find_end = true;
        }

        undo_link = undo_log.prev_version_;
      }

      if (!find_end) {
        // Collect next tuple
        ++(*table_iter_);
        continue;
      }

      auto op_tuple = ReconstructTuple(&GetOutputSchema(), Tuple(tuple_data), metadata, undo_logs);
      if (op_tuple == std::nullopt) {
        return false;
      }

      *tuple = op_tuple.value();
      *rid = table_iter_->GetRID();
      ++(*table_iter_);
      return true;
    }

    return false;
  }

  while (!table_iter_->IsEnd()) {
    auto [metadata, tuple_data] = table_iter_->GetTuple();

    // Check for non-deleted tuples
    if (!metadata.is_deleted_) {
      bool is_valid_tuple = true;

      // If seq scan node have predicate, should filter
      if (plan_->filter_predicate_ != nullptr) {
        auto value = plan_->filter_predicate_->Evaluate(&tuple_data, GetOutputSchema());
        is_valid_tuple = !value.IsNull() && value.GetAs<bool>();
      }

      if (is_valid_tuple) {
        *tuple = Tuple(tuple_data);
        *rid = table_iter_->GetRID();
        ++(*table_iter_);
        return true;
      }
    }

    ++(*table_iter_);
  }
  return false;
}

}  // namespace bustub
