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
    txn_id_ = exec_ctx_->GetTransaction()->GetTransactionId();
    txn_mgr_ = exec_ctx_->GetTransactionManager();
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::SNAPSHOT_ISOLATION) {
    while (!table_iter_->IsEnd()) {
      *rid = table_iter_->GetRID();
      std::vector<UndoLog> undo_logs{};
      bool find_end = false;

      auto [metadata, tuple_data] = table_iter_->GetTuple();
      if (metadata.ts_ == txn_id_ || metadata.ts_ <= ts_) {
        printf("DEBUG: directly return uncommitted tuple\n");
        if (!metadata.is_deleted_) {
          *tuple = Tuple(tuple_data);
          ++(*table_iter_);
          return true;
        }
        ++(*table_iter_);
        continue;
      }

      auto undo_link = txn_mgr_->GetUndoLink(table_iter_->GetRID()).value();
      while (undo_link.IsValid()) {
        auto undo_log = txn_mgr_->GetUndoLog(undo_link);
        printf("DEBUG: log ts = %lld, ts = %lld\n", undo_log.ts_, ts_);
        undo_logs.push_back(undo_log);
        if (ts_ >= undo_log.ts_) {
          printf("DEBUG: Find end\n");
          find_end = true;
          break;
        }
        undo_link = undo_log.prev_version_;
      }

      printf("DEBUG: undo logs collect finished\n");
      auto op_tuple = ReconstructTuple(&GetOutputSchema(), Tuple(tuple_data), metadata, undo_logs);
      if (find_end && op_tuple != std::nullopt) {
        *tuple = op_tuple.value();
        ++(*table_iter_);
        return true;
      }

      ++(*table_iter_);
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
