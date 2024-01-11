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
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

auto IndexScanExecutor::CollectUndoLogs(std::vector<UndoLog> &undo_logs, RID rid) -> bool {
  bool find_end = false;
  // collect undo logs
  auto undo_link = txn_mgr_->GetUndoLink(rid).value();
  while (undo_link.IsValid()) {
    auto undo_log_op = txn_mgr_->GetUndoLogOptional(undo_link);
    if (!undo_log_op.has_value()) {
      break;
    }

    auto undo_log = undo_log_op.value();

    undo_logs.push_back(undo_log);
    if (ts_ >= undo_log.ts_) {
      find_end = true;
      break;
    }
    undo_link = undo_log.prev_version_;
  }

  return find_end;
}

IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  auto *index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info->index_.get());

  const auto *right_expr =
      dynamic_cast<const ConstantValueExpression *>(plan_->filter_predicate_->children_[1].get());
  Value v = right_expr->val_;
  std::vector<Value> values = {v};
  htable_->ScanKey(Tuple(values, index_info->index_->GetKeySchema()), &rids_, exec_ctx_->GetTransaction());
  rid_iter_ = rids_.begin();  // Get the result in rids_ for later use

  if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::SNAPSHOT_ISOLATION) {
    ts_ = exec_ctx_->GetTransaction()->GetReadTs();
    txn_id_ = exec_ctx_->GetTransaction()->GetTransactionId();
    txn_mgr_ = exec_ctx_->GetTransactionManager();
  }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);

  if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::SNAPSHOT_ISOLATION) {
    for (; rid_iter_ != rids_.end(); ++rid_iter_) {
      *rid = *rid_iter_;
      auto [metadata, tuple_data] = table_info->table_->GetTuple(*rid_iter_);
      auto pred = plan_->filter_predicate_ == nullptr ||
                  (plan_->filter_predicate_->Evaluate(&tuple_data, GetOutputSchema()).GetAs<bool>());

      // can directly read tuple from tuple heap
      if (metadata.ts_ == txn_id_ || metadata.ts_ <= ts_) {
        if (!metadata.is_deleted_ && pred) {
          *tuple = Tuple(tuple_data);
          ++rid_iter_;
          return true;
        }
        continue;
      }

      if (!txn_mgr_->GetUndoLink(*rid).has_value()) {
        continue;
      }

      // read previous version
      std::vector<UndoLog> undo_logs{};
      bool find_end = CollectUndoLogs(undo_logs, *rid);

      auto op_tuple = ReconstructTuple(&GetOutputSchema(), Tuple(tuple_data), metadata, undo_logs);
      if (find_end && op_tuple != std::nullopt) {
        auto matched_tuple = op_tuple.value();

        if (pred) {
          *tuple = matched_tuple;
          ++rid_iter_;
          return true;
        }
      }
    }
    return false;
  }

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