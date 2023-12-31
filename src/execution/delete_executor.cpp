//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();

  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::SNAPSHOT_ISOLATION) {
    ts_ = exec_ctx_->GetTransaction()->GetReadTs();
    txn_mgr_ = exec_ctx_->GetTransactionManager();
    txn_id_ = exec_ctx_->GetTransaction()->GetTransactionId();

    // Since update executor is pipeline breaker
    Tuple child_tuple{};
    RID rid{};
    while (child_executor_->Next(&child_tuple, &rid)) {
      rids_.push_back(rid);
    }
  }
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }

  std::vector<Value> values;

  if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::SNAPSHOT_ISOLATION) {
    for (const auto &r : rids_) {
      auto t = table_info_->table_->GetTuple(r);
      auto meta = t.first;
      auto tuple_data = t.second;

      if ((meta.ts_ >= TXN_START_ID && meta.ts_ != txn_id_) || (meta.ts_ < TXN_START_ID && meta.ts_ > ts_)) {
        // Two cases need to be aborted
        exec_ctx_->GetTransaction()->SetTainted();
        throw ExecutionException("write-write conflict");
      }

      // Modify tuple heap with txn
      TupleMeta new_meta{txn_id_, true};
      table_info_->table_->UpdateTupleMeta(new_meta, r);

      auto undo_link_op = txn_mgr_->GetUndoLink(r);
      if (!undo_link_op.has_value() && meta.ts_ == txn_id_) {
        continue; // Directly modify the table heap tuple without generating any undo log
      }

      // Generate undo log
      std::vector<bool> mf(GetOutputSchema().GetColumns().size(), true);
      auto new_undo_log = UndoLog{false, mf, tuple_data, meta.ts_};

      if (undo_link_op.has_value()) {
        auto undo_link = undo_link_op.value();
        auto undo_log = txn_mgr_->GetUndoLog(undo_link);

        if (meta.ts_ == txn_id_) {
          // Update current undo link
          new_undo_log.ts_ = undo_log.ts_; // change ts to old ts
          new_undo_log.prev_version_ = undo_log.prev_version_;
          exec_ctx_->GetTransaction()->ModifyUndoLog(undo_link.prev_log_idx_, new_undo_log);
        } else if (meta.ts_ < TXN_START_ID) {
          // Append new undo link
          new_undo_log.prev_version_ = undo_link;
          auto new_link = exec_ctx_->GetTransaction()->AppendUndoLog(new_undo_log);
          txn_mgr_->UpdateUndoLink(r, new_link, nullptr);
        }
      } else {
        auto new_link = exec_ctx_->GetTransaction()->AppendUndoLog(new_undo_log);
        txn_mgr_->UpdateUndoLink(r, new_link, nullptr);
      }

      // add write set for later use
      exec_ctx_->GetTransaction()->AppendWriteSet(plan_->table_oid_, r);
    }
    values.emplace_back(INTEGER, static_cast<int>(rids_.size()));
  } else {
    Tuple child_tuple{};
    int cnt = 0;
    while (child_executor_->Next(&child_tuple, rid)) {
      TupleMeta meta = {0, true};
      table_info_->table_->UpdateTupleMeta(meta, *rid);

      for (auto index : table_indexes_) {
        index->index_->DeleteEntry(
            child_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()), *rid,
            exec_ctx_->GetTransaction());
      }
      cnt++;
    }
    values.emplace_back(INTEGER, cnt);
  }

  *tuple = Tuple(values, &GetOutputSchema());
  is_end_ = true;
  return true;
}

}  // namespace bustub
