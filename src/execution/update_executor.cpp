//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
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

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }

  std::vector<Value> ret_values;

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

      // Use to construct new tuple contents and undo log tuple
      std::vector<Value> whole_values{};
      std::vector<Value> part_values{};
      std::vector<uint32_t> attrs;
      std::vector<bool> mf{};

      // evaluate the tuple data
      uint32_t i = 0;
      for (const auto &expr : plan_->target_expressions_) {
        auto before = tuple_data.GetValue(&child_executor_->GetOutputSchema(), i);
        auto after = expr->Evaluate(&tuple_data, child_executor_->GetOutputSchema());

        if (before.CompareExactlyEquals(after)) {
          mf.push_back(false);
        } else {
          mf.push_back(true);
          part_values.push_back(before);
          attrs.push_back(i);
        }

        whole_values.push_back(after);
        i++;
      }

      // Update tuple heap
      auto to_update_tuple = Tuple{whole_values, &child_executor_->GetOutputSchema()};
      table_info_->table_->UpdateTupleInPlace({txn_id_, false}, to_update_tuple, r);

      auto undo_link_op = txn_mgr_->GetUndoLink(r);
      if (!undo_link_op.has_value() && meta.ts_ == txn_id_) {
        continue; // Directly modify the table heap tuple without generating any undo log
      }

      // Generate undo log
      Schema s = Schema::CopySchema(&child_executor_->GetOutputSchema(), attrs);
      auto new_undo_log = UndoLog{false, mf, Tuple{part_values, &s}, meta.ts_};

      if (undo_link_op.has_value()) {
        auto undo_link = undo_link_op.value();
        auto undo_log = txn_mgr_->GetUndoLog(undo_link);

        if (meta.ts_ == txn_id_) {
          // TODO: undo log should be a aggravate 叠加 undo log
          printf("DEBUG: SELF MODIFICATION\n");
          new_undo_log.ts_ = ts_;
          new_undo_log.prev_version_ = undo_log.prev_version_;
          exec_ctx_->GetTransaction()->ModifyUndoLog(undo_link.prev_log_idx_, new_undo_log);
        } else if (meta.ts_ < TXN_START_ID) {
          new_undo_log.prev_version_ = undo_link;
          txn_mgr_->UpdateUndoLink(r, exec_ctx_->GetTransaction()->AppendUndoLog(new_undo_log));
        }
      } else {
        txn_mgr_->UpdateUndoLink(r, exec_ctx_->GetTransaction()->AppendUndoLog(new_undo_log));
      }

      // add write set for later use
      exec_ctx_->GetTransaction()->AppendWriteSet(plan_->table_oid_, r);
    }
    ret_values.emplace_back(INTEGER, static_cast<int>(rids_.size()));
  }
  else {
    Tuple child_tuple{};
    int cnt = 0;
    while (child_executor_->Next(&child_tuple, rid)) {
      // delete the current data
      TupleMeta meta = table_info_->table_->GetTupleMeta(*rid);
      meta.is_deleted_ = true;
      table_info_->table_->UpdateTupleMeta(meta, *rid);

      // Insert
      std::vector<Value> values{};
      values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
      // evaluate the tuple data
      for (const auto &expr : plan_->target_expressions_) {
        values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
      }

      auto to_update_tuple = Tuple{values, &child_executor_->GetOutputSchema()};
      TupleMeta metadata{INVALID_TXN_ID, false};  // may need assign value
      auto new_rid = table_info_->table_->InsertTuple(metadata, to_update_tuple, exec_ctx_->GetLockManager(),
                                                      exec_ctx_->GetTransaction(), plan_->table_oid_);

      for (auto index : table_indexes_) {
        index->index_->DeleteEntry(
            child_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()), *rid,
            exec_ctx_->GetTransaction());
        index->index_->InsertEntry(
            to_update_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()),
            *new_rid, exec_ctx_->GetTransaction());
      }
      cnt++;
    }
    ret_values.emplace_back(INTEGER, cnt);
  }

  *tuple = Tuple(ret_values, &GetOutputSchema());
  is_end_ = true;
  return true;
}

}  // namespace bustub
