//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();

  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::SNAPSHOT_ISOLATION) {
    ts_ = exec_ctx_->GetTransaction()->GetReadTs();
    txn_id_ = exec_ctx_->GetTransaction()->GetTransactionId();
    txn_mgr_ = exec_ctx_->GetTransactionManager();
  }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }

  Tuple to_insert_tuple{};
  int cnt = 0;
  while (child_executor_->Next(&to_insert_tuple, rid)) {
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::SNAPSHOT_ISOLATION) {
      if (!table_indexes_.empty()) {
        // If table have index
        BUSTUB_ASSERT(table_indexes_.size() == 1, "Only support primary key");
        auto primary_index = table_indexes_[0];
        BUSTUB_ASSERT(primary_index->is_primary_key_,
                      "In the case that db only contain one index, it must be primary index");

        std::vector<RID> rids{};
        primary_index->index_->ScanKey(to_insert_tuple.KeyFromTuple(table_info_->schema_, primary_index->key_schema_,
                                                                    primary_index->index_->GetKeyAttrs()),
                                       &rids, exec_ctx_->GetTransaction());

        if (!rids.empty()) {
          BUSTUB_ASSERT(rids.size() == 1, "Should only scan 1 rid, since we always update in place");

          auto r = rids[0];
          auto meta = table_info_->table_->GetTuple(r).first;

          // check write-write conflict
          if (!meta.is_deleted_ || (meta.ts_ >= TXN_START_ID && meta.ts_ != txn_id_) ||
              (meta.ts_ < TXN_START_ID && meta.ts_ > ts_)) {
            exec_ctx_->GetTransaction()->SetTainted();
            throw ExecutionException("insert: write-write conflict meta");
          }

          std::vector<bool> mf(child_executor_->GetOutputSchema().GetColumns().size(), true);
          auto new_undo_log = UndoLog{true, mf, {}, meta.ts_};

          if (meta.ts_ == txn_id_) {
            auto ver_link_op = txn_mgr_->GetVersionLink(r);
            if (ver_link_op.has_value()) {
              auto undo_link = ver_link_op.value().prev_;
              auto undo_log = txn_mgr_->GetUndoLog(undo_link);
              new_undo_log = OverlayUndoLog(new_undo_log, undo_log, &child_executor_->GetOutputSchema());
              exec_ctx_->GetTransaction()->ModifyUndoLog(undo_link.prev_log_idx_, new_undo_log);
            }

            TupleMeta to_insert_meta{txn_id_, false};
            table_info_->table_->UpdateTupleInPlace(to_insert_meta, to_insert_tuple, r, nullptr);
            // modify itself, don't need to add write set
            continue;
          }

          auto ver_link_op = txn_mgr_->GetVersionLink(r);
          if (ver_link_op.has_value()) {
            auto ver_link = ver_link_op.value();
            if (ver_link.in_progress_) {
              exec_ctx_->GetTransaction()->SetTainted();
              throw ExecutionException("insert: write-write conflict in progress");
            }

            // set ver_link in_progress to true
            ver_link.in_progress_ = true;
            txn_mgr_->UpdateVersionLink(r, ver_link, nullptr);

            // link undo log to version chain
            auto undo_link = ver_link.prev_;
            auto undo_log = txn_mgr_->GetUndoLog(undo_link);
            new_undo_log.prev_version_ = undo_link;
            ver_link.prev_ = exec_ctx_->GetTransaction()->AppendUndoLog(new_undo_log);

            // update table heap content
            TupleMeta to_insert_meta{txn_id_, false};
            table_info_->table_->UpdateTupleInPlace(to_insert_meta, to_insert_tuple, r, nullptr);

            // set in_progress back to false
            ver_link.in_progress_ = false;
            txn_mgr_->UpdateVersionLink(r, ver_link, nullptr);
          } else {
            // create a placeholder version link
            auto ver_link = VersionUndoLink{{}, true};
            txn_mgr_->UpdateVersionLink(r, ver_link, nullptr);

            // detect write-write conflict
            if (!meta.is_deleted_ || (meta.ts_ >= TXN_START_ID) || (meta.ts_ < TXN_START_ID && meta.ts_ > ts_)) {
              exec_ctx_->GetTransaction()->SetTainted();
              throw ExecutionException("insert: write-write conflict meta");
            }

            // link undo log to version chain
            ver_link.prev_ = exec_ctx_->GetTransaction()->AppendUndoLog(new_undo_log);

            // update tuple heap contents
            TupleMeta to_insert_meta{txn_id_, false};
            table_info_->table_->UpdateTupleInPlace(to_insert_meta, to_insert_tuple, r, nullptr);

            // set in_progress back to false
            ver_link.in_progress_ = false;
            txn_mgr_->UpdateVersionLink(r, ver_link, nullptr);
          }

          // Step4: add write set
          exec_ctx_->GetTransaction()->AppendWriteSet(plan_->table_oid_, r);
          continue;
        }

        // create a tuple on the table heap with a transaction temporary timestamp
        TupleMeta meta{txn_id_, false};
        auto new_rid = table_info_->table_->InsertTuple(meta, to_insert_tuple, exec_ctx_->GetLockManager(),
                                                        exec_ctx_->GetTransaction(), plan_->table_oid_);
        BUSTUB_ASSERT(new_rid.has_value(), "New allocated tuple must have valid rid");

        // insert index entry
        if (!primary_index->index_->InsertEntry(
                to_insert_tuple.KeyFromTuple(table_info_->schema_, primary_index->key_schema_,
                                             primary_index->index_->GetKeyAttrs()),
                *new_rid, exec_ctx_->GetTransaction())) {
          // unique key constraint is violated
          exec_ctx_->GetTransaction()->SetTainted();
          throw ExecutionException("write-write conflict: already have index");
        }

        exec_ctx_->GetTransaction()->AppendWriteSet(plan_->table_oid_, *new_rid);  // append write set
        txn_mgr_->UpdateUndoLink(*new_rid, std::nullopt, nullptr);
      } else {
        // There is no index
        TupleMeta meta{txn_id_, false};
        auto new_rid = table_info_->table_->InsertTuple(meta, to_insert_tuple, exec_ctx_->GetLockManager(),
                                                        exec_ctx_->GetTransaction(), plan_->table_oid_);
        BUSTUB_ASSERT(new_rid.has_value(), "New allocated tuple must have valid rid");

        exec_ctx_->GetTransaction()->AppendWriteSet(plan_->table_oid_, *new_rid);  // append write set
        txn_mgr_->UpdateUndoLink(*new_rid, std::nullopt, nullptr);
      }
    } else {
      TupleMeta meta{INVALID_TXN_ID, false};
      auto new_rid = table_info_->table_->InsertTuple(meta, to_insert_tuple, exec_ctx_->GetLockManager(),
                                                      exec_ctx_->GetTransaction(), plan_->table_oid_);
      BUSTUB_ASSERT(new_rid.has_value(), "New allocated tuple must have valid rid");

      for (auto index : table_indexes_) {
        index->index_->InsertEntry(
            to_insert_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()),
            *new_rid, exec_ctx_->GetTransaction());
      }
    }
    cnt++;
  }

  // prepare thr return values
  std::vector<Value> values;
  values.emplace_back(INTEGER, cnt);
  *tuple = Tuple(values, &GetOutputSchema());
  is_end_ = true;
  return true;
}

}  // namespace bustub
