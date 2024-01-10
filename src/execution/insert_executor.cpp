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
          if (!meta.is_deleted_) {
            exec_ctx_->GetTransaction()->SetTainted();
            throw ExecutionException("insert: write-write conflict meta");
          }

          size_t mf_sz = child_executor_->GetOutputSchema().GetColumns().size();
          UpdateWithVersionLink(r, to_insert_tuple, mf_sz, INSERT_OP, exec_ctx_->GetTransaction(), txn_mgr_,
                                table_info_, &child_executor_->GetOutputSchema(), plan_->table_oid_);
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
