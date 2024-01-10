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

#include "execution/execution_common.h"
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
    printf("DEBUG: Modify the non-primary key\n");
    for (const auto & r : rids_) {
      auto [meta, tuple_data] = table_info_->table_->GetTuple(r);

      auto [undo_log, to_update_tuple] = GetPartialAndWholeTuple(tuple_data, meta.ts_);

      UpdateWithVersionLink(r, to_update_tuple, undo_log, exec_ctx_->GetTransaction(), txn_mgr_,
                            table_info_, &child_executor_->GetOutputSchema(), plan_->table_oid_);
    }

    ret_values.emplace_back(INTEGER, static_cast<int>(rids_.size()));
  } else {
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

auto UpdateExecutor::GetPartialAndWholeTuple(Tuple &tuple_data, timestamp_t meta_ts) -> std::pair<UndoLog, Tuple> {
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

  // TODO(chenzonghao): Check the stmt below
  // auto key = to_insert_tuple.KeyFromTuple(table_info_->schema_, primary_index->key_schema_,
  // primary_index->index_->GetKeyAttrs());

  Schema s = Schema::CopySchema(&child_executor_->GetOutputSchema(), attrs);

  auto part = Tuple(part_values, &s);
  auto whole = Tuple(whole_values, &child_executor_->GetOutputSchema());
  auto new_undo_log = UndoLog{false, mf, Tuple{part_values, &s}, meta_ts};

//  if (!table_indexes_.empty()) {
//    BUSTUB_ASSERT(table_indexes_.size() == 1, "Only support primary key");
//    auto primary_index = table_indexes_[0];
//    BUSTUB_ASSERT(primary_index->is_primary_key_,
//                  "In the case that db only contain one index, it must be primary index");
//
//    auto key = Tuple(part_values, &primary_index->key_schema_);
//    if (IsTupleContentEqual(key, part)) {
//      modify_pkey_ = true;
//    }
//  }

  return {new_undo_log, whole};
}

}  // namespace bustub
