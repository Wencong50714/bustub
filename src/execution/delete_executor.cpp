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

#include "execution/execution_common.h"
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
    txn_mgr_ = exec_ctx_->GetTransactionManager();

    // Since update executor is pipeline breaker
    Tuple child_tuple{};
    RID rid{};
    while (child_executor_->Next(&child_tuple, &rid)) {
      rids_.push_back(rid);
    }

    BUSTUB_ENSURE(!rids_.empty(), "Delete: It's impossible that rids is empty");
    BUSTUB_ENSURE(rids_.size() == 1, "delete: only handle one element");
  }
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }

  std::vector<Value> values;

  if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::SNAPSHOT_ISOLATION) {
    for (const auto &r : rids_) {
      size_t mf_sz = child_executor_->GetOutputSchema().GetColumns().size();
      UpdateWithVersionLink(r, std::nullopt, mf_sz, DELETE_OP, exec_ctx_->GetTransaction(), txn_mgr_, table_info_,
                            &child_executor_->GetOutputSchema(), plan_->table_oid_);
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
