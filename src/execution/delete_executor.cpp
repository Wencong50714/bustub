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
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }

  Tuple child_tuple{};
  int cnt = 0;
  while(child_executor_->Next(&child_tuple, rid)) {
    // delete
    TupleMeta meta = table_info_->table_->GetTupleMeta(*rid);
    meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(meta, *rid);
    cnt++;
  }

  std::vector<Value> values;
  values.emplace_back(INTEGER, cnt);
  *tuple = Tuple(values, &GetOutputSchema());
  is_end_ = true;
  return true;
}

}  // namespace bustub
