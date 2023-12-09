//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  child_executor_->Init();

  Tuple child_tuple{};
  RID rid;

  size_t cnt = 0;
  while (child_executor_->Next(&child_tuple, &rid) && cnt < plan_->GetLimit()) {
    tuples_.emplace_back(child_tuple);
    cnt++;
  }
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (it_ == tuples_.size()) {
    return false;
  }

  *tuple = tuples_[it_++];
  return true;
}

}  // namespace bustub
