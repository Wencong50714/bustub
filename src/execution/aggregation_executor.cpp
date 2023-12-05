//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(SimpleAggregationHashTable(plan_->GetAggregates(), plan_->agg_types_)),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_executor_->Init();

  Tuple child_tuple{};
  RID rid{};

  // In Init phase, receive all data
  auto status = child_executor_->Next(&child_tuple, &rid);
  while (status) {
    AggregateKey keys = MakeAggregateKey(&child_tuple);
    AggregateValue values = MakeAggregateValue(&child_tuple);
    aht_.InsertCombine(keys, values);
    status = child_executor_->Next(&child_tuple, &rid);
    empty_ = false;
  }

  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::vector<Value> values{};

  if (aht_.Begin() == aht_.End() && empty_) {
    empty_ = false;
    // Make sure that  (values.size() == schema->GetColumnCount())
    if (GetOutputSchema().GetColumnCount() == aht_.GenerateInitialAggregateValue().aggregates_.size()) {
      for (auto &value : aht_.GenerateInitialAggregateValue().aggregates_) {
        values.push_back(value);
      }
      *tuple = Tuple(values, &GetOutputSchema());
      return true;
    }
    return false;
  }

  if (aht_iterator_ == aht_.End() && !empty_) {
    return false;
  }

  values.reserve(GetOutputSchema().GetColumnCount());

  auto keys = aht_iterator_.Key();
  for (auto &key : keys.group_bys_) {
    values.push_back(key);
  }

  auto v = aht_iterator_.Val();
  for (auto &value : v.aggregates_) {
    values.push_back(value);
  }

  *tuple = Tuple(values, &GetOutputSchema());
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
