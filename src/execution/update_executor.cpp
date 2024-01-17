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
#include <regex>

#include "execution/execution_common.h"
#include "execution/executors/update_executor.h"

namespace bustub {

auto UpdateExecutor::CheckModifyPrimaryKey() {
  for (const auto &expr : plan_->target_expressions_) {
    auto str = expr->ToString();
    std::regex pattern(R"(\(#0\.0[+\-]([0-9])\))");
    if (std::regex_match(str, pattern)) {
      modify_primary_ = true;
    }
  }
}

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

    if (rids_.empty()) {
      exec_ctx_->GetTransaction()->SetTainted();
      throw ExecutionException("read-write conflict: read nothing");
      //      TxnMgrDbg("..", txn_mgr_, table_info_, table_info_->table_.get());
      //      BUSTUB_ENSURE(false, fmt::format("update: expected 1 element, but found {}: {}", rids_.size(),
      //      plan_->ToString()));
    }

    CheckModifyPrimaryKey();
  }
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }

  std::vector<Value> ret_values;

  if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::SNAPSHOT_ISOLATION) {
    if (modify_primary_) {
      // delete phase
      for (const auto &r : rids_) {
        size_t mf_sz = child_executor_->GetOutputSchema().GetColumns().size();
        UpdateWithVersionLink(r, std::nullopt, mf_sz, DELETE_OP, exec_ctx_->GetTransaction(), txn_mgr_, table_info_,
                              &child_executor_->GetOutputSchema(), plan_->table_oid_);
      }

      // insert phase
      for (const auto &ir : rids_) {
        auto child_tuple = table_info_->table_->GetTuple(ir).second;

        std::vector<Value> values{};
        values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
        for (const auto &expr : plan_->target_expressions_) {
          values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
        }
        auto to_update_tuple = Tuple{values, &child_executor_->GetOutputSchema()};

        auto primary_index = table_indexes_[0];
        std::vector<RID> rids{};
        rids.clear();
        primary_index->index_->ScanKey(to_update_tuple.KeyFromTuple(table_info_->schema_, primary_index->key_schema_,
                                                                    primary_index->index_->GetKeyAttrs()),
                                       &rids, exec_ctx_->GetTransaction());

        std::cout << "tuple :" << to_update_tuple.ToString(&table_info_->schema_) << ' ';
        if (rids.empty()) {
          std::cout << "create a new index\n";
          // Insert a new tuple in tuple heap
          TupleMeta meta{txn_id_, false};
          auto new_rid = table_info_->table_->InsertTuple(meta, to_update_tuple, exec_ctx_->GetLockManager(),
                                                          exec_ctx_->GetTransaction(), plan_->table_oid_);
          BUSTUB_ASSERT(new_rid.has_value(), "New allocated tuple must have valid rid");

          // insert index entry
          if (!primary_index->index_->InsertEntry(
                  to_update_tuple.KeyFromTuple(table_info_->schema_, primary_index->key_schema_,
                                               primary_index->index_->GetKeyAttrs()),
                  *new_rid, exec_ctx_->GetTransaction())) {
            // unique key constraint is violated
            exec_ctx_->GetTransaction()->SetTainted();
            throw ExecutionException("write-write conflict");
          }

          txn_mgr_->UpdateUndoLink(*new_rid, std::nullopt, nullptr);
          exec_ctx_->GetTransaction()->AppendWriteSet(plan_->table_oid_, *new_rid);  // append write set
        } else {
          std::cout << "create a new index\n";
          // Update the version link
          BUSTUB_ASSERT(rids.size() == 1, "Should only scan 1 rid, since we always update in place");

          auto old_r = rids[0];
          auto meta = table_info_->table_->GetTuple(old_r).first;

          if (meta.ts_ == txn_id_) {
            auto ver_link_op = txn_mgr_->GetVersionLink(old_r);

            if (ver_link_op.has_value()) {
              auto undo_link = ver_link_op.value().prev_;
              auto undo_log_op = txn_mgr_->GetUndoLogOptional(undo_link);
              BUSTUB_ENSURE(undo_log_op.has_value(), "update: undo log must valid");

              // Step2: generate undo log
              std::vector<bool> mf(child_executor_->GetOutputSchema().GetColumns().size(), true);
              auto new_undo_log = UndoLog{true, mf, {}, meta.ts_};

              auto undo_log = undo_log_op.value();

              new_undo_log = OverlayUndoLog(new_undo_log, undo_log, &child_executor_->GetOutputSchema());
              exec_ctx_->GetTransaction()->ModifyUndoLog(undo_link.prev_log_idx_, new_undo_log);
            }

            // Modify tuple heap
            table_info_->table_->UpdateTupleInPlace({txn_id_, false}, to_update_tuple, old_r, nullptr);
            continue;
          }

          auto ver_link_op = txn_mgr_->GetVersionLink(old_r);
          if (ver_link_op.has_value()) {
            // 1: set ver_link in_progress to true
            auto ver_link = ver_link_op.value();
            ver_link.in_progress_ = true;
            if (!txn_mgr_->UpdateVersionLink(old_r, ver_link, VersionLinkCheck)) {
              exec_ctx_->GetTransaction()->SetTainted();
              throw ExecutionException("write-write conflict: version link in progress");
            }

            // 2. Get latest tuple from table
            auto [new_meta, new_tuple] = table_info_->table_->GetTuple(old_r);

            // detect write-write conflict
            if ((new_meta.ts_ > TXN_START_ID) || (new_meta.ts_ < TXN_START_ID && new_meta.ts_ > ts_)) {
              // Two cases need to be aborted
              ver_link.in_progress_ = false;
              txn_mgr_->UpdateVersionLink(old_r, ver_link, nullptr);

              exec_ctx_->GetTransaction()->SetTainted();
              throw ExecutionException("write-write conflict: another");
            }

            // 3. Generate Undo Log and to_update_tuple
            std::vector<bool> mf(child_executor_->GetOutputSchema().GetColumns().size(), true);
            auto new_undo_log = UndoLog{true, mf, {}, meta.ts_};

            // 4. link undo log to version chain
            auto undo_link = ver_link.prev_;
            new_undo_log.prev_version_ = undo_link;
            auto link = exec_ctx_->GetTransaction()->AppendUndoLog(new_undo_log);
            ver_link.prev_ = link;

            // 5. update table heap content
            table_info_->table_->UpdateTupleInPlace({txn_id_, false}, to_update_tuple, old_r, nullptr);

            // 6. set in_progress back to false
            ver_link.in_progress_ = false;
            txn_mgr_->UpdateVersionLink(old_r, ver_link, nullptr);
          } else {
            // create a placeholder version link
            auto ver_link = VersionUndoLink{{INVALID_TXN_ID, 0}, true};
            if (!txn_mgr_->UpdateVersionLink(old_r, ver_link, VersionLinkCheck)) {
              exec_ctx_->GetTransaction()->SetTainted();
              throw ExecutionException("write-write conflict: version link in progress");
            }

            // 2. Get latest tuple from table and detect write-write conflict
            auto [new_meta, new_tuple] = table_info_->table_->GetTuple(old_r);

            if ((new_meta.ts_ > TXN_START_ID) || (new_meta.ts_ < TXN_START_ID && new_meta.ts_ > ts_)) {
              // Two cases need to be aborted
              txn_mgr_->UpdateVersionLink(old_r, std::nullopt, nullptr);

              exec_ctx_->GetTransaction()->SetTainted();
              throw ExecutionException("write-write conflict: another");
            }

            // 3. Generate Undo Log and to_update_tuple
            std::vector<bool> mf(child_executor_->GetOutputSchema().GetColumns().size(), true);
            auto new_undo_log = UndoLog{true, mf, {}, meta.ts_};

            // 4. link undo log to version chain
            auto link = exec_ctx_->GetTransaction()->AppendUndoLog(new_undo_log);
            ver_link.prev_ = link;

            // 5. update tuple heap contents
            table_info_->table_->UpdateTupleInPlace({txn_id_, false}, to_update_tuple, old_r, nullptr);

            // 6. set in_progress back to false
            ver_link.in_progress_ = false;
            txn_mgr_->UpdateVersionLink(old_r, ver_link, nullptr);
          }

          exec_ctx_->GetTransaction()->AppendWriteSet(plan_->table_oid_, old_r);
        }
      }
    } else {
      for (const auto &r : rids_) {
        auto [meta, tuple_data] = table_info_->table_->GetTuple(r);

        if (meta.ts_ == txn_id_) {
          auto ver_link_op = txn_mgr_->GetVersionLink(r);

          auto [new_undo_log, to_update_tuple] = GetPartialAndWholeTuple(tuple_data, meta.ts_);

          if (ver_link_op.has_value()) {
            auto undo_link = ver_link_op.value().prev_;
            auto undo_log_op = txn_mgr_->GetUndoLogOptional(undo_link);
            BUSTUB_ENSURE(undo_log_op.has_value(), "update: undo log must valid");

            auto undo_log = undo_log_op.value();

            new_undo_log = OverlayUndoLog(new_undo_log, undo_log, &child_executor_->GetOutputSchema());
            exec_ctx_->GetTransaction()->ModifyUndoLog(undo_link.prev_log_idx_, new_undo_log);
          }

          // Modify tuple heap
          table_info_->table_->UpdateTupleInPlace({txn_id_, false}, to_update_tuple, r, nullptr);
          continue;
        }

        auto ver_link_op = txn_mgr_->GetVersionLink(r);
        if (ver_link_op.has_value()) {
          // 1: set ver_link in_progress to true
          auto ver_link = ver_link_op.value();
          ver_link.in_progress_ = true;
          if (!txn_mgr_->UpdateVersionLink(r, ver_link, VersionLinkCheck)) {
            exec_ctx_->GetTransaction()->SetTainted();
            throw ExecutionException("write-write conflict: version link in progress");
          }

          // 2. Get latest tuple from table
          auto [new_meta, new_tuple] = table_info_->table_->GetTuple(r);

          // detect write-write conflict
          if ((new_meta.ts_ > TXN_START_ID) || (new_meta.ts_ < TXN_START_ID && new_meta.ts_ > ts_)) {
            // Two cases need to be aborted
            ver_link.in_progress_ = false;
            txn_mgr_->UpdateVersionLink(r, ver_link, nullptr);

            exec_ctx_->GetTransaction()->SetTainted();
            throw ExecutionException("write-write conflict: another");
          }

          // 3. Generate Undo Log and to_update_tuple
          auto [new_undo_log, to_update_tuple] = GetPartialAndWholeTuple(new_tuple, new_meta.ts_);

          // 4. link undo log to version chain
          auto undo_link = ver_link.prev_;
          new_undo_log.prev_version_ = undo_link;
          auto link = exec_ctx_->GetTransaction()->AppendUndoLog(new_undo_log);
          ver_link.prev_ = link;

          // 5. update table heap content
          table_info_->table_->UpdateTupleInPlace({txn_id_, false}, to_update_tuple, r, nullptr);

          // 6. set in_progress back to false
          ver_link.in_progress_ = false;
          txn_mgr_->UpdateVersionLink(r, ver_link, nullptr);
        } else {
          // create a placeholder version link
          auto ver_link = VersionUndoLink{{INVALID_TXN_ID, 0}, true};
          if (!txn_mgr_->UpdateVersionLink(r, ver_link, VersionLinkCheck)) {
            exec_ctx_->GetTransaction()->SetTainted();
            throw ExecutionException("write-write conflict: version link in progress");
          }

          // 2. Get latest tuple from table and detect write-write conflict
          auto [new_meta, new_tuple] = table_info_->table_->GetTuple(r);

          if ((new_meta.ts_ > TXN_START_ID) || (new_meta.ts_ < TXN_START_ID && new_meta.ts_ > ts_)) {
            // Two cases need to be aborted
            txn_mgr_->UpdateVersionLink(r, std::nullopt, nullptr);

            exec_ctx_->GetTransaction()->SetTainted();
            throw ExecutionException("write-write conflict: another");
          }

          // 3. generate undo log
          auto [new_undo_log, to_update_tuple] = GetPartialAndWholeTuple(new_tuple, new_meta.ts_);

          // 4. link undo log to version chain
          auto link = exec_ctx_->GetTransaction()->AppendUndoLog(new_undo_log);
          ver_link.prev_ = link;

          // 5. update tuple heap contents
          table_info_->table_->UpdateTupleInPlace({txn_id_, false}, to_update_tuple, r, nullptr);

          // 6. set in_progress back to false
          ver_link.in_progress_ = false;
          txn_mgr_->UpdateVersionLink(r, ver_link, nullptr);
        }

        exec_ctx_->GetTransaction()->AppendWriteSet(plan_->table_oid_, r);
      }
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

  Schema s = Schema::CopySchema(&child_executor_->GetOutputSchema(), attrs);

  auto part = Tuple(part_values, &s);
  auto whole = Tuple(whole_values, &child_executor_->GetOutputSchema());
  auto new_undo_log = UndoLog{false, mf, Tuple{part_values, &s}, meta_ts};

  return {new_undo_log, whole};
}

}  // namespace bustub
