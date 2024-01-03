//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  txn_ref->read_ts_.store(last_commit_ts_);

  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  auto commit_ts = last_commit_ts_.load() + 1;

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // For tuples in write set, modify its commit ts
  for (const auto &write_set : txn->GetWriteSets()) {
    auto table_info = catalog_->GetTable(write_set.first);

    for (const auto &rid : write_set.second) {
      auto meta = table_info->table_->GetTuple(rid).first;

      if (!GetUndoLink(rid).has_value() && meta.is_deleted_) {
        table_info->table_->UpdateTupleMeta({0, true}, rid);
        continue;
      }

      meta.ts_ = commit_ts;
      table_info->table_->UpdateTupleMeta(meta, rid); // write back
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->commit_ts_ = commit_ts;

  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  last_commit_ts_.fetch_add(1);

  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

/**
 * Traverse the version chain, if find invisible undolog, change it's ts_ to INVALID_TS
 * @param undo_link
 */
auto TransactionManager::FindAndSetInvisible(UndoLink undo_link) -> void {
  auto watermark = GetWatermark();
  auto invalid_undo_link = UndoLink{INVALID_TXN_ID, 0};
  bool flag = false;

  // traverse the version chain
  while (undo_link != invalid_undo_link) {
    auto undo_log_op = GetUndoLogOptional(undo_link);
    if (undo_log_op == std::nullopt) {
      return; // may be point to removed txn undolog
    }
    auto undo_log = undo_log_op.value();

    std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
    auto txn_ref = txn_map_[undo_link.prev_txn_];
    BUSTUB_ASSERT(txn_ref->GetTransactionState() != TransactionState::RUNNING, "It's impossible");

    if (flag) {
      undo_log.ts_ = INVALID_TS;
      txn_ref->ModifyUndoLog(undo_link.prev_log_idx_, undo_log); // change ts to invalid indicate invisible
    } else if (undo_log.ts_ <= watermark) {
      flag = true;

      if (watermark > txn_ref->GetCommitTs()) {
        undo_log.ts_ = INVALID_TS;
        txn_ref->ModifyUndoLog(undo_link.prev_log_idx_, undo_log);
      }
    }

    undo_link = undo_log.prev_version_;
  }
}

void TransactionManager::GarbageCollection() {
  // PHASE 1: Iterate all version chain to set invisible undo logs
  std::shared_lock<std::shared_mutex> lck(version_info_mutex_);
  for (const auto & iter : version_info_) {
    auto pg_ver_info = iter.second;

    std::shared_lock<std::shared_mutex> lck2(pg_ver_info->mutex_);
    for (const auto & iter2 : pg_ver_info->prev_version_) {
      auto first_link = iter2.second.prev_;
      FindAndSetInvisible(first_link); // first log can't be collect
    }
  }
  lck.unlock();

  // PHASE 2: remove expired txns
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);

  std::vector<txn_id_t> txns_to_remove{}; // reserve to_remove txn id
  for (const auto & txn_entry : txn_map_) {
    auto txn_ref = txn_entry.second;

    if (txn_ref->GetTransactionState() == TransactionState::COMMITTED || txn_ref->GetTransactionState() == TransactionState::ABORTED) {
      // TODO: handle TAINTED txn
      bool gc_flag = true;

      for (size_t i = 0; i < txn_ref->GetUndoLogNum(); i++) {
        if (txn_ref->GetUndoLog(i).ts_ != INVALID_TS) {
          gc_flag = false;
        }
      }

      if (gc_flag) {
        txns_to_remove.emplace_back(txn_entry.first);
      }
    }
  }

  for (const auto &txn_id : txns_to_remove) {
    txn_map_.erase(txn_id);
  }
}

}  // namespace bustub
