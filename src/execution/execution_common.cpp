#include "execution/execution_common.h"
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto VersionLinkCheck(std::optional<VersionUndoLink> link) -> bool {
  if (!link.has_value()) {
    return true;
  }
  return !link->in_progress_;
}

auto UpdateTupleHeap(const RID &r, const std::optional<Tuple> &to_update_tuple, const TableInfo *table_info,
                     txn_id_t txn_id) {
  if (to_update_tuple.has_value()) {
    table_info->table_->UpdateTupleInPlace({txn_id, false}, to_update_tuple.value(), r, nullptr);
  } else {
    table_info->table_->UpdateTupleMeta({txn_id, true}, r);
  }
}

auto GenerateUndoLog(uint32_t type, size_t mf_sz, TupleMeta meta, const Tuple &tuple) -> UndoLog {
  std::vector<bool> mf(mf_sz, true);
  UndoLog undo_log;

  switch (type) {
    case INSERT_OP:
      undo_log = UndoLog{true, mf, {}, meta.ts_};
      break;
    case DELETE_OP:
      undo_log = UndoLog{false, mf, tuple, meta.ts_};
      break;
    default:
      throw std::runtime_error("type that is neither INSERT_OP and DELETE_OP");
  }

  return undo_log;
}

auto UpdateWithVersionLink(const RID &r, std::optional<Tuple> to_update_tuple, size_t mf_sz, uint32_t type,
                           Transaction *txn, TransactionManager *txn_mgr, const TableInfo *table_info,
                           const Schema *child_schema, table_oid_t t_id) -> void {
  auto txn_id = txn->GetTransactionId();

  auto [meta, tuple] = table_info->table_->GetTuple(r);

  if (meta.ts_ == txn_id) {
    auto ver_link_op = txn_mgr->GetVersionLink(r);
    if (ver_link_op.has_value()) {
      // Update undo log
      auto new_undo_log = GenerateUndoLog(type, mf_sz, meta, tuple);

      auto undo_link = ver_link_op.value().prev_;

      // version link may have invalid value
      auto undo_log_op = txn_mgr->GetUndoLogOptional(undo_link);
      BUSTUB_ENSURE(undo_log_op.has_value(), "undo_log_op must have value");
      auto undo_log = undo_log_op.value();
      new_undo_log = OverlayUndoLog(new_undo_log, undo_log, child_schema);
      txn->ModifyUndoLog(undo_link.prev_log_idx_, new_undo_log);
    }

    // Modify tuple heap
    if (to_update_tuple.has_value()) {
      table_info->table_->UpdateTupleInPlace({txn_id, false}, to_update_tuple.value(), r, nullptr);
    } else {
      table_info->table_->UpdateTupleMeta({txn_id, true}, r);
    }
    return;
  }

  // check and modify version link
  auto ver_link_op = txn_mgr->GetVersionLink(r);
  if (ver_link_op.has_value()) {
    // 1: set ver_link in_progress to true
    auto ver_link = ver_link_op.value();
    ver_link.in_progress_ = true;
    if (!txn_mgr->UpdateVersionLink(r, ver_link, VersionLinkCheck)) {
      txn->SetTainted();
      throw ExecutionException("write-write conflict: version link in progress");
    }

    // 2. Get latest tuple from table
    auto [new_meta, new_tuple] = table_info->table_->GetTuple(r);

    // detect write-write conflict
    if ((new_meta.ts_ > TXN_START_ID) || (new_meta.ts_ < TXN_START_ID && new_meta.ts_ > txn->GetReadTs())) {
      // Two cases need to be aborted
      ver_link.in_progress_ = false;
      txn_mgr->UpdateVersionLink(r, ver_link, nullptr);

      txn->SetTainted();
      throw ExecutionException("write-write conflict: another");
    }

    // 3. Generate undo log
    auto new_undo_log = GenerateUndoLog(type, mf_sz, new_meta, new_tuple);

    // 4. link undo log to version chain
    auto undo_link = ver_link.prev_;
    new_undo_log.prev_version_ = undo_link;
    auto link = txn->AppendUndoLog(new_undo_log);
    ver_link.prev_ = link;

    // 5. update table heap content
    UpdateTupleHeap(r, to_update_tuple, table_info, txn_id);

    // 6. set in_progress back to false
    ver_link.in_progress_ = false;
    txn_mgr->UpdateVersionLink(r, ver_link, nullptr);
  } else {
    // create a placeholder version link
    auto ver_link = VersionUndoLink{{}, true};
    if (!txn_mgr->UpdateVersionLink(r, ver_link, VersionLinkCheck)) {
      txn->SetTainted();
      throw ExecutionException("write-write conflict: version link in progress");
    }

    // 2. Get latest tuple from table and detect write-write conflict
    auto [new_meta, new_tuple] = table_info->table_->GetTuple(r);

    if ((new_meta.ts_ > TXN_START_ID) || (new_meta.ts_ < TXN_START_ID && new_meta.ts_ > txn->GetReadTs())) {
      // Two cases need to be aborted
      ver_link.in_progress_ = false;
      txn_mgr->UpdateVersionLink(r, std::nullopt, nullptr);

      txn->SetTainted();
      throw ExecutionException("write-write conflict: another");
    }

    // 3. generate undo log
    auto new_undo_log = GenerateUndoLog(type, mf_sz, new_meta, new_tuple);

    // 4. link undo log to version chain
    auto link = txn->AppendUndoLog(new_undo_log);
    ver_link.prev_ = link;

    // 5. update tuple heap contents
    UpdateTupleHeap(r, to_update_tuple, table_info, txn_id);

    // 6. set in_progress back to false
    ver_link.in_progress_ = false;
    txn_mgr->UpdateVersionLink(r, ver_link, nullptr);
  }

  // add write set
  txn->AppendWriteSet(t_id, r);
}

auto BoolVectorToString(const std::vector<bool> &boolVector) -> std::string {
  std::ostringstream oss;
  oss << "mf: ( ";
  for (bool value : boolVector) {
    // Convert boolean value to its integer representation and append to the string
    oss << static_cast<int>(value) << ' ';
  }
  oss << ") ";
  return oss.str();
}

/**
 * Overlay two undo logs
 * @param new_undo_log The newer one
 * @param old_undo_log The older one
 * @param schema The Intact tuple schema
 * @return The overlaid undo log
 */
auto OverlayUndoLog(UndoLog &new_undo_log, const UndoLog &old_undo_log, const Schema *schema) -> UndoLog {
  BUSTUB_ENSURE(new_undo_log.modified_fields_.size() == old_undo_log.modified_fields_.size(), "Scheme should be same");

  if (old_undo_log.is_deleted_) {
    return old_undo_log;
  }

  auto size = new_undo_log.modified_fields_.size();
  std::vector<bool> mf(size, false);
  Schema new_schema = GetPartialSchema(new_undo_log.modified_fields_, schema);
  Schema old_schema = GetPartialSchema(old_undo_log.modified_fields_, schema);
  int idx1 = 0;
  int idx2 = 0;

  std::vector<Value> values{};

  for (size_t i = 0; i < size; i++) {
    if (old_undo_log.modified_fields_[i]) {
      mf[i] = true;
      values.push_back(old_undo_log.tuple_.GetValue(&old_schema, idx1));
    } else if (new_undo_log.modified_fields_[i]) {
      mf[i] = true;
      values.push_back(new_undo_log.tuple_.GetValue(&new_schema, idx2));
    }

    if (old_undo_log.modified_fields_[i]) {
      idx1++;
    }
    if (new_undo_log.modified_fields_[i]) {
      idx2++;
    }
  }

  Schema out_schema = GetPartialSchema(mf, schema);
  return {false, mf, {values, &out_schema}, old_undo_log.ts_, old_undo_log.prev_version_};
}

auto GetPartialSchema(const std::vector<bool> &mf, const Schema *schema) -> Schema {
  std::vector<Column> columns{};
  for (uint32_t i = 0; i < mf.size(); i++) {
    if (mf[i]) {
      columns.push_back(schema->GetColumn(i));
    }
  }

  return Schema{columns};
}

/**
 * Apply the undo_log's modification to ret tuple
 * @param ret  The return tuple
 * @param modified_fields
 * @param tuple
 * @param schema
 */
auto ApplyModification(const Tuple &ret, const UndoLog &undoLog, const Schema *schema) -> Tuple {
  // Get partial schema
  Schema partial = GetPartialSchema(undoLog.modified_fields_, schema);

  int cnt = 0;
  std::vector<Value> values{};
  for (uint32_t i = 0; i < undoLog.modified_fields_.size(); i++) {
    undoLog.modified_fields_[i] ? values.push_back(undoLog.tuple_.GetValue(&partial, cnt++))
                                : values.push_back(ret.GetValue(schema, i));
  }

  return {values, schema};
}

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  if (undo_logs.empty()) {
    return (base_meta.is_deleted_ ? std::nullopt : std::optional<Tuple>(base_tuple));
  }

  auto ret_tuple = base_tuple;

  for (const auto &undo_log : undo_logs) {
    if (!undo_log.is_deleted_) {
      ret_tuple = ApplyModification(ret_tuple, undo_log, schema);
    }
  }

  return (undo_logs.back().is_deleted_ ? std::nullopt : std::optional<Tuple>(ret_tuple));
}

auto GetHumanReadableTxnId(txn_id_t txn_id) -> txn_id_t { return txn_id ^ TXN_START_ID; }

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  std::stringstream output;  // Batch output to reduce terminal print I/O operations

  output << "debug_hook: " << info << '\n';

  for (auto it = table_heap->MakeIterator(); !it.IsEnd(); ++it) {
    auto rid = it.GetRID();

    /*----------------- Tuple Heap Info -----------------*/
    output << "RID=" << rid.GetPageId() << '/' << rid.GetSlotNum() << ' ';
    auto t = it.GetTuple();
    auto meta = t.first;

    if (meta.ts_ >= TXN_START_ID) {
      // tuple is modified by uncommitted txn, meta's ts is txn_id now
      output << "ts=txn" << GetHumanReadableTxnId(meta.ts_) << ' ';
    } else {
      output << "ts=" << meta.ts_ << ' ';
    }

    if (meta.is_deleted_) {
      output << "<del marker> ";
    }
    output << "tuple=" << t.second.ToString(&table_info->schema_) << '\n';

    if (!txn_mgr->GetUndoLink(rid).has_value()) {
      continue;
    }

    /*----------------- Undo log Info -----------------*/
    auto undo_link = txn_mgr->GetUndoLink(rid).value();
    while (undo_link.IsValid()) {
      auto undo_log_op = txn_mgr->GetUndoLogOptional(undo_link);
      if (!undo_log_op.has_value()) {
        break;
      }

      auto undo_log = undo_log_op.value();

      output << "  txn" << GetHumanReadableTxnId(undo_link.prev_txn_) << ' ';
      if (undo_log.is_deleted_) {
        output << "<del> ";
      } else {
        auto sub_tuple = ApplyModification(t.second, undo_log, &table_info->schema_);
        output << sub_tuple.ToString(&table_info->schema_) << ' ';
        output << BoolVectorToString(undo_log.modified_fields_);
      }
      output << "ts=" << undo_log.ts_ << '\n';

      undo_link = undo_log.prev_version_;
    }
  }
  // Print all collected strings at once
  output << "---------------- Divider ----------------\n";
  std::cout << output.str();

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

}  // namespace bustub
