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

/**
 * Apply the undo_log's modification to ret tuple
 * @param ret  The return tuple
 * @param modified_fields
 * @param tuple
 * @param schema
 */
auto ApplyModification(const Tuple& ret, const UndoLog& undoLog, const Schema *schema) -> Tuple {
  // Get partial schema
  std::vector<Column> columns{};
  for (uint32_t i = 0; i < undoLog.modified_fields_.size(); i++) {
    if (undoLog.modified_fields_[i]) {
      columns.push_back(schema->GetColumn(i));
    }
  }

  Schema partial = Schema{columns};

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

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  fmt::println(
      stderr,
      "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
      "finished task 2. Implementing this helper function will save you a lot of time for debugging in later tasks.");

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
