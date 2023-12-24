#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }

  current_reads_[read_ts]++;
  watermark_ = current_reads_.begin()->first;
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  if (--current_reads_[read_ts] == 0) {
    current_reads_.erase(read_ts);
  }
  if (!current_reads_.empty()) {
    watermark_ = current_reads_.begin()->first;
  }
}

}  // namespace bustub
