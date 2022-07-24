//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.h
//
// Identification: src/include/execution/executors/distinct_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/distinct_plan.h"

namespace bustub {

/** DistinctKey represents a key in an distinct operation */
struct DistinctKey {
  /** The values */
  std::vector<Value> values_;

  /**
   * Compares two distinct keys for equality.
   * @param other the other distinct key to be compared with
   * @return `true` if equivalent, `false` otherwise
   */
  bool operator==(const DistinctKey &other) const {
    for (uint32_t i = 0; i < other.values_.size(); i++) {
      if (values_[i].CompareEquals(other.values_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};
}  // namespace bustub

namespace std {

/** Implements std::hash on AggregateKey */
template <>
struct hash<bustub::DistinctKey> {
  std::size_t operator()(const bustub::DistinctKey &dis_key) const {
    size_t curr_hash = 0;
    for (const auto &key : dis_key.values_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};
}  // namespace std

namespace bustub {

/**
 * DistinctExecutor removes duplicate rows from child ouput.
 */
class DistinctExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new DistinctExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The limit plan to be executed
   * @param child_executor The child executor from which tuples are pulled
   */
  DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the distinct */
  void Init() override;

  /**
   * Yield the next tuple from the distinct.
   * @param[out] tuple The next tuple produced by the distinct
   * @param[out] rid The next tuple RID produced by the distinct
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  DistinctKey MakeDistinctKey(const Tuple *tuple) {
    DistinctKey distinctkey;
    uint32_t column_count = plan_->OutputSchema()->GetColumnCount();
    for (uint32_t column_idx = 0; column_idx < column_count; column_idx++) {
      distinctkey.values_.emplace_back(tuple->GetValue(plan_->OutputSchema(), column_idx));
    }
    return distinctkey;
  }

  /** @return The output schema for the distinct */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  /** The distinct plan node to be executed */
  const DistinctPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  /** The map from DistinctKey to tuple */
  std::unordered_map<DistinctKey, Tuple> hash_table_;
  /** The iterator of hash_table_ */
  std::unordered_map<DistinctKey, Tuple>::const_iterator iter_;
};
}  // namespace bustub
