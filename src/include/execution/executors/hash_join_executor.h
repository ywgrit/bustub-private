//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
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
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
struct HashKey {
  Value key_;
  bool operator==(const HashKey &hashkey) const { return key_.CompareEquals(hashkey.key_) == CmpBool::CmpTrue; }
};
}  // namespace bustub

namespace std {
template <>
struct hash<bustub::HashKey> {
  size_t operator()(const bustub::HashKey &hashkey) const {
    size_t key_hash = 0;
    if (!hashkey.key_.IsNull()) {
      key_hash = bustub::HashUtil::CombineHashes(key_hash, bustub::HashUtil::HashValue(&hashkey.key_));
    }
    return key_hash;
  }
};
}  // namespace std

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced by the join
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the join */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  /** The left child executor */
  std::unique_ptr<AbstractExecutor> left_executor_;
  /** The right child executor */
  std::unique_ptr<AbstractExecutor> right_executor_;
  /** Map from key to tuples */
  std::unordered_map<HashKey, std::vector<Tuple>> hashtable_;  // HashKey need a hash function
  /** Result tuples of join of left_tuples and right_tuples */
  std::vector<Tuple> result_tuples_;
  /** Id of result_tuples */
  uint32_t id_;
};

}  // namespace bustub
