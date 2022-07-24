//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)),
      id_(0) {}

void HashJoinExecutor::Init() {
  Tuple tuple;
  RID rid;
  left_executor_->Init();
  try {
    while (left_executor_->Next(&tuple, &rid)) {
      HashKey hashkey;
      hashkey.key_ = plan_->LeftJoinKeyExpression()->Evaluate(
          &tuple, left_executor_->GetOutputSchema());  // TODO(wx): need judge count(hashkey) == 0 or not ?
                                                       /* if (hashtable_.count(hashkey) != 0) { */
      hashtable_[hashkey].emplace_back(tuple);
      /* } else { */
      /* hashtable_[hashkey] = std::vector<Tuple>{tuple}; */
      /* } */
    }
  } catch (Exception &e) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, " hash_join_executor: left_executor_ execute error. ");
  }

  right_executor_->Init();
  try {
    while (right_executor_->Next(&tuple, &rid)) {
      HashKey hashkey;
      hashkey.key_ = plan_->RightJoinKeyExpression()->Evaluate(
          &tuple, right_executor_->GetOutputSchema());  // need to judge count(hashkey) == 0 or not ?
                                                        /* if (hashtable_.count(hashkey) != 0) { */
      for (const auto &left_tuple : hashtable_[hashkey]) {
        std::vector<Value> out_value;
        for (const auto &column : GetOutputSchema()->GetColumns()) {
          out_value.emplace_back(column.GetExpr()->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(), &tuple,
                                                                right_executor_->GetOutputSchema()));
        }
        result_tuples_.emplace_back(Tuple(out_value, GetOutputSchema()));
      }
      /* } */
    }
  } catch (Exception &e) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, " hash_join_executor: right_executor_ execute error. ");
  }
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (id_ < result_tuples_.size()) {
    *tuple = result_tuples_[id_++];
    *rid = tuple->GetRid();
    return true;
  }
  return false;
}

}  // namespace bustub
