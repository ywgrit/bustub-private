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
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      hash_table_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      iter_(hash_table_.Begin()) {}

void AggregationExecutor::Init() {
  Tuple tuple;
  RID rid;
  child_->Init();
  try {
    while (child_->Next(&tuple, &rid)) {
      AggregateKey key = MakeAggregateKey(&tuple);
      AggregateValue value = MakeAggregateValue(&tuple);
      hash_table_.InsertCombine(key, value);
    }
  } catch (Exception &e) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, " aggregation_executor: child_ execute error. ");
  }
  iter_ = hash_table_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  if (iter_ != hash_table_.End()) {
    const AggregateKey &key = iter_.Key();
    const AggregateValue &val = iter_.Val();
    ++iter_;

    if (plan_->GetHaving() == nullptr ||
        plan_->GetHaving()->EvaluateAggregate(key.group_bys_, val.aggregates_).GetAs<bool>()) {
      std::vector<Value> value;
      for (const auto &column : plan_->OutputSchema()->GetColumns()) {
        value.push_back(column.GetExpr()->EvaluateAggregate(key.group_bys_, val.aggregates_));
      }
      *tuple = Tuple(value, plan_->OutputSchema());
      *rid = tuple->GetRid();
      return true;
    }

    return Next(tuple, rid);
  }
  return false;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
