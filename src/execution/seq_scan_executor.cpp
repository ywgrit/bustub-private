//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), table_heap_(nullptr), iter_(nullptr, RID(), nullptr) {}

void SeqScanExecutor::Init() {
  table_heap_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
  iter_ = table_heap_->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  // return false if iter_ is exhausted
  if (iter_ == table_heap_->End()) {
    return false;
  }

  RID res_rid = iter_->GetRid();
  const Schema *output_schema = plan_->OutputSchema();

  Transaction *txn = GetExecutorContext()->GetTransaction();
  LockManager *lock_manager = GetExecutorContext()->GetLockManager();
  if (lock_manager != nullptr) {
    if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      if (!txn->IsSharedLocked(res_rid) && !txn->IsExclusiveLocked(res_rid)) {
        lock_manager->LockShared(txn, res_rid);
      }
    }
  }

  size_t columns = output_schema->GetColumnCount();
  std::vector<Value> values;
  values.reserve(columns);
  for (size_t index = 0; index < columns; index++) {
    values.push_back(output_schema->GetColumn(index).GetExpr()->Evaluate(
        &(*iter_), &(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->schema_)));
  }

  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lock_manager != nullptr) {
    lock_manager->Unlock(txn, res_rid);
  }

  ++iter_;

  Tuple res_tuple(values, output_schema);

  // check whether the tuple is legal
  const AbstractExpression *predict = plan_->GetPredicate();
  if (predict == nullptr || predict->Evaluate(&res_tuple, output_schema).GetAs<bool>()) {
    *tuple = res_tuple;
    *rid = res_rid;
    return true;
  }

  return Next(tuple, rid);
}

}  // namespace bustub
