//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DistinctExecutor::Init() {
  Tuple tuple;
  RID rid;
  child_executor_->Init();
  try {
    while (child_executor_->Next(&tuple, &rid)) {
      DistinctKey distinctkey = MakeDistinctKey(&tuple);
      hash_table_[distinctkey] = tuple;
    }
  } catch (Exception &e) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, " distinct_executor: child_executor_ execute error. ");
  }
  iter_ = hash_table_.begin();
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  if (iter_ != hash_table_.end()) {
    *tuple = iter_->second;
    *rid = tuple->GetRid();
    ++iter_;
    return true;
  }
  return false;
}

}  // namespace bustub
