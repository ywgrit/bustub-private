//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      catalog_(nullptr),
      table_info_(nullptr),
      child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  catalog_ = exec_ctx_->GetCatalog();
  table_info_ = catalog_->GetTable(plan_->TableOid());
  table_heap_ = table_info_->table_.get();
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (plan_->IsRawInsert()) {
    for (auto const &raw_value : plan_->RawValues()) {
      Tuple ori_tuple = Tuple(raw_value, &(table_info_->schema_));
      InsertTableAndUpdateIndex(&ori_tuple);
    }
    return false;
  }

  std::vector<Tuple> pro_tuples;
  child_executor_->Init();
  try {
    Tuple tuple;
    RID rid;
    while (child_executor_->Next(&tuple, &rid)) {
      pro_tuples.push_back(tuple);
    }
  } catch (Exception &e) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, "InsertExecutor: child_executor_ execute error.");
    return false;
  }

  for (auto &pro_tuple : pro_tuples) {
    InsertTableAndUpdateIndex(&pro_tuple);
  }
  return false;
}

void InsertExecutor::InsertTableAndUpdateIndex(Tuple *tuple) {
  RID rid;

  if (!table_heap_->InsertTuple(*tuple, &rid, exec_ctx_->GetTransaction())) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "InsertExecutor:no enough space for this tuple");
  }

  Transaction *txn = exec_ctx_->GetTransaction();
  LockManager *lock_mgr = GetExecutorContext()->GetLockManager();
  if (lock_mgr != nullptr) {
    if (txn->IsSharedLocked(rid)) {
      lock_mgr->LockUpgrade(txn, rid);
    } else if (!txn->IsExclusiveLocked(rid)) {
      lock_mgr->LockExclusive(txn, rid);
    }
  }

  for (const auto &indexinfo : catalog_->GetTableIndexes(table_info_->name_)) {
    indexinfo->index_->InsertEntry(
        tuple->KeyFromTuple(table_info_->schema_, *indexinfo->index_->GetKeySchema(), indexinfo->index_->GetKeyAttrs()),
        rid, exec_ctx_->GetTransaction());
    txn->GetIndexWriteSet()->emplace_back(IndexWriteRecord(rid, table_info_->oid_, WType::INSERT, *tuple,
                                                           indexinfo->index_oid_, exec_ctx_->GetCatalog()));
  }

  if (txn->GetIsolationLevel() != IsolationLevel::REPEATABLE_READ && lock_mgr != nullptr) {
    lock_mgr->Unlock(txn, rid);
  }
}

}  // namespace bustub
