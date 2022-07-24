//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), table_info_(nullptr), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid()); }

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple del_tuple;
  RID del_rid;
  Transaction *txn = exec_ctx_->GetTransaction();
  LockManager *lock_manager = GetExecutorContext()->GetLockManager();
  child_executor_->Init();

  while (true) {
    try {
      if (!child_executor_->Next(&del_tuple, &del_rid)) {
        break;
      }
    } catch (Exception &e) {
      throw Exception(ExceptionType::UNKNOWN_TYPE, "InsertExecutor: child_executor_ execute error.");
      return false;
    }

    if (lock_manager != nullptr) {
      if (txn->IsSharedLocked(del_rid)) {
        lock_manager->LockUpgrade(txn, del_rid);
      } else if (!txn->IsExclusiveLocked(del_rid)) {
        lock_manager->LockExclusive(txn, del_rid);
      }
    }

    TableHeap *tableheap = table_info_->table_.get();
    tableheap->MarkDelete(del_rid, txn);

    for (const auto &indexinfo : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
      auto index_info = indexinfo->index_.get();
      index_info->DeleteEntry(
          del_tuple.KeyFromTuple(table_info_->schema_, *index_info->GetKeySchema(), index_info->GetKeyAttrs()), del_rid,
          exec_ctx_->GetTransaction());
      txn->GetIndexWriteSet()->emplace_back(IndexWriteRecord(del_rid, table_info_->oid_, WType::DELETE, del_tuple,
                                                             indexinfo->index_oid_, exec_ctx_->GetCatalog()));
    }

    if (txn->GetIsolationLevel() != IsolationLevel::REPEATABLE_READ && lock_manager != nullptr) {
      lock_manager->Unlock(txn, del_rid);
    }
  }

  return false;
}

}  // namespace bustub
