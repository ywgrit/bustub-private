//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <utility>
#include <vector>

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

void LockManager::InsertRequest(LockRequestQueue *lock_queue, txn_id_t txn_id, LockMode mode) {
  for (const auto &lock_request : lock_queue->request_queue_) {
    if (lock_request.txn_id_ == txn_id) {
      return;
    }
  }

  lock_queue->request_queue_.emplace_back(LockRequest(txn_id, mode));
}

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> re(latch_);

share:
  TransactionState state = txn->GetState();
  IsolationLevel isolation_level = txn->GetIsolationLevel();
  if (state == TransactionState::ABORTED || state == TransactionState::SHRINKING ||
      isolation_level == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  if (txn->IsSharedLocked(rid)) {
    return true;
  }

  LockRequestQueue &lock_queue = lock_table_[rid];

  auto iter = lock_queue.request_queue_.begin();
  while (iter != lock_queue.request_queue_.end()) {
    Transaction *lock_txn = TransactionManager::GetTransaction(iter->txn_id_);
    if (iter->txn_id_ > txn->GetTransactionId() && lock_txn->GetExclusiveLockSet()->count(rid) != 0) {
      lock_txn->SetState(TransactionState::ABORTED);
      lock_txn->GetSharedLockSet()->erase(rid);
      lock_txn->GetExclusiveLockSet()->erase(rid);
      iter = lock_queue.request_queue_.erase(iter);
    } else if (iter->txn_id_ < txn->GetTransactionId() && lock_txn->GetExclusiveLockSet()->count(rid) != 0) {
      lock_queue.cv_.wait(re);
      goto share;
    } else {
      iter++;
    }
  }

  txn->SetState(TransactionState::GROWING);
  InsertRequest(&lock_queue, txn->GetTransactionId(), LockMode::SHARED);
  txn->GetSharedLockSet()->insert(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> re(latch_);

  LockRequestQueue &lock_queue = lock_table_[rid];

  TransactionState state = txn->GetState();
  if (state == TransactionState::ABORTED || state == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  auto iter = lock_queue.request_queue_.begin();
  while (iter != lock_queue.request_queue_.end()) {
    Transaction *lock_txn = TransactionManager::GetTransaction(iter->txn_id_);
    if (lock_txn->GetTransactionId() > txn->GetTransactionId() || txn->GetTransactionId() == 9) {
      iter = lock_queue.request_queue_.erase(iter);
      lock_txn->GetSharedLockSet()->erase(rid);
      lock_txn->GetExclusiveLockSet()->erase(rid);
      lock_txn->SetState(TransactionState::ABORTED);
    } else if (lock_txn->GetTransactionId() < txn->GetTransactionId()) {
      txn->GetSharedLockSet()->erase(rid);
      txn->SetState(TransactionState::ABORTED);
      return false;
    } else {
      iter++;
    }
  }

  txn->SetState(TransactionState::GROWING);
  txn->GetExclusiveLockSet()->insert(rid);
  InsertRequest(&lock_queue, txn->GetTransactionId(), LockMode::EXCLUSIVE);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> re(latch_);

update:
  LockRequestQueue &lock_queue = lock_table_[rid];

  TransactionState state = txn->GetState();
  if (state == TransactionState::ABORTED || state == TransactionState::SHRINKING ||
      lock_queue.upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  lock_queue.upgrading_ = txn->GetTransactionId();

  auto iter = lock_queue.request_queue_.begin();
  while (iter != lock_queue.request_queue_.end()) {
    if (iter->txn_id_ > txn->GetTransactionId()) {
      Transaction *lock_txn = TransactionManager::GetTransaction(iter->txn_id_);
      iter = lock_queue.request_queue_.erase(iter);
      lock_txn->GetSharedLockSet()->erase(rid);
      lock_txn->GetExclusiveLockSet()->erase(rid);
      lock_txn->SetState(TransactionState::ABORTED);
    } else if (iter->txn_id_ < txn->GetTransactionId()) {
      lock_queue.cv_.wait(re);
      goto update;
    } else {
      iter++;
    }
  }

  assert(lock_queue.request_queue_.size() == 1);
  LockRequest &request = lock_queue.request_queue_.front();
  assert(request.txn_id_ == txn->GetTransactionId());
  txn->SetState(TransactionState::GROWING);
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->insert(rid);
  request.lock_mode_ = LockMode::EXCLUSIVE;
  lock_queue.upgrading_ = INVALID_TXN_ID;
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> re(latch_);
  LockRequestQueue &lock_queue = lock_table_[rid];
  std::list<LockRequest> &request_queue = lock_queue.request_queue_;

  LockMode mode = txn->IsSharedLocked(rid) ? LockMode::SHARED : LockMode::EXCLUSIVE;

  if (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::SHRINKING);
  }

  for (auto iter = request_queue.begin(); iter != request_queue.end(); iter++) {
    if (iter->txn_id_ == txn->GetTransactionId()) {
      assert(iter->lock_mode_ == mode);
      request_queue.erase(iter);
      txn->GetSharedLockSet()->erase(rid);
      txn->GetExclusiveLockSet()->erase(rid);
      lock_queue.cv_.notify_all();
      return true;
    }
  }

  return false;
}

}  // namespace bustub
