//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
/* #include "common/config.h" */
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  directory_page_id_ = INVALID_PAGE_ID;
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::PrintMemberDirectory() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();

  page_id_t page_id = dir_page->GetBucketPageId(0);
  Page *raw_page = FetchBucketPage(page_id);
  HASH_TABLE_BUCKET_TYPE *bucket = GetBucketPageData(raw_page);
  bucket->PrintBucket();

  /* std::cout << "The following is directory_page\n\n\n\n\n"; */
  /* dir_page->PrintDirectory(); */
  /* std::cout << "\n\n\n\n\n"; */
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::GetBucketPageData(Page *page) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  HashTableDirectoryPage *directory_page;

  // Create directory_page if not exist
  directory_latch_.lock();
  if (directory_page_id_ == INVALID_PAGE_ID) {
    page_id_t page_id;
    Page *raw_page = buffer_pool_manager_->NewPage(&page_id);
    assert(raw_page != nullptr);  // use assert appropriately
    directory_page_id_ = page_id;
    directory_page = reinterpret_cast<HashTableDirectoryPage *>(
        raw_page->GetData());  // convert page->GetData() rather than page* to HashTableDirectoryPage*
    directory_page->SetPageId(directory_page_id_);

    // Create the first bucket for this new directory_page
    raw_page = buffer_pool_manager_->NewPage(&page_id);
    assert(raw_page != nullptr);
    directory_page->SetBucketPageId(0, page_id);

    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));
    assert(buffer_pool_manager_->UnpinPage(page_id, true));
  }
  directory_latch_.unlock();

  assert(directory_page_id_ != INVALID_PAGE_ID);
  Page *raw_page = buffer_pool_manager_->FetchPage(directory_page_id_);
  assert(raw_page != nullptr);

  return reinterpret_cast<HashTableDirectoryPage *>(raw_page->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
Page *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  Page *raw_page = buffer_pool_manager_->FetchPage(bucket_page_id);
  assert(raw_page != nullptr);
  return raw_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::GetBucket(uint32_t bucket_idx) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  Page *bucket_page = FetchBucketPage(bucket_page_id);
  return GetBucketPageData(bucket_page);
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t page_id = KeyToPageId(key, dir_page);
  Page *raw_page = buffer_pool_manager_->FetchPage(page_id);
  if (raw_page == nullptr) {
    return false;
  }

  raw_page->RLatch();
  HASH_TABLE_BUCKET_TYPE *buc_page = GetBucketPageData(raw_page);
  bool res = buc_page->GetValue(key, comparator_, result);
  raw_page->RUnlatch();

  // must unpin page fetched before
  assert(buffer_pool_manager_->UnpinPage(page_id, false));
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));

  table_latch_.RUnlock();

  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t page_id = KeyToPageId(key, dir_page);
  Page *raw_page = buffer_pool_manager_->FetchPage(page_id);
  if (raw_page == nullptr) {
    return false;
  }
  raw_page->WLatch();
  HASH_TABLE_BUCKET_TYPE *buc_page = GetBucketPageData(raw_page);

  if (!buc_page->IsFull()) {
    bool res = buc_page->Insert(key, value, comparator_);

    raw_page->WUnlatch();
    assert(buffer_pool_manager_->UnpinPage(page_id, true));
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.RUnlock();
    return res;
  }

  raw_page->WUnlatch();
  assert(buffer_pool_manager_->UnpinPage(page_id, false));
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
  table_latch_.RUnlock();
  return SplitInsert(transaction, key, value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t split_bucket_idx = KeyToDirectoryIndex(key, dir_page);
  /* std::cout << "split_bucket_idx == " << split_bucket_idx << std::endl; */
  /* std::cout << "before split, the directory is:" << std::endl; */
  /* dir_page->PrintDirectory(); */
  /* std::cout << "\n\n"; */

  uint32_t split_depth = dir_page->GetLocalDepth(split_bucket_idx);

  // if directory's size is full, then can't split any more
  if (split_depth > MAX_BUCKET_DEPTH) {
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.WUnlock();
    return false;
  }

  uint32_t global_depth = dir_page->GetGlobalDepth();
  if (split_depth == global_depth) {
    dir_page->IncrGlobalDepth();
  }
  dir_page->IncrLocalDepth(split_bucket_idx);

  page_id_t split_page_id = KeyToPageId(key, dir_page);
  Page *split_page = buffer_pool_manager_->FetchPage(split_page_id);
  assert(split_page != nullptr);
  split_page->WLatch();
  HASH_TABLE_BUCKET_TYPE *split_bucket = GetBucketPageData(split_page);
  uint32_t split_array_size = split_bucket->NumReadable();
  MappingType *split_array = split_bucket->GetArrayCopy();
  /* std::cout << "split_array[" << split_array_size - 1 << "].first == " << split_array[split_array_size - 1].first <<
   * " split_bucket->IsReadable(split_array_size - 1) == " << split_bucket->IsReadable(split_array_size - 1) <<
   * std::endl; */
  split_bucket->Reset();

  page_id_t bro_page_id;
  Page *bro_page = buffer_pool_manager_->NewPage(&bro_page_id);
  assert(bro_page != nullptr);
  bro_page->WLatch();
  HASH_TABLE_BUCKET_TYPE *bro_bucket = GetBucketPageData(bro_page);
  uint32_t bro_bucket_idx = dir_page->GetSplitImageIndex(split_bucket_idx);
  dir_page->SetBucketPageId(bro_bucket_idx, bro_page_id);
  dir_page->SetLocalDepth(bro_bucket_idx, dir_page->GetLocalDepth(split_bucket_idx));

  /* PrintMemberDirectory(); */
  /* dir_page->PrintDirectory(); */

  /* std::cout << "The following is split_array" << std::endl; */
  /* for (uint32_t index = 0; index < split_array_size; index++) { */
  /*     std::cout << split_array[index].first << " " << split_array[index].second << std::endl; */
  /* } */

  /* std::cout << "split_array end" << std::endl; */

  for (uint32_t index = 0; index < split_array_size; index++) {
    uint32_t target_index = Hash(split_array[index].first) & dir_page->GetLocalDepthMask(split_bucket_idx);
    page_id_t target_page_id = dir_page->GetBucketPageId(target_index);
    /* std::cout << "split_array[" << index << "].first == " << split_array[index].first << " split_bucket_idx == " <<
     * split_bucket_idx << " bro_bucket_idx == " << bro_bucket_idx << " target_index == " << target_index << "
     * target_page_id == " << target_page_id << std::endl; */
    assert(target_page_id == split_page_id || target_page_id == bro_page_id);
    if (target_page_id == split_page_id) {
      assert(split_bucket->Insert(split_array[index].first, split_array[index].second, comparator_));
    } else {
      assert(bro_bucket->Insert(split_array[index].first, split_array[index].second, comparator_));
    }
  }
  delete[] split_array;

  split_depth++;
  uint32_t interval = 1 << dir_page->GetLocalDepth(split_bucket_idx);
  uint32_t split_start = split_bucket_idx % interval;
  for (uint32_t index = split_start; index < dir_page->Size(); index += interval) {
    dir_page->SetBucketPageId(index, split_page_id);
    dir_page->SetLocalDepth(index, split_depth);
  }
  uint32_t bro_start = bro_bucket_idx % interval;
  for (uint32_t index = bro_start; index < dir_page->Size(); index += interval) {
    dir_page->SetBucketPageId(index, bro_page_id);
    dir_page->SetLocalDepth(index, split_depth);
  }

  /* std::cout << "split_bucket's local depth == " << dir_page->GetLocalDepth(split_bucket_idx) << " bro_bucket's local
   * depth" << dir_page->GetLocalDepth(bro_bucket_idx) << std::endl; */

  split_page->WUnlatch();
  bro_page->WUnlatch();

  assert(buffer_pool_manager_->UnpinPage(split_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(bro_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true));

  table_latch_.WUnlock();

  Insert(transaction, key, value);

  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  Page *raw_page = FetchBucketPage(bucket_page_id);
  raw_page->WLatch();
  HASH_TABLE_BUCKET_TYPE *bucket_page = GetBucketPageData(raw_page);

  bool res = bucket_page->Remove(key, value, comparator_);

  raw_page->WUnlatch();
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
  table_latch_.RUnlock();

  if (bucket_page->IsEmpty()) {
    Merge(transaction, key, value);
  }

  return res;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t delete_page_id = KeyToPageId(key, dir_page);
  uint32_t delete_bucket_idx = KeyToDirectoryIndex(key, dir_page);
  uint32_t bro_bucket_idx = dir_page->GetSplitImageIndex(delete_bucket_idx);
  uint32_t delete_depth = dir_page->GetLocalDepth(delete_bucket_idx);

  // can't merge in following situation, Note: can't combine these situations together, because if delete_depth == 0,
  // then dir_page->GetLocalDepth(bro_bucket_idx) maybe run error
  if (delete_depth == 0) {
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.WUnlock();
    return;
  }

  uint32_t bro_depth = dir_page->GetLocalDepth(bro_bucket_idx);
  if (delete_depth != bro_depth) {
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.WUnlock();
    return;
  }

  page_id_t bro_page_id = dir_page->GetBucketPageId(bro_bucket_idx);
  Page *delete_page = FetchBucketPage(delete_page_id);
  delete_page->RLatch();
  HASH_TABLE_BUCKET_TYPE *delete_bucket = GetBucketPageData(delete_page);

  if (!delete_bucket->IsEmpty()) {
    delete_page->RUnlatch();
    assert(buffer_pool_manager_->UnpinPage(delete_page_id, false));
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.WUnlock();
    return;
  }

  // delete empty bucket
  delete_page->RUnlatch();
  assert(buffer_pool_manager_->UnpinPage(delete_page_id, false));
  assert(buffer_pool_manager_->DeletePage(delete_page_id));

  delete_depth--;
  dir_page->DecrLocalDepth(delete_bucket_idx);
  dir_page->DecrLocalDepth(bro_bucket_idx);
  dir_page->SetBucketPageId(delete_bucket_idx, bro_page_id);
  assert(dir_page->GetLocalDepth(delete_bucket_idx) == dir_page->GetLocalDepth(bro_bucket_idx));

  for (uint32_t index = 0; index < dir_page->Size(); index++) {
    page_id_t traverse_page_id = dir_page->GetBucketPageId(index);
    if (traverse_page_id == delete_page_id || traverse_page_id == bro_page_id) {
      dir_page->SetLocalDepth(index, delete_depth);
      dir_page->SetBucketPageId(index, bro_page_id);
    }
  }

  while (dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
  }

  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
