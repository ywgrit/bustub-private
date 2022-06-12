//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  bool res = false;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i) && cmp(key, array_[i].first) == 0) {
      result->push_back(array_[i].second);
      res = true;
    }
  }
  return res;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  int id = -1;
  for (uint32_t index = 0; index < BUCKET_ARRAY_SIZE; index++) {
    if (IsReadable(index)) {
      if (cmp(key, array_[index].first) == 0 && value == array_[index].second) {
        return false;
      }  // Allows to insert pairs of the same key but different values into an array, the first location found is the
         // insert location
    } else if (id == -1) {
      id = index;
    }
  }

  if (id == -1) {
    return false;
  }

  array_[id] = MappingType(key, value);
  /* std::cout << "array_.size() = " << sizeof(array_) / sizeof(MappingType) << std::endl; */
  /* for (auto mappair : array_) { */
  /* std::cout << mappair.first << " " << mappair.second << std::endl; */
  /* } */

  /* std::cout << "\n\n\n\n"; */

  /* std::cout << "Insert, key == " << key << ", value == " << value << ", id == " << id << " sizeof(array_) == " <<
   * sizeof(array_)/sizeof(MappingType) << " after insert, array_[id] == [" << array_[id].first << ", " <<
   * array_[id].second << "]" << std::endl; */

  SetOccupied(id);
  SetReadable(id);

  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  for (uint32_t index = 0; index < BUCKET_ARRAY_SIZE; index++) {
    if (IsReadable(index)) {
      if (cmp(key, array_[index].first) == 0 && value == array_[index].second) {
        RemoveAt(index);
        return true;
      }  // Allows to insert pairs of the same key but different values into an array, the first location found is the
         // insert location
    }
  }

  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(
    uint32_t bucket_idx) {  // just unset readable_ flag, doesn't need to remove pair from array_ actually
  if (IsReadable(bucket_idx)) {
    uint8_t c = static_cast<uint8_t>(readable_[bucket_idx / 8]);
    c &= (~(1 << (bucket_idx % 8)));
    readable_[bucket_idx / 8] = static_cast<char>(c);
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  uint8_t c = static_cast<uint8_t>(occupied_[bucket_idx / 8]);
  return (c & (1 << (bucket_idx % 8))) > 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  uint8_t c = static_cast<uint8_t>(occupied_[bucket_idx / 8]);
  c |= (1 << (bucket_idx % 8));
  occupied_[bucket_idx / 8] = static_cast<char>(c);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  uint8_t c = static_cast<uint8_t>(readable_[bucket_idx / 8]);
  return (c & (1 << (bucket_idx % 8))) > 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  uint8_t c = static_cast<uint8_t>(readable_[bucket_idx / 8]);
  c |= (1 << (bucket_idx % 8));
  readable_[bucket_idx / 8] = static_cast<char>(c);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  uint8_t mask = 0XFF;
  uint32_t num = BUCKET_ARRAY_SIZE / 8;
  for (uint32_t index = 0; index < num; index++) {
    uint8_t c = static_cast<uint8_t>(readable_[index]);
    if (c != mask) {
      return false;
    }
  }

  uint8_t id = BUCKET_ARRAY_SIZE % 8;
  for (uint8_t index = 0; index < id; index++) {
    if ((readable_[num] & (1 << index)) > 0) {
      return false;
    }
  }

  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  uint32_t i_num = BUCKET_ARRAY_SIZE / 8;
  uint32_t num = 0;
  for (uint32_t index = 0; index < i_num; index++) {
    uint8_t c = static_cast<uint8_t>(readable_[index]);
    for (uint8_t k = 0; k < 8; k++) {
      if ((c & 1) == 1) {
        num++;
      }
      c >>= 1;
    }
  }

  uint8_t c = static_cast<uint8_t>(readable_[i_num]);
  for (uint8_t k = 0; k < BUCKET_ARRAY_SIZE % 8; k++) {
    if ((c & 1) == 1) {
      num++;
    }
    c >>= 1;
  }

  return num;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  uint32_t num = sizeof(readable_) / sizeof(readable_[0]);
  for (uint32_t index = 0; index < num; index++) {
    if ((readable_[index] & 0XFF) != 0) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
MappingType *HASH_TABLE_BUCKET_TYPE::GetArrayCopy() {
  uint32_t num = NumReadable();
  MappingType *copy = new MappingType[num];
  for (uint32_t i = 0, index = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      copy[index++] = array_[i];
    }
  }
  return copy;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::Reset() {
  memset(occupied_, 0, sizeof(occupied_));
  memset(readable_, 0, sizeof(readable_));
  memset(array_, 0, sizeof(array_));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
