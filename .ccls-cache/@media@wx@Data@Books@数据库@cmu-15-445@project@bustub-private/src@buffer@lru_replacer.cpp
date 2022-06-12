//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : capacity_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  const std::lock_guard<std::mutex> guardmutex(
      mtx_);  // because lock_guard wouldn't be changed, so it's should be const

  if (lt_.empty()) {
    return false;
  }
  *frame_id = lt_.front();
  lt_.pop_front();

  auto it = mp_.find(*frame_id);
  if (it != mp_.end()) {
    mp_.erase(it);
    return true;
  }

  std::cout << "lt.front() not in unordered_map, it's wried" << std::endl;
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  const std::lock_guard<std::mutex> guardmutex(mtx_);

  auto it = mp_.find(frame_id);
  if (it != mp_.end()) {
    lt_.erase(it->second);
    mp_.erase(it);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  const std::lock_guard<std::mutex> guardmutex(mtx_);

  if (lt_.size() >= capacity_) {
    return;
  }

  if (mp_.find(frame_id) == mp_.end()) {
    lt_.push_back(frame_id);
    mp_[frame_id] = std::prev(lt_.end());
  }
}

size_t LRUReplacer::Size() {
  const std::lock_guard<std::mutex> guardmutex(
      mtx_);  // although Size() doesn't change data, it maybe access wrong data when other threads execute function
              // which will change data in parallel
  return lt_.size();
}

}  // namespace bustub
