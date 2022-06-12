//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  const std::lock_guard<std::mutex> guardmutex(latch_);
  auto it = page_table_.find(page_id);
  if (page_id != INVALID_PAGE_ID && it != page_table_.end()) {
    auto frame = &pages_[it->second];  // it is better to use pointer to page rather than page, because copy will occur
                                       // if use page, copy is expensive and maybe not allowed
    disk_manager_->WritePage(frame->GetPageId(), frame->GetData());
    frame->is_dirty_ = false;
    return true;
  }

  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  const std::lock_guard<std::mutex> guardmutex(latch_);
  for (auto it : page_table_) {
    auto frame = &pages_[it.second];
    if (frame->IsDirty()) {
      FlushPgImp(it.first);
      disk_manager_->WritePage(frame->GetPageId(), frame->GetData());
      frame->is_dirty_ = false;
    }
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  const std::lock_guard<std::mutex> guardmutex(latch_);

  bool all_pinned = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPinCount() <= 0) {  // TODO(wx): == 0 or <= 0 ?
      all_pinned = false;
      break;
    }
  }

  if (all_pinned) {
    return nullptr;
  }

  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    bool vict_found = replacer_->Victim(&frame_id);

    if (!vict_found) {
      return nullptr;
    }
  }

  auto frame = &pages_[frame_id];
  page_table_.erase(frame->GetPageId());

  if (frame->IsDirty()) {  // if frame->IsDirty() == true, then frame must not belong to free_list, so frame can flush
                           // to disk safely
    disk_manager_->WritePage(frame->GetPageId(), frame->GetData());
    frame->is_dirty_ = false;
  }

  *page_id = AllocatePage();
  frame->page_id_ = *page_id;
  frame->pin_count_++;
  frame->ResetMemory();  // this frame maybe filled in something belongs to other page, so need to clear it
  page_table_[*page_id] = frame_id;
  replacer_->Pin(frame_id);

  return &pages_[frame_id];
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  const std::lock_guard<std::mutex> guardmutex(latch_);

  Page *frame;
  frame_id_t frame_id;

  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    frame_id = it->second;
    frame = &pages_[frame_id];
    frame->pin_count_++;

    replacer_->Pin(frame_id);

    return frame;
  }

  bool all_pinned = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPinCount() <= 0) {  // TODO(wx): == 0 or <= 0 ?
      all_pinned = false;
      break;
    }
  }

  if (all_pinned) {
    return nullptr;
  }

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    bool vict_found = replacer_->Victim(&frame_id);
    if (!vict_found) {
      return nullptr;
    }
  }

  frame = &pages_[frame_id];

  if (frame->IsDirty()) {  // if frame->IsDirty() == true, then frame must not belong to free_list, so frame can flush
                           // to disk safely
    disk_manager_->WritePage(frame->GetPageId(), frame->GetData());
    frame->is_dirty_ = false;
  }

  page_table_.erase(frame->GetPageId());
  page_table_[page_id] = frame_id;

  frame->page_id_ = page_id;
  frame->pin_count_++;

  disk_manager_->ReadPage(page_id, frame->GetData());

  replacer_->Pin(frame_id);
  return frame;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  const std::lock_guard<std::mutex> guardmutex(latch_);

  auto it = page_table_.find(page_id);

  if (it == page_table_.end()) {
    return true;
  }

  auto frame_id = it->second;
  auto frame = &pages_[frame_id];
  if (frame->GetPinCount() > 0) {  // TODO(wx): > 0 or != 0 ? two ways: 1、deduct 1 from frame->pin_count anyway 2、just
                                   // return if frame->pin_count > 0. so which way should I choose?
    return false;
  }

  if (frame->IsDirty()) {
    disk_manager_->WritePage(page_id, frame->GetData());
  }

  frame->page_id_ = INVALID_PAGE_ID;
  frame->pin_count_ = 0;
  frame->is_dirty_ = false;
  frame->ResetMemory();

  DeallocatePage(page_id);

  page_table_.erase(page_id);

  free_list_.push_back(frame_id);

  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  const std::lock_guard<std::mutex> guardmutex(latch_);

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {  // page has been delected
    return true;
  }

  auto frame_id = it->second;
  auto frame = &pages_[frame_id];

  frame->is_dirty_ = frame->is_dirty_ || is_dirty;

  if (frame->GetPinCount() <= 0) {
    return false;
  }

  frame->pin_count_--;
  if (frame->pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }

  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
