//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <chrono>
#include <cstddef>
#include <limits>
#include <mutex>
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  begin_timestamp_ = std::chrono::high_resolution_clock::now();
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  auto guard = std::scoped_lock(lrukreplacer_latch_);
  auto evictable = false;
  auto evictable_id = frame_id_t{};
  auto isinf = false;
  auto timestamp = std::numeric_limits<size_t>::max();

  auto take_this_frame = [&](frame_id_t id_, size_t timestamp_) {
    evictable = true;
    evictable_id = id_;
    timestamp = timestamp_;
  };

  for (const auto &[id, node] : node_store_) {
    if (!node.is_evictable_) {
      continue;
    }
    if (node.history_.size() < k_) {
      if (!isinf || node.history_.back() < timestamp) {
        isinf = true;
        take_this_frame(id, node.history_.back());
      }
    } else {
      BUSTUB_ASSERT(node.history_.size() == k_, "size of history_ in LRUKNode should always equal to or less than k");
      if (!isinf && node.history_.back() < timestamp) {
        take_this_frame(id, node.history_.back());
      }
    }
  }

  if (evictable) {
    *frame_id = evictable_id;
    node_store_.erase(evictable_id);
    curr_size_--;
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  EnsureFrameIdValid(frame_id);

  auto guard = std::scoped_lock(lrukreplacer_latch_);
  if (!HaveFrameId(frame_id)) {
    node_store_.insert({frame_id, {}});
  }
  auto &history = node_store_.at(frame_id).history_;
  // record the most recent timestamp in front
  history.push_front(CurrentTimeStamp());
  // tail history to only record the most recent k_ timestamps
  while (history.size() > k_) {
    history.pop_back();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  EnsureFrameIdValid(frame_id);

  auto guard = std::scoped_lock(lrukreplacer_latch_);
  auto &evictable = node_store_.at(frame_id).is_evictable_;
  if (evictable && !set_evictable) {
    evictable = set_evictable;
    curr_size_--;
  } else if (!evictable && set_evictable) {
    evictable = set_evictable;
    curr_size_++;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  auto guard = std::scoped_lock(lrukreplacer_latch_);
  node_store_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

void LRUKReplacer::EnsureFrameIdValid(frame_id_t frame_id) {
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw Exception("Invalid frame id");
  }
}

auto LRUKReplacer::HaveFrameId(frame_id_t frame_id) -> bool { return node_store_.find(frame_id) != node_store_.end(); }

auto LRUKReplacer::CurrentTimeStamp() -> size_t {
  auto elapsed = std::chrono::high_resolution_clock::now() - begin_timestamp_;
  return static_cast<size_t>(std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count());
}

}  // namespace bustub
