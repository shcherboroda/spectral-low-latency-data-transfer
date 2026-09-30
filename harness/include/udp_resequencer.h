// Bounded per-receiver sequencer: only publishes strictly increasing seq_id.
#pragma once

#include <cstdint>
#include <cstring>
#include <vector>

#include "message.h"

namespace udp_resequencer {

enum class InsertResult { kBuffered, kDuplicateOrLate, kTooFarAhead };

struct Counters {
  uint64_t released = 0;
  uint64_t declared_drops = 0;
  uint64_t duplicates_or_late = 0;
  uint64_t too_far_ahead = 0;
};

class Window {
 public:
  explicit Window(uint32_t slots) : slots_(slots), mask_(slots - 1) {}

  InsertResult insert(uint64_t seq_id, const uint8_t* frame, uint32_t frame_len,
                      uint64_t now_ns) {
    if (next_expected_ == 0) next_expected_ = seq_id;
    if (seq_id < next_expected_) {
      ++counters_.duplicates_or_late;
      return InsertResult::kDuplicateOrLate;
    }
    if (seq_id - next_expected_ >= slots_.size()) {
      ++counters_.too_far_ahead;
      return InsertResult::kTooFarAhead;
    }
    Slot& slot = slots_[static_cast<size_t>(seq_id) & mask_];
    if (slot.present) {
      ++counters_.duplicates_or_late;
      return InsertResult::kDuplicateOrLate;
    }
    slot.seq_id = seq_id;
    slot.frame_len = frame_len;
    std::memcpy(slot.frame, frame, frame_len);
    slot.present = true;
    ++buffered_slots_;
    if (seq_id > next_expected_ && gap_started_ns_ == 0) gap_started_ns_ = now_ns;
    return InsertResult::kBuffered;
  }

  template <typename Publish>
  void release_ready(uint64_t now_ns, uint64_t gap_wait_ns, Publish publish) {
    while (next_expected_ != 0) {
      Slot& slot = slots_[static_cast<size_t>(next_expected_) & mask_];
      if (slot.present && slot.seq_id == next_expected_) {
        publish(slot.frame, slot.frame_len);
        slot.present = false;
        --buffered_slots_;
        ++next_expected_;
        ++counters_.released;
        if (buffered_slots_ == 0) gap_started_ns_ = 0;
        continue;
      }
      // No future frame is buffered, so there is no evidence that this is a
      // gap rather than the normal end of currently received traffic.
      if (buffered_slots_ == 0) {
        gap_started_ns_ = 0;
        break;
      }
      if (gap_wait_ns == 0) {
        ++next_expected_;
        ++counters_.declared_drops;
        continue;
      }
      if (gap_started_ns_ == 0) {
        gap_started_ns_ = now_ns;
        break;
      }
      if (now_ns - gap_started_ns_ < gap_wait_ns) break;
      ++next_expected_;
      ++counters_.declared_drops;
      // A later buffered frame proves that this is a new, independent gap.
      // It must receive its own wait interval rather than inheriting the
      // deadline of the gap we have just declared lost.
      gap_started_ns_ = 0;
    }
  }

  uint64_t next_expected() const { return next_expected_; }
  const Counters& counters() const { return counters_; }

 private:
  struct Slot {
    uint64_t seq_id = 0;
    uint32_t frame_len = 0;
    bool present = false;
    uint8_t frame[msg::kMaxFrame]{};
  };

  std::vector<Slot> slots_;
  size_t mask_;
  uint64_t next_expected_ = 0;
  uint64_t gap_started_ns_ = 0;
  uint32_t buffered_slots_ = 0;
  Counters counters_{};
};

}  // namespace udp_resequencer
