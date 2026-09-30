// Fixed-capacity de-duplication window for a single UDP session.
#pragma once

#include <cstdint>
#include <vector>

namespace udp_deduper {

enum class Result { kAccept, kDuplicate, kTooOld };

class Window {
 public:
  explicit Window(uint32_t slots) : tags_(slots, 0), mask_(slots - 1) {}

  Result observe(uint64_t seq_id) {
    if (max_seen_ != 0 && max_seen_ >= tags_.size() &&
        seq_id <= max_seen_ - tags_.size()) {
      return Result::kTooOld;
    }
    const size_t index = static_cast<size_t>(seq_id) & mask_;
    if (tags_[index] == seq_id) return Result::kDuplicate;
    tags_[index] = seq_id;
    if (seq_id > max_seen_) max_seen_ = seq_id;
    return Result::kAccept;
  }

 private:
  std::vector<uint64_t> tags_;  // allocated once before the receive hot path
  size_t mask_;
  uint64_t max_seen_ = 0;
};

inline bool is_power_of_two(uint32_t value) {
  return value != 0 && (value & (value - 1)) == 0;
}

}  // namespace udp_deduper
