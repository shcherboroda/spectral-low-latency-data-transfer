// Bounded diagnostic-only latency samples. Disabled callers do not read a clock.
#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>

namespace stage_metrics {

class Samples {
 public:
  static constexpr size_t kCapacity = 4096;

  // Callers use this before taking timestamps.  Once the bounded diagnostic
  // buffer is full, profiling must become a true no-op in the hot path.
  bool can_record() const { return count_ < kCapacity; }

  void record(uint64_t value_ns, uint64_t seq_id = 0) {
    if (count_ == kCapacity) return;
    values_[count_++] = value_ns;
    sum_ns_ += value_ns;
    if (seq_id != 0) {
      if (first_seq_ == 0 || seq_id < first_seq_) first_seq_ = seq_id;
      if (seq_id > last_seq_) last_seq_ = seq_id;
    }
  }

  void print(FILE* stream, const char* name) {
    if (count_ == 0) {
      std::fprintf(stream, "%s_samples=0\n", name);
      return;
    }
    std::sort(values_.begin(), values_.begin() + count_);
    std::fprintf(stream,
                 "%s_samples=%zu %s_first_seq=%llu %s_last_seq=%llu %s_mean_ns=%llu "
                 "%s_p50_ns=%llu %s_p99_ns=%llu %s_p999_ns=%llu %s_p9999_ns=%llu %s_max_ns=%llu\n",
                 name, count_, name, static_cast<unsigned long long>(first_seq_), name,
                 static_cast<unsigned long long>(last_seq_), name,
                 static_cast<unsigned long long>(sum_ns_ / count_),
                 name, static_cast<unsigned long long>(percentile(50)), name,
                 static_cast<unsigned long long>(percentile(99)), name,
                 static_cast<unsigned long long>(percentile_basis_points(9990)), name,
                 static_cast<unsigned long long>(percentile_basis_points(9999)), name,
                 static_cast<unsigned long long>(values_[count_ - 1]));
  }

 private:
  uint64_t percentile(uint32_t percent) const {
    const size_t index = (count_ * percent + 99) / 100 - 1;
    return values_[index];
  }

  uint64_t percentile_basis_points(uint32_t basis_points) const {
    const size_t index = (count_ * basis_points + 9999) / 10000 - 1;
    return values_[index];
  }

  std::array<uint64_t, kCapacity> values_{};
  size_t count_ = 0;
  uint64_t sum_ns_ = 0;
  uint64_t first_seq_ = 0;
  uint64_t last_seq_ = 0;
};

}  // namespace stage_metrics
