// Fixed-memory XOR(8+1) sender and receiver group state.
#pragma once

#include <array>
#include <cstdint>
#include <cstring>

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif

#include "fec_wire.h"

namespace fec_xor {

inline void xor_bytes_scalar(uint8_t* destination, const uint8_t* source, uint32_t len) {
  for (uint32_t i = 0; i < len; ++i) destination[i] ^= source[i];
}

#if defined(__x86_64__) || defined(__i386__)
__attribute__((target("avx2"))) inline void xor_bytes_avx2(
    uint8_t* destination, const uint8_t* source, uint32_t len) {
  uint32_t offset = 0;
  for (; offset + 32 <= len; offset += 32) {
    const __m256i left = _mm256_loadu_si256(
        reinterpret_cast<const __m256i*>(destination + offset));
    const __m256i right = _mm256_loadu_si256(
        reinterpret_cast<const __m256i*>(source + offset));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(destination + offset),
                        _mm256_xor_si256(left, right));
  }
  xor_bytes_scalar(destination + offset, source + offset, len - offset);
}
#endif

inline void xor_bytes(uint8_t* destination, const uint8_t* source, uint32_t len) {
#if defined(__x86_64__) || defined(__i386__)
  static const bool has_avx2 = __builtin_cpu_supports("avx2");
  if (has_avx2) { xor_bytes_avx2(destination, source, len); return; }
#endif
  xor_bytes_scalar(destination, source, len);
}

class Encoder {
 public:
  explicit Encoder(uint16_t group_size = fec_wire::kGroupSize)
      : group_size_(group_size) {}

  void set_group_size(uint16_t group_size) {
    group_size_ = group_size;
    reset();
  }
  struct DataInfo {
    uint64_t group_id = 0;
    uint16_t shard_index = 0;
  };
  struct ParityInfo {
    uint64_t group_id = 0;
    uint16_t shard_count = 0;
    uint32_t frame_len = 0;
    const uint8_t* bytes = nullptr;
  };

  bool add(const uint8_t* frame, uint32_t frame_len, uint64_t* next_group_id,
           DataInfo* data, ParityInfo* completed) {
    if (count_ == 0) {
      group_id_ = (*next_group_id)++;
      frame_len_ = frame_len;
      std::memset(parity_.data(), 0, frame_len_);
    }
    if (frame_len != frame_len_) return false;
    data->group_id = group_id_;
    data->shard_index = count_;
    xor_bytes(parity_.data(), frame, frame_len_);
    ++count_;
    if (count_ != group_size_) return false;
    *completed = parity_info();
    reset();
    return true;
  }

  bool flush(ParityInfo* completed) {
    if (count_ == 0) return false;
    *completed = parity_info();
    reset();
    return true;
  }

 private:
  ParityInfo parity_info() const {
    return ParityInfo{group_id_, count_, frame_len_, parity_.data()};
  }
  void reset() { count_ = 0; }

  uint64_t group_id_ = 0;
  uint16_t group_size_ = fec_wire::kGroupSize;
  uint16_t count_ = 0;
  uint32_t frame_len_ = 0;
  alignas(64) std::array<uint8_t, msg::kMaxFrame> parity_{};
};

class Group {
 public:
  bool add_data(const fec_wire::Header& header, const uint8_t* frame) {
    if (!bind(header)) return false;
    const uint8_t bit = static_cast<uint8_t>(1u << header.shard_index);
    if ((present_mask_ & bit) != 0) return true;
    xor_bytes(data_xor_.data(), frame, frame_len_);
    present_mask_ |= bit;
    return true;
  }

  bool add_parity(const fec_wire::Header& header, const uint8_t* parity) {
    if (!bind(header) || header.kind != fec_wire::Kind::kParity) return false;
    if (has_parity_) return true;
    expected_count_ = header.shard_count;
    std::memcpy(parity_.data(), parity, frame_len_);
    has_parity_ = true;
    return true;
  }

  bool recover(uint8_t* out, uint32_t* out_len) {
    if (recovered_ || !has_parity_ || expected_count_ == 0 ||
        popcount(present_mask_) != expected_count_ - 1) {
      return false;
    }
    const uint8_t full_mask = static_cast<uint8_t>((1u << expected_count_) - 1u);
    const uint8_t missing = static_cast<uint8_t>(full_mask & ~present_mask_);
    if (missing == 0 || (missing & static_cast<uint8_t>(missing - 1)) != 0) return false;
    std::memcpy(out, parity_.data(), frame_len_);
    xor_bytes(out, data_xor_.data(), frame_len_);
    *out_len = frame_len_;
    recovered_ = true;
    return true;
  }

  bool belongs_to(uint64_t session_id, uint64_t group_id) const {
    return initialized_ && session_id_ == session_id && group_id_ == group_id;
  }
  bool compact() const { return compact_; }
  bool active() const { return initialized_; }
  bool complete() const {
    return !recovered_ && has_parity_ && expected_count_ != 0 &&
           popcount(present_mask_) == expected_count_;
  }
  void reset() { initialized_ = false; }

 private:
  bool bind(const fec_wire::Header& header) {
    if (!initialized_) {
      initialized_ = true;
      session_id_ = header.session_id;
      group_id_ = header.group_id;
      frame_len_ = header.frame_len;
      compact_ = header.compact;
      expected_count_ = 0;
      present_mask_ = 0;
      has_parity_ = false;
      recovered_ = false;
      std::memset(data_xor_.data(), 0, frame_len_);
    }
    return session_id_ == header.session_id && group_id_ == header.group_id &&
           frame_len_ == header.frame_len && compact_ == header.compact;
  }

  static uint16_t popcount(uint8_t value) {
    uint16_t count = 0;
    while (value != 0) { value &= static_cast<uint8_t>(value - 1); ++count; }
    return count;
  }

  bool initialized_ = false;
  bool has_parity_ = false;
  bool recovered_ = false;
  uint64_t session_id_ = 0;
  uint64_t group_id_ = 0;
  uint32_t frame_len_ = 0;
  bool compact_ = false;
  uint16_t expected_count_ = 0;
  uint8_t present_mask_ = 0;
  alignas(64) std::array<uint8_t, msg::kMaxFrame> data_xor_{};
  alignas(64) std::array<uint8_t, msg::kMaxFrame> parity_{};
};

}  // namespace fec_xor
