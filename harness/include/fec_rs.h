// Systematic Reed-Solomon-style 8+2 erasure recovery over GF(256).
#pragma once

#include <array>
#include <cstdint>
#include <cstring>

#include "fec_wire.h"

namespace fec_rs {

inline uint8_t mul(uint8_t a, uint8_t b) {
  uint8_t out = 0;
  while (b != 0) {
    if (b & 1) out ^= a;
    const bool high = (a & 0x80) != 0;
    a <<= 1;
    if (high) a ^= 0x1d;
    b >>= 1;
  }
  return out;
}

inline uint8_t pow2(uint16_t exponent) {
  uint8_t out = 1;
  while (exponent-- != 0) out = mul(out, 2);
  return out;
}

inline uint8_t inv(uint8_t value) {
  uint8_t out = 1;
  uint8_t base = value;
  uint16_t power = 254;
  while (power != 0) {
    if (power & 1) out = mul(out, base);
    base = mul(base, base);
    power >>= 1;
  }
  return out;
}

inline void add_scaled(uint8_t* dst, const uint8_t* src, uint8_t coefficient, uint32_t len) {
  for (uint32_t i = 0; i < len; ++i) dst[i] ^= mul(src[i], coefficient);
}

class Encoder {
 public:
  explicit Encoder(uint16_t group_size = fec_wire::kGroupSize)
      : group_size_(group_size) {}

  void set_group_size(uint16_t group_size) {
    group_size_ = group_size;
    count_ = 0;
  }
  struct DataInfo { uint64_t group_id = 0; uint16_t shard_index = 0; };
  struct ParityInfo {
    uint64_t group_id = 0; uint16_t shard_count = 0; uint32_t frame_len = 0;
    std::array<const uint8_t*, 2> bytes{};
  };

  bool add(const uint8_t* frame, uint32_t frame_len, uint64_t* next_group_id,
           DataInfo* data, ParityInfo* completed) {
    if (count_ == 0) { group_id_ = (*next_group_id)++; frame_len_ = frame_len; parity_[0].fill(0); parity_[1].fill(0); }
    if (frame_len != frame_len_) return false;
    data->group_id = group_id_; data->shard_index = count_;
    add_scaled(parity_[0].data(), frame, 1, frame_len_);
    add_scaled(parity_[1].data(), frame, pow2(count_), frame_len_);
    if (++count_ != group_size_) return false;
    *completed = parity_info(); count_ = 0; return true;
  }

  bool flush(ParityInfo* completed) {
    if (count_ == 0) return false;
    *completed = parity_info(); count_ = 0; return true;
  }

 private:
  ParityInfo parity_info() const { return {group_id_, count_, frame_len_, {parity_[0].data(), parity_[1].data()}}; }
  uint64_t group_id_ = 0; uint16_t group_size_ = fec_wire::kGroupSize; uint16_t count_ = 0; uint32_t frame_len_ = 0;
  std::array<std::array<uint8_t, msg::kMaxFrame>, 2> parity_{};
};

class Group {
 public:
  bool add_data(const fec_wire::Header& h, const uint8_t* frame) {
    if (h.kind != fec_wire::Kind::kData || h.shard_index >= fec_wire::kMaxGroupSize ||
        !bind_identity(h) || (count_ != 0 && h.shard_index >= count_)) {
      return false;
    }
    const uint8_t bit = static_cast<uint8_t>(1u << h.shard_index);
    if ((present_ & bit) != 0) return true;
    std::memcpy(data_[h.shard_index].data(), frame, frame_len_); present_ |= bit; return true;
  }
  bool add_parity(const fec_wire::Header& h, const uint8_t* parity) {
    if (!bind_identity(h) || h.shard_index < h.shard_count ||
        h.shard_index >= h.shard_count + 2) return false;
    // Data datagrams announce the nominal 8-shard group because the sender
    // cannot know that a stream will end early.  The parity datagram carries
    // the authoritative count for a flushed partial group.
    if (count_ == 0) count_ = h.shard_count;
    if (count_ != h.shard_count || (present_ & ~mask_for(count_)) != 0) return false;
    const uint16_t index = h.shard_index - h.shard_count;
    if (parity_present_[index]) return true;
    std::memcpy(parity_[index].data(), parity, frame_len_); parity_present_[index] = true; return true;
  }
  uint16_t recover(std::array<std::array<uint8_t, msg::kMaxFrame>, 2>* output, uint32_t* out_len) {
    if (recovered_) return 0;
    const uint8_t full = static_cast<uint8_t>((1u << count_) - 1u);
    const uint8_t missing = static_cast<uint8_t>(full & ~present_);
    const uint16_t missing_count = popcount(missing);
    if (missing_count == 0 || missing_count > 2 || parity_count() < missing_count) return 0;
    uint16_t indices[2]{}; uint16_t n = 0;
    for (uint16_t i = 0; i < count_; ++i) if (missing & (1u << i)) indices[n++] = i;
    std::array<uint8_t, msg::kMaxFrame> s0{}, s1{};
    if (parity_present_[0]) std::memcpy(s0.data(), parity_[0].data(), frame_len_);
    if (parity_present_[1]) std::memcpy(s1.data(), parity_[1].data(), frame_len_);
    for (uint16_t i = 0; i < count_; ++i) if ((present_ & (1u << i)) != 0) {
      if (parity_present_[0]) add_scaled(s0.data(), data_[i].data(), 1, frame_len_);
      if (parity_present_[1]) add_scaled(s1.data(), data_[i].data(), pow2(i), frame_len_);
    }
    if (missing_count == 1) {
      const uint16_t x = indices[0];
      if (parity_present_[0]) std::memcpy((*output)[0].data(), s0.data(), frame_len_);
      else { const uint8_t inv_a = inv(pow2(x)); for (uint32_t j = 0; j < frame_len_; ++j) (*output)[0][j] = mul(s1[j], inv_a); }
      *out_len = frame_len_; recovered_ = true; return 1;
    }
    if (!parity_present_[0] || !parity_present_[1]) return 0;
    const uint8_t a = pow2(indices[0]), b = pow2(indices[1]), inv_diff = inv(a ^ b);
    for (uint32_t j = 0; j < frame_len_; ++j) {
      (*output)[0][j] = mul(s1[j] ^ mul(b, s0[j]), inv_diff);
      (*output)[1][j] = (*output)[0][j] ^ s0[j];
    }
    *out_len = frame_len_; recovered_ = true; return 2;
  }
  bool belongs_to(uint64_t s, uint64_t g) const { return active_ && session_ == s && group_ == g; }
  bool compact() const { return compact_; }
  bool active() const { return active_; }
  // The sender always emits both parity shards.  Keep the group alive after
  // the first one so that the second cannot resurrect a reset group.
  bool complete() const {
    return !recovered_ && count_ != 0 && popcount(present_) == count_ &&
           parity_count() == 2;
  }
  void reset() { active_ = false; }
 private:
  static uint8_t mask_for(uint16_t count) {
    return static_cast<uint8_t>((1u << count) - 1u);
  }
  bool bind_identity(const fec_wire::Header& h) {
    if (!active_) {
      active_ = true;
      session_ = h.session_id;
      group_ = h.group_id;
      frame_len_ = h.frame_len;
      compact_ = h.compact;
      count_ = 0;
      present_ = 0;
      parity_present_ = {false, false};
      recovered_ = false;
    }
    return session_ == h.session_id && group_ == h.group_id && frame_len_ == h.frame_len && compact_ == h.compact;
  }
  static uint16_t popcount(uint8_t x) { uint16_t n=0; while(x){x&=x-1;++n;} return n; }
  uint16_t parity_count() const { return parity_present_[0] + parity_present_[1]; }
  bool active_=false; bool recovered_=false; bool compact_=false; uint64_t session_=0, group_=0; uint16_t count_=0; uint32_t frame_len_=0; uint8_t present_=0; std::array<bool,2> parity_present_{};
  std::array<std::array<uint8_t,msg::kMaxFrame>,8> data_{}; std::array<std::array<uint8_t,msg::kMaxFrame>,2> parity_{};
};

}  // namespace fec_rs
