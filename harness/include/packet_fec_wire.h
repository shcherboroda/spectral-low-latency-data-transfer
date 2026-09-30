// Packet-level XOR(4+1) envelope. The protected payload is one complete UDB2
// datagram; parity XORs zero-padded payload bytes and is sent separately.
#pragma once

#include <arpa/inet.h>

#include <cstddef>
#include <cstdint>
#include <cstring>

#include "fec_xor.h"
#include "udp_wire.h"

namespace packet_fec_wire {

inline constexpr uint32_t kMagic = 0x50463131u;  // "PF11"
inline constexpr uint16_t kVersion = 1;
inline constexpr size_t kHeaderSize = 28;
inline constexpr size_t kMaxPayload = 1472 - kHeaderSize;
inline constexpr uint8_t kGroupSize = 4;

enum class Kind : uint8_t { kData = 0, kParity = 1 };

struct Header {
  Kind kind{};
  uint8_t shard_index = 0;
  uint8_t shard_count = 0;
  uint64_t session_id = 0;
  uint32_t group_id = 0;
  uint32_t payload_len = 0;
};

inline void encode(void* destination, Kind kind, uint64_t session_id,
                   uint32_t group_id, uint8_t shard_index, uint8_t shard_count,
                   uint32_t payload_len) {
  std::memset(destination, 0, kHeaderSize);
  udp_wire::store<uint32_t>(destination, 0, htonl(kMagic));
  udp_wire::store<uint16_t>(destination, 4, htons(kVersion));
  udp_wire::store<uint16_t>(destination, 6, htons(kHeaderSize));
  udp_wire::store<uint8_t>(destination, 8, static_cast<uint8_t>(kind));
  udp_wire::store<uint8_t>(destination, 9, shard_index);
  udp_wire::store<uint8_t>(destination, 10, shard_count);
  udp_wire::store<uint64_t>(destination, 12, udp_wire::host_to_be64(session_id));
  udp_wire::store<uint32_t>(destination, 20, htonl(group_id));
  udp_wire::store<uint32_t>(destination, 24, htonl(payload_len));
}

inline bool decode(const void* packet, size_t packet_len, Header* out) {
  if (packet_len < kHeaderSize ||
      ntohl(udp_wire::load<uint32_t>(packet, 0)) != kMagic ||
      ntohs(udp_wire::load<uint16_t>(packet, 4)) != kVersion ||
      ntohs(udp_wire::load<uint16_t>(packet, 6)) != kHeaderSize) return false;
  const uint8_t kind = udp_wire::load<uint8_t>(packet, 8);
  out->kind = static_cast<Kind>(kind);
  out->shard_index = udp_wire::load<uint8_t>(packet, 9);
  out->shard_count = udp_wire::load<uint8_t>(packet, 10);
  out->session_id = udp_wire::be64_to_host(udp_wire::load<uint64_t>(packet, 12));
  out->group_id = ntohl(udp_wire::load<uint32_t>(packet, 20));
  out->payload_len = ntohl(udp_wire::load<uint32_t>(packet, 24));
  return (kind == static_cast<uint8_t>(Kind::kData) || kind == static_cast<uint8_t>(Kind::kParity)) &&
         out->session_id != 0 && out->group_id != 0 && out->payload_len != 0 &&
         ((out->kind == Kind::kData && out->shard_count == 0 && out->shard_index < kGroupSize) ||
          (out->kind == Kind::kData && out->shard_count >= 1 && out->shard_count <= kGroupSize && out->shard_index < out->shard_count) ||
          (out->kind == Kind::kParity && out->shard_count >= 1 && out->shard_count <= kGroupSize && out->shard_index == out->shard_count)) &&
         out->payload_len <= kMaxPayload && packet_len == kHeaderSize + out->payload_len;
}

class Encoder {
 public:
  struct DataInfo { uint32_t group_id = 0; uint8_t shard_index = 0; };
  struct ParityInfo { uint32_t group_id = 0; uint8_t shard_count = 0; uint32_t payload_len = 0; const uint8_t* bytes = nullptr; };

  bool add(const uint8_t* bytes, uint32_t bytes_len, uint32_t* next_group_id,
           DataInfo* data, ParityInfo* completed) {
    if (bytes_len == 0 || bytes_len > kMaxPayload) return false;
    if (count_ == 0) {
      group_id_ = (*next_group_id)++;
      // Zero is reserved as an invalid on-wire group id. Skip it on the
      // 32-bit wrap boundary rather than creating one undecodable group.
      if (group_id_ == 0) group_id_ = (*next_group_id)++;
      max_len_ = 0;
      std::memset(parity_.data(), 0, parity_.size());
    }
    data->group_id = group_id_; data->shard_index = count_;
    fec_xor::xor_bytes(parity_.data(), bytes, bytes_len);
    if (bytes_len > max_len_) max_len_ = bytes_len;
    ++count_;
    if (count_ != kGroupSize) return false;
    *completed = parity_info(); reset(); return true;
  }
  bool flush(ParityInfo* completed) { if (count_ == 0) return false; *completed = parity_info(); reset(); return true; }
 private:
  ParityInfo parity_info() const { return {group_id_, count_, max_len_, parity_.data()}; }
  void reset() { count_ = 0; }
  uint32_t group_id_ = 0, max_len_ = 0; uint8_t count_ = 0;
  alignas(64) std::array<uint8_t, kMaxPayload> parity_{};
};

class Group {
 public:
  bool add_data(const Header& header, const uint8_t* bytes) {
    if (!bind(header) || header.kind != Kind::kData || header.shard_index >= kGroupSize ||
        (header.shard_count != 0 && header.shard_index >= header.shard_count) ||
        (shard_count_ != 0 && header.shard_count != 0 && header.shard_count != shard_count_)) return false;
    const uint8_t bit = static_cast<uint8_t>(1u << header.shard_index);
    if (present_ & bit) return true;
    fec_xor::xor_bytes(data_xor_.data(), bytes, header.payload_len); present_ |= bit; return true;
  }
  bool add_parity(const Header& header, const uint8_t* bytes) {
    if (!bind(header) || header.kind != Kind::kParity || header.shard_index != header.shard_count ||
        (shard_count_ != 0 && shard_count_ != header.shard_count)) return false;
    if (has_parity_) return true;
    shard_count_ = header.shard_count;
    std::memcpy(parity_.data(), bytes, header.payload_len); parity_len_ = header.payload_len; has_parity_ = true; return true;
  }
  bool recover(uint8_t* out, uint32_t* out_len) {
    if (recovered_ || !has_parity_ || popcount(present_) != shard_count_ - 1) return false;
    const uint8_t mask = static_cast<uint8_t>((1u << shard_count_) - 1u);
    const uint8_t missing = static_cast<uint8_t>(mask & ~present_);
    if (missing == 0 || (missing & static_cast<uint8_t>(missing - 1))) return false;
    std::memcpy(out, parity_.data(), parity_len_); fec_xor::xor_bytes(out, data_xor_.data(), parity_len_);
    *out_len = parity_len_; recovered_ = true; return true;
  }
  bool belongs_to(uint64_t session_id, uint32_t group_id) const { return initialized_ && session_id_ == session_id && group_id_ == group_id; }
  bool active() const { return initialized_; }
  bool complete() const {
    return recovered_ || (has_parity_ && popcount(present_) == shard_count_);
  }
  void reset() { initialized_ = false; }
 private:
  bool bind(const Header& header) {
    if (!initialized_) { initialized_=true; session_id_=header.session_id; group_id_=header.group_id; shard_count_=header.kind == Kind::kData ? header.shard_count : 0; present_=0; has_parity_=false; recovered_=false; std::memset(data_xor_.data(),0,data_xor_.size()); }
    return session_id_==header.session_id && group_id_==header.group_id;
  }
  static uint8_t popcount(uint8_t value) { uint8_t count=0; while(value){ value &= static_cast<uint8_t>(value-1); ++count;} return count; }
  bool initialized_=false, has_parity_=false, recovered_=false; uint64_t session_id_=0; uint32_t group_id_=0, parity_len_=0; uint8_t shard_count_=0, present_=0;
  alignas(64) std::array<uint8_t,kMaxPayload> data_xor_{}, parity_{};
};

}  // namespace packet_fec_wire
