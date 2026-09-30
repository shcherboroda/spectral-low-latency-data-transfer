// Version-2 UDP envelope for systematic XOR FEC. Data frames remain unchanged
// inside the envelope; parity frames contain the XOR of one equal-length group.
#pragma once

#include <arpa/inet.h>

#include <cstddef>
#include <cstdint>
#include <cstring>

#include "message.h"
#include "udp_wire.h"

namespace fec_wire {

inline constexpr uint32_t kMagic = 0x46454331u;  // "FEC1"
inline constexpr uint16_t kVersion = 2;
inline constexpr uint16_t kHeaderSize = 32;
inline constexpr uint16_t kMaxGroupSize = 8;
inline constexpr uint16_t kGroupSize = kMaxGroupSize;  // Legacy default: 8.

enum class Kind : uint16_t { kData = 1, kParity = 2 };

struct Header {
  Kind kind;
  uint64_t session_id;
  uint64_t group_id;
  uint16_t shard_index;
  uint16_t shard_count;
  uint32_t frame_len;
  bool compact = false;
};

inline void encode(void* destination, Kind kind, uint64_t session_id,
                   uint64_t group_id, uint16_t shard_index,
                   uint16_t shard_count, uint32_t frame_len, bool compact = false) {
  std::memset(destination, 0, kHeaderSize);
  udp_wire::store<uint32_t>(destination, 0, htonl(kMagic));
  udp_wire::store<uint16_t>(destination, 4, htons(kVersion));
  udp_wire::store<uint16_t>(destination, 6, htons(static_cast<uint16_t>(kind)));
  udp_wire::store<uint64_t>(destination, 8, udp_wire::host_to_be64(session_id));
  udp_wire::store<uint64_t>(destination, 16, udp_wire::host_to_be64(group_id));
  udp_wire::store<uint16_t>(destination, 24, htons(shard_index));
  const uint16_t encoded_shard_count = static_cast<uint16_t>(
      shard_count | (compact ? 0x8000u : 0u));
  udp_wire::store<uint16_t>(destination, 26, htons(encoded_shard_count));
  udp_wire::store<uint32_t>(destination, 28, htonl(frame_len));
}

inline bool decode_prefix(const void* packet, size_t packet_len, Header* out,
                          size_t* consumed) {
  if (packet_len < kHeaderSize) return false;
  if (ntohl(udp_wire::load<uint32_t>(packet, 0)) != kMagic ||
      ntohs(udp_wire::load<uint16_t>(packet, 4)) != kVersion) {
    return false;
  }
  const uint16_t encoded_kind = ntohs(udp_wire::load<uint16_t>(packet, 6));
  if (encoded_kind != static_cast<uint16_t>(Kind::kData) &&
      encoded_kind != static_cast<uint16_t>(Kind::kParity)) {
    return false;
  }
  const uint64_t session_id = udp_wire::be64_to_host(udp_wire::load<uint64_t>(packet, 8));
  const uint64_t group_id = udp_wire::be64_to_host(udp_wire::load<uint64_t>(packet, 16));
  const uint16_t shard_index = ntohs(udp_wire::load<uint16_t>(packet, 24));
  const uint16_t encoded_shard_count = ntohs(udp_wire::load<uint16_t>(packet, 26));
  const bool compact = (encoded_shard_count & 0x8000u) != 0;
  const uint16_t shard_count = static_cast<uint16_t>(encoded_shard_count & 0x7fffu);
  const uint32_t frame_len = ntohl(udp_wire::load<uint32_t>(packet, 28));
  const Kind kind = static_cast<Kind>(encoded_kind);
  if (session_id == 0 || group_id == 0 || shard_count == 0 ||
      shard_count > kMaxGroupSize || frame_len < sizeof(msg::Header) ||
      frame_len > msg::kMaxFrame || packet_len < kHeaderSize + frame_len) {
    return false;
  }
  if ((kind == Kind::kData && (shard_index >= shard_count || shard_index >= kMaxGroupSize)) ||
      (kind == Kind::kParity && (shard_index < shard_count || shard_index >= shard_count + 2))) {
    return false;
  }
  out->kind = kind;
  out->compact = compact;
  out->session_id = session_id;
  out->group_id = group_id;
  out->shard_index = shard_index;
  out->shard_count = shard_count;
  out->frame_len = frame_len;
  *consumed = kHeaderSize + frame_len;
  return true;
}

inline bool decode(const void* packet, size_t packet_len, Header* out) {
  size_t consumed = 0;
  return decode_prefix(packet, packet_len, out, &consumed) && consumed == packet_len;
}

inline constexpr size_t kMaxDatagram = kHeaderSize + msg::kMaxFrame;

}  // namespace fec_wire
