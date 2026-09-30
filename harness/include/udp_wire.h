// Compact UDP envelope for exactly one existing harness frame per datagram.
// The envelope is network byte order; the enclosed frame is deliberately left
// byte-for-byte unchanged so the fixed consumer can read it from SHM.
#pragma once

#include <arpa/inet.h>

#include <cstddef>
#include <cstdint>
#include <cstring>

#include "message.h"

namespace udp_wire {

inline constexpr uint32_t kMagic = 0x55445031u;  // "UDP1"
inline constexpr uint16_t kVersion = 1;
inline constexpr size_t kHeaderSize = 32;

enum class Format : uint16_t { kFull = 0, kCompact = 1 };

struct DecodedHeader {
  uint64_t session_id;
  uint64_t seq_id;
  uint32_t frame_len;
  Format format;
};

inline uint64_t host_to_be64(uint64_t value) {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  return __builtin_bswap64(value);
#else
  return value;
#endif
}

inline uint64_t be64_to_host(uint64_t value) { return host_to_be64(value); }

template <typename T>
inline void store(void* destination, size_t offset, T value) {
  std::memcpy(static_cast<uint8_t*>(destination) + offset, &value, sizeof(value));
}

template <typename T>
inline T load(const void* source, size_t offset) {
  T value{};
  std::memcpy(&value, static_cast<const uint8_t*>(source) + offset, sizeof(value));
  return value;
}

inline void encode(void* destination, uint64_t session_id, uint64_t seq_id,
                   uint32_t frame_len, Format format = Format::kFull) {
  std::memset(destination, 0, kHeaderSize);
  store<uint32_t>(destination, 0, htonl(kMagic));
  store<uint16_t>(destination, 4, htons(kVersion));
  store<uint16_t>(destination, 6, htons(kHeaderSize));
  store<uint64_t>(destination, 8, host_to_be64(session_id));
  store<uint64_t>(destination, 16, host_to_be64(seq_id));
  store<uint32_t>(destination, 24, htonl(frame_len));
  store<uint16_t>(destination, 28, htons(static_cast<uint16_t>(format)));
}

inline bool decode_prefix(const void* packet, size_t packet_len, DecodedHeader* out,
                          size_t* consumed) {
  if (packet_len < kHeaderSize) return false;
  if (ntohl(load<uint32_t>(packet, 0)) != kMagic ||
      ntohs(load<uint16_t>(packet, 4)) != kVersion ||
      ntohs(load<uint16_t>(packet, 6)) != kHeaderSize) {
    return false;
  }
  const uint32_t frame_len = ntohl(load<uint32_t>(packet, 24));
  const uint16_t format = ntohs(load<uint16_t>(packet, 28));
  if (frame_len < sizeof(msg::Header) || frame_len > msg::kMaxFrame ||
      packet_len < kHeaderSize + static_cast<size_t>(frame_len)) {
    return false;
  }
  if (format != static_cast<uint16_t>(Format::kFull) &&
      format != static_cast<uint16_t>(Format::kCompact)) return false;
  out->session_id = be64_to_host(load<uint64_t>(packet, 8));
  out->seq_id = be64_to_host(load<uint64_t>(packet, 16));
  out->frame_len = frame_len;
  out->format = static_cast<Format>(format);
  *consumed = kHeaderSize + static_cast<size_t>(frame_len);
  return out->session_id != 0 && out->seq_id != 0;
}

inline bool decode(const void* packet, size_t packet_len, DecodedHeader* out) {
  size_t consumed = 0;
  return decode_prefix(packet, packet_len, out, &consumed) && consumed == packet_len;
}

// Validate the fixed local frame before it is put on the network or output
// ring.  This also prevents a malformed datagram from becoming consumer input.
inline bool valid_frame(const void* frame, uint32_t len, uint64_t expected_seq) {
  if (len < sizeof(msg::Header) || len > msg::kMaxFrame) return false;
  msg::Header header{};
  std::memcpy(&header, frame, sizeof(header));
  if (header.seq_id != expected_seq || header.body_len != len) return false;
  switch (static_cast<msg::Type>(header.type)) {
    case msg::Type::Trade:
      return len == sizeof(msg::Trade);
    case msg::Type::Bbo:
      return len == sizeof(msg::Bbo);
    case msg::Type::OrderBook:
      return len == sizeof(msg::OrderBook);
    default:
      return false;
  }
}

inline constexpr size_t kMaxDatagram = kHeaderSize + msg::kMaxFrame;

}  // namespace udp_wire
