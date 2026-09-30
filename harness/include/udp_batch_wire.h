// Datagram framing for a non-waiting batch of complete UDP1 envelopes.
#pragma once

#include <arpa/inet.h>

#include <cstddef>
#include <cstdint>
#include <cstring>

#include "udp_wire.h"

namespace udp_batch_wire {

inline constexpr uint32_t kMagic = 0x55444231u;  // "UDB1"
inline constexpr uint16_t kVersion = 1;
inline constexpr uint16_t kPacketFecVersion = 2;
inline constexpr size_t kHeaderSize = 24;
inline constexpr size_t kMaxDatagram = 1472;  // Ethernet MTU 1500 without IPv4/UDP headers.

struct Header {
  uint64_t session_id = 0;
  uint16_t frame_count = 0;
  size_t packet_bytes = 0;
};

inline void encode(void* destination, uint64_t session_id, uint16_t frame_count) {
  std::memset(destination, 0, kHeaderSize);
  udp_wire::store<uint32_t>(destination, 0, htonl(kMagic));
  udp_wire::store<uint16_t>(destination, 4, htons(kVersion));
  udp_wire::store<uint16_t>(destination, 6, htons(kHeaderSize));
  udp_wire::store<uint16_t>(destination, 8, htons(frame_count));
  udp_wire::store<uint64_t>(destination, 16, udp_wire::host_to_be64(session_id));
}

// Version two retains the same 24-byte header but records the exact used
// length. Packet-level FEC XORs zero-padded datagrams, so a recovered packet
// needs an intrinsic boundary rather than relying on recvfrom's byte count.
inline void encode_packet_fec(void* destination, uint64_t session_id,
                              uint16_t frame_count, uint32_t packet_bytes) {
  std::memset(destination, 0, kHeaderSize);
  udp_wire::store<uint32_t>(destination, 0, htonl(kMagic));
  udp_wire::store<uint16_t>(destination, 4, htons(kPacketFecVersion));
  udp_wire::store<uint16_t>(destination, 6, htons(kHeaderSize));
  udp_wire::store<uint16_t>(destination, 8, htons(frame_count));
  udp_wire::store<uint32_t>(destination, 12, htonl(packet_bytes));
  udp_wire::store<uint64_t>(destination, 16, udp_wire::host_to_be64(session_id));
}

inline bool decode(const void* packet, size_t packet_len, Header* out) {
  if (packet_len < kHeaderSize ||
      ntohl(udp_wire::load<uint32_t>(packet, 0)) != kMagic ||
      (ntohs(udp_wire::load<uint16_t>(packet, 4)) != kVersion &&
       ntohs(udp_wire::load<uint16_t>(packet, 4)) != kPacketFecVersion) ||
      ntohs(udp_wire::load<uint16_t>(packet, 6)) != kHeaderSize) return false;
  const uint16_t version = ntohs(udp_wire::load<uint16_t>(packet, 4));
  out->frame_count = ntohs(udp_wire::load<uint16_t>(packet, 8));
  out->session_id = udp_wire::be64_to_host(udp_wire::load<uint64_t>(packet, 16));
  out->packet_bytes = version == kPacketFecVersion
      ? ntohl(udp_wire::load<uint32_t>(packet, 12)) : packet_len;
  return out->session_id != 0 && out->frame_count > 0 &&
         out->packet_bytes >= kHeaderSize && out->packet_bytes <= packet_len;
}

}  // namespace udp_batch_wire
