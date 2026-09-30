// UDP transport receiver: validates and de-duplicates datagrams, then publishes
// the unchanged fixed frame into an output SHM ring for the fixed consumer.
#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/resource.h>
#include <unistd.h>

#include <cerrno>
#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include "fec_wire.h"
#include "fec_xor.h"
#include "packet_fec_wire.h"
#include "fec_rs.h"
#include "compact_wire.h"
#include "shm_ring.h"
#include "shm_segment.h"
#include "stage_metrics.h"
#include "udp_deduper.h"
#include "udp_resequencer.h"
#include "udp_wire.h"
#include "udp_batch_wire.h"
#include "util.h"

namespace {

inline constexpr uint64_t kMaxMilliseconds =
    std::numeric_limits<uint64_t>::max() / 1000000ull;
inline constexpr size_t kReceiveBatch = 16;

uint64_t timeval_ns(const timeval& value) {
  return static_cast<uint64_t>(value.tv_sec) * 1000000000ull +
         static_cast<uint64_t>(value.tv_usec) * 1000ull;
}

struct Config {
  std::string out_shm = "/fanout_udp_ring";
  uint32_t out_slots = 1024;
  std::string bind_host = "0.0.0.0";
  uint16_t port = 9000;
  uint64_t count = 0;
  uint64_t idle_ms = 2000;
  uint64_t session_id = 0;  // 0 accepts the first valid session
  uint32_t dedupe_window = 65536;
  uint32_t reorder_window = 1024;
  // Low-latency default. Larger values can improve recovery after reordering,
  // but keep subsequent messages behind a missing sequence for longer.
  uint64_t reorder_wait_us = 25;
  bool reorder_wait_auto = false;
  uint32_t receive_batch = 1;
  uint64_t post_count_drain_us = 1000;
  int socket_buffer = 4 * 1024 * 1024;
  bool socket_drop_counters = false;
  bool fec_xor8 = false;
  bool fec_rs8_2 = false;
  bool fec_packet_xor4 = false;
  uint16_t fec_group_size = 8;
  uint32_t batch_bytes = 0;
  uint32_t profile_sample_every = 0;
  uint64_t profile_from_seq = 0;
  bool compact_wire = false;
};

uint64_t parse_u64(const std::string& value, const char* flag) {
  try {
    if (value.empty() || value.front() == '-') throw std::invalid_argument("negative");
    size_t consumed = 0;
    const uint64_t parsed = std::stoull(value, &consumed);
    if (consumed != value.size()) throw std::invalid_argument("trailing characters");
    return parsed;
  } catch (...) {
    std::fprintf(stderr, "invalid value for %s: %s\n", flag, value.c_str());
    std::exit(2);
  }
}

template <typename T>
T parse_bounded(const std::string& value, const char* flag) {
  const uint64_t parsed = parse_u64(value, flag);
  if (parsed > static_cast<uint64_t>(std::numeric_limits<T>::max())) {
    std::fprintf(stderr, "value for %s is out of range: %s\n", flag, value.c_str());
    std::exit(2);
  }
  return static_cast<T>(parsed);
}

Config parse_args(int argc, char** argv) {
  Config c;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    auto next = [&]() -> std::string {
      if (i + 1 == argc) {
        std::fprintf(stderr, "missing value for %s\n", arg.c_str());
        std::exit(2);
      }
      return argv[++i];
    };
    if (arg == "--out-shm") c.out_shm = next();
    else if (arg == "--out-slots") c.out_slots = parse_bounded<uint32_t>(next(), "--out-slots");
    else if (arg == "--bind") c.bind_host = next();
    else if (arg == "--port") c.port = parse_bounded<uint16_t>(next(), "--port");
    else if (arg == "--count") c.count = parse_u64(next(), "--count");
    else if (arg == "--idle-ms") c.idle_ms = parse_u64(next(), "--idle-ms");
    else if (arg == "--session-id") c.session_id = parse_u64(next(), "--session-id");
    else if (arg == "--dedupe-window") c.dedupe_window = parse_bounded<uint32_t>(next(), "--dedupe-window");
    else if (arg == "--reorder-window") c.reorder_window = parse_bounded<uint32_t>(next(), "--reorder-window");
    else if (arg == "--reorder-wait-us") {
      const std::string value = next();
      if (value == "auto") c.reorder_wait_auto = true;
      else c.reorder_wait_us = parse_u64(value, "--reorder-wait-us");
    }
    else if (arg == "--recv-batch") c.receive_batch = parse_bounded<uint32_t>(next(), "--recv-batch");
    else if (arg == "--post-count-drain-us") c.post_count_drain_us = parse_u64(next(), "--post-count-drain-us");
    else if (arg == "--socket-buffer") c.socket_buffer = parse_bounded<int>(next(), "--socket-buffer");
    else if (arg == "--socket-drop-counters") c.socket_drop_counters = true;
    else if (arg == "--batch-bytes") c.batch_bytes = parse_bounded<uint32_t>(next(), "--batch-bytes");
    else if (arg == "--profile-sample-every") c.profile_sample_every = parse_bounded<uint32_t>(next(), "--profile-sample-every");
    else if (arg == "--profile-from-seq") c.profile_from_seq = parse_u64(next(), "--profile-from-seq");
    else if (arg == "--fec") {
      const std::string mode = next();
      if (mode == "none") c.fec_xor8 = c.fec_rs8_2 = c.fec_packet_xor4 = false;
      else if (mode == "xor4") { c.fec_xor8 = true; c.fec_rs8_2 = c.fec_packet_xor4 = false; c.fec_group_size = 4; }
      else if (mode == "xor8") { c.fec_xor8 = true; c.fec_rs8_2 = c.fec_packet_xor4 = false; c.fec_group_size = 8; }
      else if (mode == "rs4_2") { c.fec_xor8 = c.fec_packet_xor4 = false; c.fec_rs8_2 = true; c.fec_group_size = 4; }
      else if (mode == "rs8_2") { c.fec_xor8 = c.fec_packet_xor4 = false; c.fec_rs8_2 = true; c.fec_group_size = 8; }
      else if (mode == "packet_xor4") { c.fec_xor8 = c.fec_rs8_2 = false; c.fec_packet_xor4 = true; c.fec_group_size = 4; }
      else {
        std::fprintf(stderr, "--fec must be none, xor4, xor8, rs4_2, rs8_2, or packet_xor4\n");
        std::exit(2);
      }
    }
    else if (arg == "--wire-format") {
      const std::string format = next();
      if (format == "full") c.compact_wire = false;
      else if (format == "compact") c.compact_wire = true;
      else {
        std::fprintf(stderr, "--wire-format must be full or compact\n");
        std::exit(2);
      }
    }
    else {
      std::fprintf(stderr, "unknown arg: %s\n", arg.c_str());
      std::exit(2);
    }
  }
  if (c.port == 0 || !udp_deduper::is_power_of_two(c.out_slots) ||
      !udp_deduper::is_power_of_two(c.dedupe_window) || c.socket_buffer < 0) {
    std::fprintf(stderr, "invalid --port, slot/window power of two, or --socket-buffer\n");
    std::exit(2);
  }
  if (!udp_deduper::is_power_of_two(c.reorder_window) ||
      c.reorder_wait_us > std::numeric_limits<uint64_t>::max() / 1000ull ||
      c.post_count_drain_us > std::numeric_limits<uint64_t>::max() / 1000ull) {
    std::fprintf(stderr, "invalid reorder window or microsecond timeout\n");
    std::exit(2);
  }
  if (c.receive_batch == 0 || c.receive_batch > kReceiveBatch) {
    std::fprintf(stderr, "--recv-batch must be from 1 through %zu\n", kReceiveBatch);
    std::exit(2);
  }
  if (c.idle_ms > kMaxMilliseconds) {
    std::fprintf(stderr, "millisecond timeout is out of range\n");
    std::exit(2);
  }
  if (c.batch_bytes != 0 && c.batch_bytes > udp_batch_wire::kMaxDatagram) {
    std::fprintf(stderr, "--batch-bytes must not exceed %zu\n",
                 udp_batch_wire::kMaxDatagram);
    std::exit(2);
  }
  if (c.fec_packet_xor4 && (!c.compact_wire || c.batch_bytes == 0)) {
    std::fprintf(stderr, "--fec packet_xor4 requires --wire-format compact and --batch-bytes\n");
    std::exit(2);
  }
  return c;
}

int open_socket(const Config& cfg, bool* socket_drop_counters_available) {
  *socket_drop_counters_available = false;
  const int fd = socket(AF_INET, SOCK_DGRAM, 0);
  if (fd < 0) return -1;
  int reuse = 1;
  (void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
  if (fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | O_NONBLOCK) != 0) {
    close(fd);
    return -1;
  }
  if (cfg.socket_buffer > 0 &&
      setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &cfg.socket_buffer, sizeof(cfg.socket_buffer)) != 0) {
    std::fprintf(stderr, "warning: SO_RCVBUF: %s\n", std::strerror(errno));
  }
  if (cfg.socket_drop_counters) {
    int enabled = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_RXQ_OVFL, &enabled, sizeof(enabled)) == 0) {
      *socket_drop_counters_available = true;
    } else {
      std::fprintf(stderr, "warning: SO_RXQ_OVFL: %s\n", std::strerror(errno));
    }
  }
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(cfg.port);
  if (inet_pton(AF_INET, cfg.bind_host.c_str(), &address.sin_addr) != 1) {
    std::fprintf(stderr, "--bind must be an IPv4 address: %s\n",
                 cfg.bind_host.c_str());
    close(fd);
    return -2;
  }
  if (bind(fd, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0) {
    const int saved_errno = errno;
    close(fd);
    errno = saved_errno;
    return -1;
  }
  return fd;
}

}  // namespace

int main(int argc, char** argv) {
  const Config cfg = parse_args(argc, argv);
  bool socket_drop_counters_available = false;
  const int socket_fd = open_socket(cfg, &socket_drop_counters_available);
  if (socket_fd == -2) return 2;
  if (socket_fd < 0) {
    std::fprintf(stderr, "cannot bind UDP socket: %s\n", std::strerror(errno));
    return 1;
  }

  shm::Segment segment = shm::Segment::open(
      cfg.out_shm, shm::region_size(cfg.out_slots), /*create=*/true);
  shm::Ring output;
  output.attach(segment.base(), cfg.out_slots, /*init=*/true);
  udp_resequencer::Window resequencer(cfg.reorder_window);
  std::fprintf(stderr, "udp_receiver: ready shm=%s slots=%u port=%u recv_batch=%u\n",
               cfg.out_shm.c_str(), cfg.out_slots, cfg.port, cfg.receive_batch);

  uint64_t accepted = 0, invalid = 0, duplicates = 0, too_old = 0;
  uint64_t session_mismatch = 0, active_session = cfg.session_id;
  uint64_t fec_parity = 0, fec_recovered = 0, fec_evicted = 0;
  uint64_t min_observed_seq = 0, max_observed_seq = 0;
  bool fatal_socket_error = false;
  uint64_t last_progress = util::now_ns();
  const uint64_t idle_ns = cfg.idle_ms * 1000000ull;
  constexpr uint64_t kAutoReorderWaitMinNs = 25'000;
  constexpr uint64_t kAutoReorderWaitMaxNs = 100'000;
  constexpr uint64_t kAutoReorderPeriods = 6;
  uint64_t reorder_wait_ns = cfg.reorder_wait_us * 1000ull;
  uint64_t last_timing_seq = 0, last_timing_send_ns = 0;
  const uint64_t post_count_drain_ns = cfg.post_count_drain_us * 1000ull;
  // The socket remains nonblocking. recvmmsg() never waits to fill this
  // batch: it drains datagrams that are already queued after the first one.
  alignas(64) std::array<std::array<uint8_t, udp_batch_wire::kMaxDatagram>, kReceiveBatch>
      packets{};
  std::array<iovec, kReceiveBatch> receive_iov{};
  std::array<mmsghdr, kReceiveBatch> receive_messages{};
  struct alignas(cmsghdr) ReceiveControl {
    std::array<char, CMSG_SPACE(sizeof(uint32_t))> bytes{};
  };
  std::array<ReceiveControl, kReceiveBatch> receive_controls{};
  for (size_t i = 0; i < kReceiveBatch; ++i) {
    receive_iov[i].iov_base = packets[i].data();
    receive_iov[i].iov_len = packets[i].size();
    receive_messages[i].msg_hdr.msg_iov = &receive_iov[i];
    receive_messages[i].msg_hdr.msg_iovlen = 1;
    if (socket_drop_counters_available) {
      receive_messages[i].msg_hdr.msg_control = receive_controls[i].bytes.data();
      receive_messages[i].msg_hdr.msg_controllen = receive_controls[i].bytes.size();
    }
  }
  alignas(64) uint8_t recovered[msg::kMaxFrame];
  alignas(64) uint8_t expanded[msg::kMaxFrame];
  std::vector<fec_xor::Group> fec_groups(cfg.fec_xor8 ? 1024 : 0);
  std::vector<fec_rs::Group> rs_groups(cfg.fec_rs8_2 ? 1024 : 0);
  std::vector<packet_fec_wire::Group> packet_fec_groups(cfg.fec_packet_xor4 ? 1024 : 0);
  stage_metrics::Samples receiver_receive_samples;
  stage_metrics::Samples receiver_process_samples;
  stage_metrics::Samples receiver_ingress_samples;
  uint64_t receive_calls = 0, processed_datagrams = 0;
  uint32_t receiver_socket_drops = 0;
  uint64_t count_reached_ns = 0;

  auto publish_frame = [&](const uint8_t* frame, uint32_t frame_len) {
    msg::Header fixed_header{};
    std::memcpy(&fixed_header, frame, sizeof(fixed_header));
    if (min_observed_seq == 0 || fixed_header.seq_id < min_observed_seq) {
      min_observed_seq = fixed_header.seq_id;
    }
    if (fixed_header.seq_id > max_observed_seq) max_observed_seq = fixed_header.seq_id;
    output.publish(frame, frame_len, util::now_ns());
    ++accepted;
    last_progress = util::now_ns();
  };

  auto release_ready = [&](uint64_t now_ns) {
    resequencer.release_ready(now_ns, reorder_wait_ns, publish_frame);
  };

  auto accept_frame = [&](const uint8_t* frame, uint32_t frame_len) {
    msg::Header fixed_header{};
    if (frame_len < sizeof(fixed_header)) return false;
    std::memcpy(&fixed_header, frame, sizeof(fixed_header));
    if (!udp_wire::valid_frame(frame, frame_len, fixed_header.seq_id)) {
      ++invalid;
      return false;
    }
    if (fixed_header.seq_id > cfg.profile_from_seq && cfg.profile_sample_every != 0 && receiver_ingress_samples.can_record() &&
        fixed_header.seq_id % cfg.profile_sample_every == 0) {
      const uint64_t now_ns = util::now_ns();
      if (now_ns >= fixed_header.send_ts_ns) {
        receiver_ingress_samples.record(now_ns - fixed_header.send_ts_ns, fixed_header.seq_id);
      }
    }
    if (cfg.reorder_wait_auto && last_timing_seq != 0 &&
        fixed_header.seq_id > last_timing_seq &&
        fixed_header.send_ts_ns > last_timing_send_ns) {
      const uint64_t sequence_gap = fixed_header.seq_id - last_timing_seq;
      const uint64_t timestamp_gap = fixed_header.send_ts_ns - last_timing_send_ns;
      const uint64_t period_ns = timestamp_gap / sequence_gap;
      if (period_ns != 0 && period_ns <= kAutoReorderWaitMaxNs) {
        const uint64_t candidate = period_ns > kAutoReorderWaitMaxNs / kAutoReorderPeriods
                                       ? kAutoReorderWaitMaxNs
                                       : period_ns * kAutoReorderPeriods;
        reorder_wait_ns = std::max(kAutoReorderWaitMinNs,
                                   std::min(kAutoReorderWaitMaxNs, candidate));
      }
    }
    if (last_timing_seq == 0 || fixed_header.seq_id > last_timing_seq) {
      last_timing_seq = fixed_header.seq_id;
      last_timing_send_ns = fixed_header.send_ts_ns;
    }
    const auto result = resequencer.insert(fixed_header.seq_id, frame, frame_len, util::now_ns());
    if (result == udp_resequencer::InsertResult::kDuplicateOrLate) {
      ++duplicates;
      return false;
    }
    if (result == udp_resequencer::InsertResult::kTooFarAhead) {
      ++too_old;
      return false;
    }
    release_ready(util::now_ns());
    return true;
  };

  auto accept_wire_frame = [&](const uint8_t* frame, uint32_t frame_len,
                               bool compact, uint64_t expected_seq = 0) {
    if (compact) {
      if (!cfg.compact_wire ||
          !compact_wire::decode(frame, frame_len, expanded, &frame_len)) {
        ++invalid;
        return false;
      }
      frame = expanded;
    } else if (cfg.compact_wire) {
      ++invalid;
      return false;
    }
    if (expected_seq != 0) {
      msg::Header fixed_header{};
      if (frame_len < sizeof(fixed_header)) {
        ++invalid;
        return false;
      }
      std::memcpy(&fixed_header, frame, sizeof(fixed_header));
      if (fixed_header.seq_id != expected_seq) {
        ++invalid;
        return false;
      }
    }
    return accept_frame(frame, frame_len);
  };

  auto select_group = [&](const fec_wire::Header& header) -> fec_xor::Group& {
    fec_xor::Group& group = fec_groups[header.group_id & (fec_groups.size() - 1)];
    if (!group.belongs_to(header.session_id, header.group_id) && group.active()) {
      ++fec_evicted;
      group.reset();
    }
    return group;
  };

  auto try_recover = [&](fec_xor::Group& group) {
    uint32_t recovered_len = 0;
    if (group.recover(recovered, &recovered_len)) {
      if (accept_wire_frame(recovered, recovered_len, group.compact())) ++fec_recovered;
      return;
    }
    if (group.complete()) group.reset();
  };

  auto process_fec_packet = [&](const uint8_t* packet, size_t packet_len) {
    fec_wire::Header header{};
    if (!fec_wire::decode(packet, packet_len, &header)) {
      ++invalid;
      return;
    }
    if (active_session == 0) active_session = header.session_id;
    if (header.session_id != active_session) {
      ++session_mismatch;
      return;
    }
    if (cfg.fec_rs8_2) {
      fec_rs::Group& group = rs_groups[header.group_id & (rs_groups.size() - 1)];
      if (!group.belongs_to(header.session_id, header.group_id) && group.active()) {
        ++fec_evicted;
        group.reset();
      }
      const uint8_t* payload = packet + fec_wire::kHeaderSize;
      if (header.kind == fec_wire::Kind::kData) {
        if (!group.add_data(header, payload)) { ++invalid; return; }
        (void)accept_wire_frame(payload, header.frame_len, header.compact);
      } else {
        if (!group.add_parity(header, payload)) { ++invalid; return; }
        ++fec_parity;
      }
      std::array<std::array<uint8_t,msg::kMaxFrame>,2> recovered_rs{};
      uint32_t recovered_len = 0;
      const uint16_t recovered_count = group.recover(&recovered_rs, &recovered_len);
      for (uint16_t i = 0; i < recovered_count; ++i) {
        if (accept_wire_frame(recovered_rs[i].data(), recovered_len, group.compact())) ++fec_recovered;
      }
      if (group.complete()) group.reset();
      return;
    }
    fec_xor::Group& group = select_group(header);
    const uint8_t* payload = packet + fec_wire::kHeaderSize;
    if (header.kind == fec_wire::Kind::kData) {
      if (!group.add_data(header, payload)) { ++invalid; return; }
      (void)accept_wire_frame(payload, header.frame_len, header.compact);
      try_recover(group);
    } else {
      if (!group.add_parity(header, payload)) { ++invalid; return; }
      ++fec_parity;
      try_recover(group);
    }
  };

  auto process_batch = [&](const uint8_t* packet, size_t packet_len,
                           bool packet_fec) {
        udp_batch_wire::Header batch{};
        if (!udp_batch_wire::decode(packet, packet_len, &batch)) {
          // XOR parity remains one FEC envelope so it can carry the complete
          // group XOR. Only systematic data envelopes are batched.
          if (!packet_fec && (cfg.fec_xor8 || cfg.fec_rs8_2)) {
            process_fec_packet(packet, packet_len);
            return;
          }
          ++invalid;
          return;
        }
        if (active_session == 0) active_session = batch.session_id;
        if (batch.session_id != active_session) {
          ++session_mismatch;
          return;
        }
        size_t offset = udp_batch_wire::kHeaderSize;
        for (uint16_t i = 0; i < batch.frame_count; ++i) {
          size_t consumed = 0;
          if (!packet_fec && (cfg.fec_xor8 || cfg.fec_rs8_2)) {
            fec_wire::Header header{};
            if (!fec_wire::decode_prefix(packet + offset, packet_len - offset,
                                         &header, &consumed)) {
              ++invalid;
              return;
            }
            process_fec_packet(packet + offset, consumed);
          } else {
            udp_wire::DecodedHeader header{};
            if (!udp_wire::decode_prefix(packet + offset, batch.packet_bytes - offset,
                                         &header, &consumed) ||
                header.session_id != active_session ||
                ((header.format == udp_wire::Format::kCompact) != cfg.compact_wire)) {
              ++invalid;
              return;
            }
            const uint8_t* frame = packet + offset + udp_wire::kHeaderSize;
            (void)accept_wire_frame(frame, header.frame_len,
                                    header.format == udp_wire::Format::kCompact,
                                    header.seq_id);
          }
          offset += consumed;
        }
        if (offset != batch.packet_bytes) ++invalid;
  };

  auto process_packet_fec = [&](const uint8_t* packet, size_t packet_len) {
    packet_fec_wire::Header header{};
    if (!packet_fec_wire::decode(packet, packet_len, &header)) { ++invalid; return; }
    if (active_session == 0) active_session = header.session_id;
    if (header.session_id != active_session) { ++session_mismatch; return; }
    packet_fec_wire::Group& group = packet_fec_groups[
        header.group_id & (packet_fec_groups.size() - 1)];
    if (!group.belongs_to(header.session_id, header.group_id) && group.active()) {
      ++fec_evicted; group.reset();
    }
    const uint8_t* payload = packet + packet_fec_wire::kHeaderSize;
    if (header.kind == packet_fec_wire::Kind::kData) {
      if (!group.add_data(header, payload)) { ++invalid; return; }
      process_batch(payload, header.payload_len, true);
    } else {
      if (!group.add_parity(header, payload)) { ++invalid; return; }
      ++fec_parity;
    }
    alignas(64) std::array<uint8_t, packet_fec_wire::kMaxPayload> recovered_packet{};
    uint32_t recovered_len = 0;
    if (group.recover(recovered_packet.data(), &recovered_len)) {
      const uint64_t before = accepted;
      process_batch(recovered_packet.data(), recovered_len, true);
      fec_recovered += accepted - before;
    }
    if (group.complete()) group.reset();
  };

  auto process_packet = [&](const uint8_t* packet, size_t packet_len) {
    if (cfg.fec_packet_xor4) {
      process_packet_fec(packet, packet_len);
      return;
    }
    if (cfg.batch_bytes != 0) {
        process_batch(packet, packet_len, false);
        return;
    }
    if (!cfg.fec_xor8 && !cfg.fec_rs8_2) {
      udp_wire::DecodedHeader header{};
      if (!udp_wire::decode(packet, packet_len, &header)) {
        ++invalid;
        return;
      }
      if ((header.format == udp_wire::Format::kCompact) != cfg.compact_wire) {
        ++invalid;
        return;
      }
      if (active_session == 0) active_session = header.session_id;
      if (header.session_id != active_session) {
        ++session_mismatch;
        return;
      }
      const uint8_t* frame = packet + udp_wire::kHeaderSize;
      (void)accept_wire_frame(frame, header.frame_len,
                              header.format == udp_wire::Format::kCompact,
                              header.seq_id);
      return;
    }

    process_fec_packet(packet, packet_len);
  };

  rusage usage_start{};
  (void)getrusage(RUSAGE_SELF, &usage_start);
  while (cfg.count == 0 || count_reached_ns == 0 ||
         util::now_ns() - count_reached_ns < post_count_drain_ns) {
    for (auto& message : receive_messages) {
      message.msg_hdr.msg_flags = 0;
      if (socket_drop_counters_available) message.msg_hdr.msg_controllen = CMSG_SPACE(sizeof(uint32_t));
    }
    // MSG_TRUNC returns each real datagram size on Linux, so an oversized
    // datagram cannot masquerade as a valid prefix.
    const bool profile_receive = (cfg.profile_from_seq == 0 || max_observed_seq > cfg.profile_from_seq) &&
                                 receiver_receive_samples.can_record() &&
                                 cfg.profile_sample_every != 0 &&
                                 ++receive_calls % cfg.profile_sample_every == 0;
    const uint64_t receive_started_ns = profile_receive ? util::steady_now_ns() : 0;
    int received = -1;
    do {
      received = recvmmsg(socket_fd, receive_messages.data(), cfg.receive_batch,
                          MSG_DONTWAIT | MSG_TRUNC, nullptr);
    } while (received < 0 && errno == EINTR);
    if (received < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        release_ready(util::now_ns());
        if (util::now_ns() - last_progress > idle_ns) break;
        continue;
      }
      std::fprintf(stderr, "recvmmsg failed: %s\n", std::strerror(errno));
      fatal_socket_error = true;
      break;
    }
    if (profile_receive) receiver_receive_samples.record(util::steady_now_ns() - receive_started_ns,
                                                          max_observed_seq);
    for (int i = 0; i < received; ++i) {
      if (socket_drop_counters_available) {
        msghdr& hdr = receive_messages[static_cast<size_t>(i)].msg_hdr;
        for (cmsghdr* cmsg = CMSG_FIRSTHDR(&hdr); cmsg != nullptr;
             cmsg = CMSG_NXTHDR(&hdr, cmsg)) {
          if (cmsg->cmsg_level == SOL_SOCKET && cmsg->cmsg_type == SO_RXQ_OVFL &&
              cmsg->cmsg_len >= CMSG_LEN(sizeof(uint32_t))) {
            std::memcpy(&receiver_socket_drops, CMSG_DATA(cmsg), sizeof(receiver_socket_drops));
          }
        }
      }
      const bool profile_process = (cfg.profile_from_seq == 0 || max_observed_seq > cfg.profile_from_seq) &&
                                   receiver_process_samples.can_record() &&
                                   cfg.profile_sample_every != 0 &&
                                   ++processed_datagrams % cfg.profile_sample_every == 0;
      const uint64_t process_started_ns = profile_process ? util::steady_now_ns() : 0;
      process_packet(packets[static_cast<size_t>(i)].data(),
                     receive_messages[static_cast<size_t>(i)].msg_len);
      if (profile_process) receiver_process_samples.record(util::steady_now_ns() - process_started_ns,
                                                            max_observed_seq);
      if (cfg.count != 0 && count_reached_ns == 0 && accepted >= cfg.count) {
        count_reached_ns = util::now_ns();
      }
    }
  }

  close(socket_fd);
  rusage usage_end{};
  (void)getrusage(RUSAGE_SELF, &usage_end);
  const uint64_t expected = min_observed_seq == 0
                                ? 0
                                : max_observed_seq - min_observed_seq + 1;
  const uint64_t apparent_missing = expected > accepted ? expected - accepted : 0;
  const std::string reorder_wait_label = cfg.reorder_wait_auto
      ? "auto" : std::to_string(cfg.reorder_wait_us);
  const char* socket_drop_counter_label = !cfg.socket_drop_counters ? "disabled" :
      (socket_drop_counters_available ? "enabled" : "unavailable");
  std::fprintf(stderr,
               "udp_receiver: session=%llu fec=%s wire_format=%s recv_batch=%u accepted=%llu first_seq=%llu last_seq=%llu "
               "apparent_missing=%llu invalid=%llu duplicates=%llu too_old=%llu session_mismatch=%llu "
               "fec_parity=%llu fec_recovered=%llu fec_evicted=%llu receiver_socket_drop_counter=%s receiver_socket_drops=%u reorder_drops=%llu reorder_duplicates=%llu reorder_too_far=%llu reorder_wait_us=%s reorder_wait_effective_us=%llu cpu_user_ns=%llu cpu_system_ns=%llu voluntary_cs=%ld involuntary_cs=%ld minor_faults=%ld major_faults=%ld\n",
               static_cast<unsigned long long>(active_session),
               cfg.fec_packet_xor4 ? "packet_xor4" : (cfg.fec_xor8 ? (cfg.fec_group_size == 4 ? "xor4" : "xor8") : (cfg.fec_rs8_2 ? (cfg.fec_group_size == 4 ? "rs4_2" : "rs8_2") : "none")),
               cfg.compact_wire ? "compact" : "full",
               cfg.receive_batch,
               static_cast<unsigned long long>(accepted),
               static_cast<unsigned long long>(min_observed_seq),
               static_cast<unsigned long long>(max_observed_seq),
               static_cast<unsigned long long>(apparent_missing),
               static_cast<unsigned long long>(invalid),
               static_cast<unsigned long long>(duplicates),
               static_cast<unsigned long long>(too_old),
               static_cast<unsigned long long>(session_mismatch),
               static_cast<unsigned long long>(fec_parity),
               static_cast<unsigned long long>(fec_recovered),
               static_cast<unsigned long long>(fec_evicted),
               socket_drop_counter_label,
               receiver_socket_drops,
               static_cast<unsigned long long>(resequencer.counters().declared_drops),
               static_cast<unsigned long long>(resequencer.counters().duplicates_or_late),
               static_cast<unsigned long long>(resequencer.counters().too_far_ahead),
               reorder_wait_label.c_str(),
               static_cast<unsigned long long>(reorder_wait_ns / 1000ull),
               static_cast<unsigned long long>(timeval_ns(usage_end.ru_utime) - timeval_ns(usage_start.ru_utime)),
               static_cast<unsigned long long>(timeval_ns(usage_end.ru_stime) - timeval_ns(usage_start.ru_stime)),
               usage_end.ru_nvcsw - usage_start.ru_nvcsw,
               usage_end.ru_nivcsw - usage_start.ru_nivcsw,
               usage_end.ru_minflt - usage_start.ru_minflt,
               usage_end.ru_majflt - usage_start.ru_majflt);
  receiver_receive_samples.print(stderr, "udp_receiver_profile_recvmmsg");
  receiver_process_samples.print(stderr, "udp_receiver_profile_process");
  receiver_ingress_samples.print(stderr, "udp_receiver_profile_ingress");
  segment.unlink();
  return fatal_socket_error ? 1 : 0;
}
