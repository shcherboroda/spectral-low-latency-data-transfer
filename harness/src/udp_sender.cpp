// UDP transport sender: reads the fixed producer SHM ring and sends one exact
// frame per nonblocking UDP datagram.
#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <sched.h>

#include <cerrno>
#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include "shm_ring.h"
#include "stage_metrics.h"
#include "compact_wire.h"
#include "udp_deduper.h"
#include "fec_wire.h"
#include "fec_xor.h"
#include "packet_fec_wire.h"
#include "fec_rs.h"
#include "udp_wire.h"
#include "udp_batch_wire.h"
#include "util.h"

namespace {

inline constexpr uint64_t kMaxMilliseconds =
    std::numeric_limits<uint64_t>::max() / 1000000ull;
static_assert(sizeof(msg::Trade) == sizeof(msg::Bbo),
              "FEC short-frame stream assumes Trade and Bbo have the same size");
static_assert(sizeof(msg::Trade) != sizeof(msg::OrderBook),
              "FEC streams require distinct short and order-book frame sizes");

uint64_t timeval_ns(const timeval& value) {
  return static_cast<uint64_t>(value.tv_sec) * 1000000000ull +
         static_cast<uint64_t>(value.tv_usec) * 1000ull;
}

struct Config {
  std::string in_shm = "/fanout_ring";
  uint32_t in_slots = 1024;
  std::string host = "127.0.0.1";
  uint16_t port = 9000;
  std::vector<std::string> targets;
  bool legacy_destination_set = false;
  bool fanout_sendmmsg = false;
  bool connected_udp = false;
  uint64_t count = 0;
  bool from_edge = false;
  uint64_t idle_ms = 2000;
  uint64_t wait_ms = 0;  // 0 is indefinitely
  uint64_t session_id = 0;
  int socket_buffer = 4 * 1024 * 1024;
  bool fec_xor8 = false;
  bool fec_rs8_2 = false;
  bool fec_packet_xor4 = false;
  uint16_t fec_group_size = 8;
  uint32_t batch_bytes = 0;
  uint32_t profile_sample_every = 0;
  uint64_t profile_from_seq = 0;
  bool compact_wire = false;
  uint32_t sender_workers = 1;
  bool sender_workers_auto = false;
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

bool parse_target(const std::string& value, sockaddr_in* out) {
  const size_t separator = value.rfind(':');
  if (separator == std::string::npos || separator == 0 ||
      separator + 1 == value.size()) {
    return false;
  }
  const std::string host = value.substr(0, separator);
  const uint16_t port = parse_bounded<uint16_t>(value.substr(separator + 1), "--target");
  if (port == 0 || inet_pton(AF_INET, host.c_str(), &out->sin_addr) != 1) {
    return false;
  }
  out->sin_family = AF_INET;
  out->sin_port = htons(port);
  return true;
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
    if (arg == "--in-shm") c.in_shm = next();
    else if (arg == "--in-slots") c.in_slots = parse_bounded<uint32_t>(next(), "--in-slots");
    else if (arg == "--host") { c.host = next(); c.legacy_destination_set = true; }
    else if (arg == "--port") { c.port = parse_bounded<uint16_t>(next(), "--port"); c.legacy_destination_set = true; }
    else if (arg == "--target") c.targets.push_back(next());
    else if (arg == "--fanout-mode") {
      const std::string mode = next();
      if (mode == "loop") c.fanout_sendmmsg = false;
      else if (mode == "sendmmsg") c.fanout_sendmmsg = true;
      else {
        std::fprintf(stderr, "--fanout-mode must be loop or sendmmsg\n");
        std::exit(2);
      }
    }
    else if (arg == "--udp-mode") {
      const std::string mode = next();
      if (mode == "sendto") c.connected_udp = false;
      else if (mode == "connected") c.connected_udp = true;
      else {
        std::fprintf(stderr, "--udp-mode must be sendto or connected\n");
        std::exit(2);
      }
    }
    else if (arg == "--count") c.count = parse_u64(next(), "--count");
    else if (arg == "--from-edge") c.from_edge = true;
    else if (arg == "--idle-ms") c.idle_ms = parse_u64(next(), "--idle-ms");
    else if (arg == "--wait-ms") c.wait_ms = parse_u64(next(), "--wait-ms");
    else if (arg == "--session-id") c.session_id = parse_u64(next(), "--session-id");
    else if (arg == "--socket-buffer") c.socket_buffer = parse_bounded<int>(next(), "--socket-buffer");
    else if (arg == "--batch-bytes") c.batch_bytes = parse_bounded<uint32_t>(next(), "--batch-bytes");
    else if (arg == "--profile-sample-every") c.profile_sample_every = parse_bounded<uint32_t>(next(), "--profile-sample-every");
    else if (arg == "--profile-from-seq") c.profile_from_seq = parse_u64(next(), "--profile-from-seq");
    else if (arg == "--sender-workers") {
      const std::string value = next();
      if (value == "auto") c.sender_workers_auto = true;
      else c.sender_workers = parse_bounded<uint32_t>(value, "--sender-workers");
    }
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
  if (c.port == 0 || !udp_deduper::is_power_of_two(c.in_slots) || c.socket_buffer < 0 ||
      c.targets.size() > 3 || (!c.targets.empty() && c.legacy_destination_set)) {
    std::fprintf(stderr, "invalid --port, --in-slots (power of two), or --socket-buffer\n");
    std::exit(2);
  }
  if (c.idle_ms > kMaxMilliseconds || c.wait_ms > kMaxMilliseconds) {
    std::fprintf(stderr, "millisecond timeout is out of range\n");
    std::exit(2);
  }
  if (c.batch_bytes != 0 && (c.batch_bytes < udp_batch_wire::kHeaderSize + udp_wire::kMaxDatagram ||
                             c.batch_bytes > udp_batch_wire::kMaxDatagram)) {
    std::fprintf(stderr, "--batch-bytes must be %zu..%zu\n",
                 udp_batch_wire::kHeaderSize + udp_wire::kMaxDatagram,
                 udp_batch_wire::kMaxDatagram);
    std::exit(2);
  }
  if (c.fec_packet_xor4 && (!c.compact_wire || c.batch_bytes == 0)) {
    std::fprintf(stderr, "--fec packet_xor4 requires --wire-format compact and --batch-bytes\n");
    std::exit(2);
  }
  if (c.targets.empty()) {
    in_addr parsed_address{};
    if (inet_pton(AF_INET, c.host.c_str(), &parsed_address) != 1) {
      std::fprintf(stderr, "--host must be an IPv4 address: %s\n", c.host.c_str());
      std::exit(2);
    }
  } else {
    std::array<sockaddr_in, 3> seen{};
    size_t seen_count = 0;
    for (const std::string& target : c.targets) {
      sockaddr_in parsed{};
      if (!parse_target(target, &parsed)) {
        std::fprintf(stderr, "--target must be IPv4:PORT: %s\n", target.c_str());
        std::exit(2);
      }
      for (size_t i = 0; i < seen_count; ++i) {
        if (seen[i].sin_addr.s_addr == parsed.sin_addr.s_addr &&
            seen[i].sin_port == parsed.sin_port) {
          std::fprintf(stderr, "duplicate --target: %s\n", target.c_str());
          std::exit(2);
        }
      }
      seen[seen_count++] = parsed;
    }
  }
  if (c.session_id == 0) c.session_id = util::now_ns() | 1ull;
  return c;
}

enum class SendResult { kSent, kDropped, kFatal };

inline constexpr size_t kFecStreams = 3;

constexpr size_t fec_stream_index(uint32_t frame_len) {
  if (frame_len == sizeof(msg::Trade) || frame_len == compact_wire::kTradeSize) return 0;
  if (frame_len == sizeof(msg::OrderBook) || frame_len == compact_wire::kBboSize) return 1;
  if (frame_len == compact_wire::kBookSize) return 2;
  return 2;
}

static_assert(fec_stream_index(sizeof(msg::Trade)) < kFecStreams);
static_assert(fec_stream_index(sizeof(msg::Bbo)) < kFecStreams);
static_assert(fec_stream_index(sizeof(msg::OrderBook)) < kFecStreams);

int wait_for_input(const Config& cfg) {
  const uint64_t start = util::now_ns();
  for (;;) {
    const int fd = shm_open(cfg.in_shm.c_str(), O_RDWR, 0600);
    if (fd >= 0) {
      struct stat status {};
      if (fstat(fd, &status) != 0) {
        const int saved_errno = errno;
        close(fd);
        errno = saved_errno;
        std::fprintf(stderr, "fstat(%s) failed: %s\n", cfg.in_shm.c_str(),
                     std::strerror(errno));
        return -1;
      }
      const size_t expected_size = shm::region_size(cfg.in_slots);
      if (status.st_size == static_cast<off_t>(expected_size)) {
        return fd;  // caller owns this exact ready-sized object
      }
      close(fd);
      if (status.st_size != 0) {
        std::fprintf(stderr,
                     "input SHM size mismatch: expected=%zu actual=%lld\n",
                     expected_size, static_cast<long long>(status.st_size));
        return -1;
      }
    } else if (errno != ENOENT) {
      std::fprintf(stderr, "shm_open(%s) failed: %s\n", cfg.in_shm.c_str(), std::strerror(errno));
      return -1;
    }
    if (cfg.wait_ms != 0 && util::now_ns() - start >= cfg.wait_ms * 1000000ull) {
      std::fprintf(stderr, "timed out waiting for input SHM %s\n", cfg.in_shm.c_str());
      return -1;
    }
    usleep(1000);  // setup only; the data path is busy-polled below
  }
}

bool wait_for_ring_initialization(void* base, const Config& cfg) {
  auto* header = static_cast<shm::Header*>(base);
  const uint64_t start = util::now_ns();
  for (;;) {
    // The fixed producer initializes every slot before publishing its first
    // frame. A positive write edge is therefore the only readiness signal the
    // existing SHM ABI exposes that cannot precede slot initialization.
    if (header->write_index.load(std::memory_order_acquire) != 0) {
      if (header->magic != shm::kMagic || header->slot_count != cfg.in_slots ||
          header->slot_size != sizeof(shm::Slot)) {
        std::fprintf(stderr, "input SHM ring metadata mismatch\n");
        return false;
      }
      return true;
    }
    if (cfg.wait_ms != 0 && util::now_ns() - start >= cfg.wait_ms * 1000000ull) {
      std::fprintf(stderr, "timed out waiting for initialized input SHM %s\n",
                   cfg.in_shm.c_str());
      return false;
    }
    usleep(1000);
  }
}

int open_socket(const Config& cfg) {
  const int fd = socket(AF_INET, SOCK_DGRAM, 0);
  if (fd < 0) return -1;
  if (fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | O_NONBLOCK) != 0) {
    close(fd);
    return -1;
  }
  if (cfg.socket_buffer > 0 &&
      setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &cfg.socket_buffer, sizeof(cfg.socket_buffer)) != 0) {
    std::fprintf(stderr, "warning: SO_SNDBUF: %s\n", std::strerror(errno));
  }
  return fd;
}

std::vector<int> physical_cpus_in_affinity() {
  cpu_set_t allowed;
  CPU_ZERO(&allowed);
  if (sched_getaffinity(0, sizeof(allowed), &allowed) != 0) return {};
  std::vector<int> selected;
  std::vector<std::string> seen;
  for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
    if (!CPU_ISSET(cpu, &allowed)) continue;
    std::ifstream package_file("/sys/devices/system/cpu/cpu" + std::to_string(cpu) +
                               "/topology/physical_package_id");
    std::ifstream core_file("/sys/devices/system/cpu/cpu" + std::to_string(cpu) +
                            "/topology/core_id");
    std::string package, core;
    if (!(package_file >> package) || !(core_file >> core)) {
      selected.push_back(cpu);
      continue;
    }
    const std::string key = package + ":" + core;
    bool duplicate = false;
    for (const std::string& value : seen) duplicate = duplicate || value == key;
    if (!duplicate) { seen.push_back(key); selected.push_back(cpu); }
  }
  return selected;
}

int launch_workers_if_requested(int argc, char** argv, const Config& cfg) {
  if (cfg.targets.size() < 2 || (!cfg.sender_workers_auto && cfg.sender_workers <= 1)) return -1;
  const std::vector<int> cpus = physical_cpus_in_affinity();
  if (cpus.empty()) { std::fprintf(stderr, "cannot determine sender CPU affinity\n"); return 2; }
  const size_t requested = cfg.sender_workers_auto
      ? std::min(cfg.targets.size(), std::max<size_t>(1, cpus.size() - 1))
      : std::min(cfg.targets.size(), static_cast<size_t>(cfg.sender_workers));
  std::vector<pid_t> children;
  std::vector<int> report_fds;
  for (size_t worker = 0; worker < requested; ++worker) {
    int report_pipe[2]{};
    if (pipe(report_pipe) != 0) {
      std::fprintf(stderr, "create sender worker report pipe failed: %s\n", std::strerror(errno));
      return 1;
    }
    const pid_t child = fork();
    if (child < 0) {
      close(report_pipe[0]); close(report_pipe[1]);
      std::fprintf(stderr, "fork sender worker failed: %s\n", std::strerror(errno));
      return 1;
    }
    if (child == 0) {
      close(report_pipe[0]);
      if (dup2(report_pipe[1], STDERR_FILENO) < 0) _exit(127);
      close(report_pipe[1]);
      // Keep the first physical core available for the supplied producer and
      // kernel work; workers use distinct later cores when auto-selected.
      const size_t cpu_index = cfg.sender_workers_auto && cpus.size() > requested
          ? worker + 1 : worker % cpus.size();
      cpu_set_t one_cpu; CPU_ZERO(&one_cpu); CPU_SET(cpus[cpu_index], &one_cpu);
      if (sched_setaffinity(0, sizeof(one_cpu), &one_cpu) != 0) _exit(127);
      std::vector<std::string> args;
      for (int i = 0; i < argc; ++i) {
        const std::string arg = argv[i];
        if ((arg == "--target" || arg == "--sender-workers" || arg == "--fanout-mode") && i + 1 < argc) { ++i; continue; }
        args.push_back(arg);
      }
      args.push_back("--sender-workers"); args.push_back("1");
      args.push_back("--fanout-mode"); args.push_back("loop");
      for (size_t target = worker; target < cfg.targets.size(); target += requested) {
        args.push_back("--target"); args.push_back(cfg.targets[target]);
      }
      std::vector<char*> raw;
      for (std::string& arg : args) raw.push_back(arg.data());
      raw.push_back(nullptr);
      execv("/proc/self/exe", raw.data());
      _exit(127);
    }
    close(report_pipe[1]);
    children.push_back(child);
    report_fds.push_back(report_pipe[0]);
  }
  int failed = 0;
  for (size_t worker = 0; worker < children.size(); ++worker) {
    int status = 0;
    if (waitpid(children[worker], &status, 0) < 0 || !WIFEXITED(status) || WEXITSTATUS(status) != 0) failed = 1;
    std::fprintf(stderr, "udp_sender: worker=%zu report_begin\n", worker + 1);
    std::array<char, 4096> buffer{};
    for (;;) {
      const ssize_t read_bytes = read(report_fds[worker], buffer.data(), buffer.size());
      if (read_bytes <= 0) break;
      const ssize_t written = write(STDERR_FILENO, buffer.data(), static_cast<size_t>(read_bytes));
      if (written != read_bytes) failed = 1;
    }
    close(report_fds[worker]);
    std::fprintf(stderr, "udp_sender: worker=%zu report_end\n", worker + 1);
  }
  std::fprintf(stderr, "udp_sender: worker_mode=%s workers=%zu physical_cpus=%zu status=%s\n",
               cfg.sender_workers_auto ? "auto" : "fixed", requested, cpus.size(), failed ? "fail" : "pass");
  return failed;
}

}  // namespace

int main(int argc, char** argv) {
  const Config cfg = parse_args(argc, argv);
  const int worker_result = launch_workers_if_requested(argc, argv, cfg);
  if (worker_result >= 0) return worker_result;
  const int input_fd = wait_for_input(cfg);
  if (input_fd < 0) return 1;
  const size_t input_size = shm::region_size(cfg.in_slots);
  void* input_base = mmap(nullptr, input_size, PROT_READ | PROT_WRITE, MAP_SHARED,
                          input_fd, 0);
  if (input_base == MAP_FAILED) {
    std::fprintf(stderr, "mmap input SHM failed: %s\n", std::strerror(errno));
    close(input_fd);
    return 1;
  }
  if (!wait_for_ring_initialization(input_base, cfg)) {
    munmap(input_base, input_size);
    close(input_fd);
    return 1;
  }
  shm::Ring input;
  input.attach(input_base, cfg.in_slots, /*init=*/false);
  if (input.slot_count() != cfg.in_slots) {
    std::fprintf(stderr, "input SHM slot count does not match --in-slots\n");
    munmap(input_base, input_size);
    close(input_fd);
    return 2;
  }

  std::array<sockaddr_in, 3> destinations{};
  size_t destination_count = 0;
  if (cfg.targets.empty()) {
    sockaddr_in& destination = destinations[destination_count++];
    destination.sin_family = AF_INET;
    destination.sin_port = htons(cfg.port);
    (void)inet_pton(AF_INET, cfg.host.c_str(), &destination.sin_addr);
  } else {
    for (const std::string& target : cfg.targets) {
      (void)parse_target(target, &destinations[destination_count++]);
    }
  }
  std::array<int, 3> socket_fds{};
  socket_fds.fill(-1);
  const size_t socket_count = cfg.connected_udp ? destination_count : 1;
  for (size_t i = 0; i < socket_count; ++i) {
    socket_fds[i] = open_socket(cfg);
    if (socket_fds[i] < 0) {
      std::fprintf(stderr, "cannot open UDP socket: %s\n", std::strerror(errno));
      for (int fd : socket_fds) if (fd >= 0) close(fd);
      munmap(input_base, input_size);
      close(input_fd);
      return 1;
    }
    if (cfg.connected_udp &&
        connect(socket_fds[i], reinterpret_cast<const sockaddr*>(&destinations[i]),
                sizeof(destinations[i])) != 0) {
      std::fprintf(stderr, "UDP connect failed for target %zu: %s\n", i,
                   std::strerror(errno));
      for (int fd : socket_fds) if (fd >= 0) close(fd);
      munmap(input_base, input_size);
      close(input_fd);
      return 1;
    }
  }
  if (cfg.connected_udp && cfg.fanout_sendmmsg && destination_count > 1) {
    std::fprintf(stderr, "--udp-mode connected cannot use --fanout-mode sendmmsg\n");
    for (int fd : socket_fds) if (fd >= 0) close(fd);
    munmap(input_base, input_size);
    close(input_fd);
    return 2;
  }
  const bool effective_sendmmsg = cfg.fanout_sendmmsg && destination_count > 1;

  uint64_t read_index = cfg.from_edge ? input.live_edge() : 0;
  uint64_t frames_read = 0, sent = 0, data_datagrams = 0, input_lapped = 0;
  uint64_t invalid_frames = 0, socket_drops = 0;
  uint64_t parity_sent = 0;
  uint64_t data_payload_bytes = 0, data_payload_bytes_max = 0;
  std::array<uint64_t, 8> data_frame_hist{};
  std::array<uint64_t, 3> destination_drops{};
  uint64_t first_sent_seq = 0, last_sent_seq = 0;
  bool fatal_socket_error = false;
  uint64_t last_progress = util::now_ns();
  const uint64_t idle_ns = cfg.idle_ms * 1000000ull;
  alignas(64) uint8_t frame[shm::kFrameCap];
  alignas(64) uint8_t packet[udp_batch_wire::kMaxDatagram];
  alignas(64) uint8_t pending_frame[shm::kFrameCap];
  uint32_t pending_frame_len = 0;
  bool has_pending_frame = false;
  alignas(64) uint8_t compact_frame[msg::kMaxFrame];
  std::array<fec_xor::Encoder, kFecStreams> fec_encoders;
  packet_fec_wire::Encoder packet_fec_encoder;
  std::array<fec_rs::Encoder, kFecStreams> rs_encoders;
  for (auto& encoder : fec_encoders) encoder.set_group_size(cfg.fec_group_size);
  for (auto& encoder : rs_encoders) encoder.set_group_size(cfg.fec_group_size);
  uint64_t next_fec_group_id = 1;
  uint32_t next_packet_fec_group_id = 1;
  stage_metrics::Samples sender_operation_samples;
  stage_metrics::Samples sender_send_samples;
  stage_metrics::Samples sender_prepare_samples;
  stage_metrics::Samples sender_ingress_samples;
  bool profile_operation_active = false;
  bool profile_send_active = false;
  uint64_t profile_operation_send_elapsed_ns = 0;
  uint64_t profile_operation_seq = 0;

  auto send_datagram = [&](const uint8_t* bytes, size_t bytes_len) {
    const uint64_t send_started_ns = profile_operation_active ? util::steady_now_ns() : 0;
    auto finish = [&](SendResult send_result) {
      if (profile_operation_active) {
        const uint64_t elapsed_ns = util::steady_now_ns() - send_started_ns;
        profile_operation_send_elapsed_ns += elapsed_ns;
        if (profile_send_active) sender_send_samples.record(elapsed_ns, profile_operation_seq);
      }
      return send_result;
    };
    if (effective_sendmmsg) {
      std::array<iovec, 3> iov{};
      std::array<mmsghdr, 3> messages{};
      for (size_t i = 0; i < destination_count; ++i) {
        iov[i].iov_base = const_cast<uint8_t*>(bytes);
        iov[i].iov_len = bytes_len;
        messages[i].msg_hdr.msg_name = &destinations[i];
        messages[i].msg_hdr.msg_namelen = sizeof(destinations[i]);
        messages[i].msg_hdr.msg_iov = &iov[i];
        messages[i].msg_hdr.msg_iovlen = 1;
      }
      int result = -1;
      do {
        result = sendmmsg(socket_fds[0], messages.data(), static_cast<unsigned int>(destination_count),
                          MSG_DONTWAIT);
      } while (result < 0 && errno == EINTR);
      if (result < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == ENOBUFS) {
          socket_drops += destination_count;
          for (size_t i = 0; i < destination_count; ++i) ++destination_drops[i];
          return finish(SendResult::kDropped);
        }
        std::fprintf(stderr, "sendmmsg failed: %s\n", std::strerror(errno));
        return finish(SendResult::kFatal);
      }
      for (int i = 0; i < result; ++i) {
        if (messages[static_cast<size_t>(i)].msg_len != bytes_len) {
          std::fprintf(stderr, "sendmmsg returned a partial UDP datagram\n");
          return finish(SendResult::kFatal);
        }
      }
      if (static_cast<size_t>(result) == destination_count) return finish(SendResult::kSent);
      for (size_t i = static_cast<size_t>(result); i < destination_count; ++i) {
        ++socket_drops;
        ++destination_drops[i];
      }
      return finish(SendResult::kDropped);
    }
    if (cfg.connected_udp) {
      bool dropped = false;
      for (size_t i = 0; i < destination_count; ++i) {
        ssize_t result = -1;
        do {
          result = send(socket_fds[i], bytes, bytes_len, MSG_DONTWAIT);
        } while (result < 0 && errno == EINTR);
        if (result < 0) {
          if (errno == EAGAIN || errno == EWOULDBLOCK || errno == ENOBUFS) {
            ++socket_drops;
            ++destination_drops[i];
            dropped = true;
            continue;
          }
          std::fprintf(stderr, "send target %zu failed: %s\n", i, std::strerror(errno));
          return finish(SendResult::kFatal);
        }
        if (result != static_cast<ssize_t>(bytes_len)) {
          std::fprintf(stderr, "send returned a partial UDP datagram\n");
          return finish(SendResult::kFatal);
        }
      }
      return finish(dropped ? SendResult::kDropped : SendResult::kSent);
    }
    bool dropped = false;
    for (size_t i = 0; i < destination_count; ++i) {
      ssize_t result = -1;
      do {
        result = sendto(socket_fds[0], bytes, bytes_len, 0,
                        reinterpret_cast<const sockaddr*>(&destinations[i]),
                        sizeof(destinations[i]));
      } while (result < 0 && errno == EINTR);
      if (result < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == ENOBUFS) {
          ++socket_drops;
          ++destination_drops[i];
          dropped = true;
          continue;
        }
        std::fprintf(stderr, "sendto target %zu failed: %s\n", i, std::strerror(errno));
        return finish(SendResult::kFatal);
      }
      if (result != static_cast<ssize_t>(bytes_len)) {
        std::fprintf(stderr, "sendto returned a partial UDP datagram\n");
        return finish(SendResult::kFatal);
      }
    }
    return finish(dropped ? SendResult::kDropped : SendResult::kSent);
  };

  // FEC parity must remain an independent UDP datagram: placing it inside the
  // data datagram it protects would make a single loss erase both. For one
  // connected destination, sendmmsg submits several independent datagrams in
  // one kernel transition when a packed data datagram completes FEC groups.
  auto send_connected_datagrams = [&](const std::array<const uint8_t*, 9>& bytes,
                                      const std::array<size_t, 9>& lengths,
                                      size_t datagram_count) -> int {
    if (!cfg.connected_udp || destination_count != 1 || datagram_count < 2) return -3;
    const uint64_t send_started_ns = profile_operation_active ? util::steady_now_ns() : 0;
    auto finish = [&](int value) {
      if (profile_operation_active) {
        const uint64_t elapsed_ns = util::steady_now_ns() - send_started_ns;
        profile_operation_send_elapsed_ns += elapsed_ns;
        if (profile_send_active) sender_send_samples.record(elapsed_ns, profile_operation_seq);
      }
      return value;
    };
    std::array<iovec, 9> iov{};
    std::array<mmsghdr, 9> messages{};
    for (size_t i = 0; i < datagram_count; ++i) {
      iov[i].iov_base = const_cast<uint8_t*>(bytes[i]);
      iov[i].iov_len = lengths[i];
      messages[i].msg_hdr.msg_iov = &iov[i];
      messages[i].msg_hdr.msg_iovlen = 1;
    }
    int result = -1;
    do {
      result = sendmmsg(socket_fds[0], messages.data(),
                        static_cast<unsigned int>(datagram_count), MSG_DONTWAIT);
    } while (result < 0 && errno == EINTR);
    if (result < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK || errno == ENOBUFS) {
        socket_drops += datagram_count;
        destination_drops[0] += datagram_count;
        return finish(0);
      }
      std::fprintf(stderr, "connected sendmmsg failed: %s\n", std::strerror(errno));
      return finish(-1);
    }
    for (int i = 0; i < result; ++i) {
      if (messages[static_cast<size_t>(i)].msg_len != lengths[static_cast<size_t>(i)]) {
        std::fprintf(stderr, "connected sendmmsg returned a partial UDP datagram\n");
        return finish(-1);
      }
    }
    if (static_cast<size_t>(result) < datagram_count) {
      const size_t dropped = datagram_count - static_cast<size_t>(result);
      socket_drops += dropped;
      destination_drops[0] += dropped;
    }
    return finish(result);
  };

  auto send_parity = [&](const fec_xor::Encoder::ParityInfo& parity) {
    fec_wire::encode(packet, fec_wire::Kind::kParity, cfg.session_id,
                     parity.group_id, parity.shard_count, parity.shard_count,
                     parity.frame_len, cfg.compact_wire);
    std::memcpy(packet + fec_wire::kHeaderSize, parity.bytes, parity.frame_len);
    const SendResult result = send_datagram(packet, fec_wire::kHeaderSize + parity.frame_len);
    if (result == SendResult::kFatal) fatal_socket_error = true;
    if (result == SendResult::kSent) ++parity_sent;
  };

  auto send_rs_parity = [&](const fec_rs::Encoder::ParityInfo& parity) {
    for (uint16_t p = 0; p < 2; ++p) {
      fec_wire::encode(packet, fec_wire::Kind::kParity, cfg.session_id,
                       parity.group_id, parity.shard_count + p,
                       parity.shard_count, parity.frame_len, cfg.compact_wire);
      std::memcpy(packet + fec_wire::kHeaderSize, parity.bytes[p], parity.frame_len);
      const SendResult result = send_datagram(packet, fec_wire::kHeaderSize + parity.frame_len);
      if (result == SendResult::kSent) ++parity_sent;
      if (result == SendResult::kFatal) fatal_socket_error = true;
    }
  };

  rusage usage_start{};
  (void)getrusage(RUSAGE_SELF, &usage_start);
  while (cfg.count == 0 || frames_read < cfg.count) {
    uint32_t frame_len = 0;
    uint64_t resume_at = 0;
    shm::Ring::FrameStatus status = shm::Ring::FrameStatus::kEmpty;
    if (has_pending_frame) {
      std::memcpy(frame, pending_frame, pending_frame_len);
      frame_len = pending_frame_len;
      has_pending_frame = false;
      status = shm::Ring::FrameStatus::kOk;
    } else {
      status = input.read(read_index, frame, &frame_len, &resume_at);
    }
    if (status == shm::Ring::FrameStatus::kEmpty) {
      if (util::now_ns() - last_progress > idle_ns) break;
      continue;
    }
    if (status == shm::Ring::FrameStatus::kLapped) {
      ++input_lapped;
      read_index = resume_at;
      continue;
    }

    ++read_index;
    ++frames_read;
    last_progress = util::now_ns();
    msg::Header fixed_header{};
    std::memcpy(&fixed_header, frame, sizeof(fixed_header));
    if (!udp_wire::valid_frame(frame, frame_len, fixed_header.seq_id)) {
      ++invalid_frames;
      continue;
    }
    const bool profile_this = fixed_header.seq_id > cfg.profile_from_seq &&
                              cfg.profile_sample_every != 0 &&
                              fixed_header.seq_id % cfg.profile_sample_every == 0;
    if (profile_this && sender_ingress_samples.can_record()) {
      const uint64_t now_ns = util::now_ns();
      if (now_ns >= fixed_header.send_ts_ns) {
        sender_ingress_samples.record(now_ns - fixed_header.send_ts_ns, fixed_header.seq_id);
      }
    }
    const bool profile_operation = profile_this &&
        sender_operation_samples.can_record() &&
        sender_send_samples.can_record() &&
        sender_prepare_samples.can_record();
    const uint64_t operation_started_ns =
        profile_operation ? util::steady_now_ns() : 0;
    profile_operation_active = profile_operation;
    profile_send_active = profile_operation;
    profile_operation_send_elapsed_ns = 0;
    profile_operation_seq = fixed_header.seq_id;
    SendResult result = SendResult::kFatal;
    uint64_t batch_last_seq = fixed_header.seq_id;
    uint64_t batch_frames = 1;
    size_t data_packet_bytes = 0;
    if (cfg.batch_bytes != 0) {
        const size_t packet_prefix = cfg.fec_packet_xor4 ? packet_fec_wire::kHeaderSize : 0;
        const size_t batch_limit = packet_prefix + (cfg.fec_packet_xor4
            ? packet_fec_wire::kMaxPayload : cfg.batch_bytes);
        size_t used = packet_prefix + udp_batch_wire::kHeaderSize;
        uint16_t frame_count = 0;
        struct PendingParity {
          uint64_t group_id;
          uint16_t shard_index;
          uint16_t shard_count;
          uint32_t frame_len;
          std::array<uint8_t, msg::kMaxFrame> bytes;
        };
        std::array<PendingParity, 8> pending_parities{};
        size_t pending_parity_count = 0;
        auto append = [&](const uint8_t* wire, uint32_t wire_len,
                          const msg::Header& source_header) {
          if (cfg.fec_packet_xor4) {
            udp_wire::encode(packet + used, cfg.session_id, source_header.seq_id,
                             wire_len, udp_wire::Format::kCompact);
          } else if (cfg.fec_xor8) {
            const size_t stream = fec_stream_index(wire_len);
            fec_xor::Encoder::DataInfo data{};
            fec_xor::Encoder::ParityInfo completed{};
            const bool group_complete = fec_encoders[stream].add(
                wire, wire_len, &next_fec_group_id, &data, &completed);
            fec_wire::encode(packet + used, fec_wire::Kind::kData, cfg.session_id,
                             data.group_id, data.shard_index, cfg.fec_group_size,
                             wire_len, cfg.compact_wire);
            if (group_complete) {
              if (pending_parity_count == pending_parities.size()) {
                std::fprintf(stderr, "too many completed FEC groups in one batch\n");
                fatal_socket_error = true;
                return;
              }
              PendingParity& saved = pending_parities[pending_parity_count++];
              saved.group_id = completed.group_id;
              saved.shard_index = completed.shard_count;
              saved.shard_count = completed.shard_count;
              saved.frame_len = completed.frame_len;
              std::memcpy(saved.bytes.data(), completed.bytes, completed.frame_len);
            }
          } else if (cfg.fec_rs8_2) {
            const size_t stream = fec_stream_index(wire_len);
            fec_rs::Encoder::DataInfo data{};
            fec_rs::Encoder::ParityInfo completed{};
            const bool group_complete = rs_encoders[stream].add(
                wire, wire_len, &next_fec_group_id, &data, &completed);
            fec_wire::encode(packet + used, fec_wire::Kind::kData, cfg.session_id,
                             data.group_id, data.shard_index, cfg.fec_group_size,
                             wire_len, cfg.compact_wire);
            if (group_complete) {
              if (pending_parity_count + 2 > pending_parities.size()) {
                std::fprintf(stderr, "too many completed FEC groups in one batch\n");
                fatal_socket_error = true;
                return;
              }
              for (uint16_t p = 0; p < 2; ++p) {
                PendingParity& saved = pending_parities[pending_parity_count++];
                saved.group_id = completed.group_id;
                saved.shard_index = completed.shard_count + p;
                saved.shard_count = completed.shard_count;
                saved.frame_len = completed.frame_len;
                std::memcpy(saved.bytes.data(), completed.bytes[p], completed.frame_len);
              }
            }
          } else {
            udp_wire::encode(packet + used, cfg.session_id, source_header.seq_id,
                             wire_len, cfg.compact_wire ? udp_wire::Format::kCompact
                                                        : udp_wire::Format::kFull);
          }
          used += udp_wire::kHeaderSize;
          std::memcpy(packet + used, wire, wire_len);
          used += wire_len;
          ++frame_count;
          batch_last_seq = source_header.seq_id;
        };
        const uint8_t* first_wire = frame;
        uint32_t first_wire_len = frame_len;
        if (cfg.compact_wire) {
          if (!compact_wire::encode(frame, frame_len, compact_frame, &first_wire_len)) {
            std::fprintf(stderr, "compact wire encoding failed\n");
            fatal_socket_error = true;
          } else {
            first_wire = compact_frame;
          }
        }
        if (!fatal_socket_error) append(first_wire, first_wire_len, fixed_header);
        while (cfg.count == 0 || frames_read < cfg.count) {
          uint32_t next_len = 0;
          uint64_t next_resume = 0;
          const auto next_status = input.read(read_index, pending_frame, &next_len, &next_resume);
          if (next_status == shm::Ring::FrameStatus::kEmpty) break;
          if (next_status == shm::Ring::FrameStatus::kLapped) {
            ++input_lapped;
            read_index = next_resume;
            continue;
          }
          ++read_index;
          ++frames_read;
          last_progress = util::now_ns();
          msg::Header next_header{};
          std::memcpy(&next_header, pending_frame, sizeof(next_header));
          if (!udp_wire::valid_frame(pending_frame, next_len, next_header.seq_id)) {
            ++invalid_frames;
            continue;
          }
          uint32_t next_wire_len = next_len;
          if (cfg.compact_wire &&
              !compact_wire::encode(pending_frame, next_len, compact_frame, &next_wire_len)) {
            std::fprintf(stderr, "compact wire encoding failed\n");
            fatal_socket_error = true;
            break;
          }
          if (used + udp_wire::kHeaderSize + next_wire_len > batch_limit) {
            pending_frame_len = next_len;
            has_pending_frame = true;
            --frames_read;
            --read_index;
            break;
          }
          append(cfg.compact_wire ? compact_frame : pending_frame, next_wire_len, next_header);
          ++batch_frames;
        }
        if (!fatal_socket_error) {
          const size_t batch_start = packet_prefix;
          const uint32_t inner_bytes = static_cast<uint32_t>(used - batch_start);
          if (cfg.fec_packet_xor4) {
            udp_batch_wire::encode_packet_fec(packet + batch_start, cfg.session_id,
                                              frame_count, inner_bytes);
          } else {
            udp_batch_wire::encode(packet, cfg.session_id, frame_count);
          }
          data_packet_bytes = cfg.fec_packet_xor4 ? packet_prefix + inner_bytes : used;
          if (cfg.fec_packet_xor4) {
            packet_fec_wire::Encoder::DataInfo data{};
            packet_fec_wire::Encoder::ParityInfo parity{};
            const bool group_complete = packet_fec_encoder.add(
                packet + batch_start, inner_bytes, &next_packet_fec_group_id, &data, &parity);
            if (!group_complete && data.group_id == 0) {
              std::fprintf(stderr, "packet FEC encoding failed\n");
              fatal_socket_error = true;
            } else {
              packet_fec_wire::encode(packet, packet_fec_wire::Kind::kData, cfg.session_id,
                                      data.group_id, data.shard_index,
                                      0,
                                      inner_bytes);
              if (group_complete) {
                alignas(64) std::array<uint8_t, udp_batch_wire::kMaxDatagram> parity_packet{};
                packet_fec_wire::encode(parity_packet.data(), packet_fec_wire::Kind::kParity,
                                        cfg.session_id, parity.group_id, parity.shard_count,
                                        parity.shard_count, parity.payload_len);
                std::memcpy(parity_packet.data() + packet_fec_wire::kHeaderSize,
                            parity.bytes, parity.payload_len);
                if (cfg.connected_udp && destination_count == 1) {
                  std::array<const uint8_t*, 9> bytes{};
                  std::array<size_t, 9> lengths{};
                  bytes[0] = packet; lengths[0] = data_packet_bytes;
                  bytes[1] = parity_packet.data(); lengths[1] = packet_fec_wire::kHeaderSize + parity.payload_len;
                  const int sent_datagrams = send_connected_datagrams(bytes, lengths, 2);
                  result = sent_datagrams < 0 ? SendResult::kFatal :
                      (sent_datagrams != 0 ? SendResult::kSent : SendResult::kDropped);
                  if (sent_datagrams > 1) ++parity_sent;
                  profile_send_active = false;
                } else {
                  result = send_datagram(packet, data_packet_bytes);
                  profile_send_active = false;
                  // A local send-queue drop can affect one unicast target but
                  // not the others.  Still offer parity to every target: a
                  // receiver that got this data datagram needs its group's
                  // parity independently of another receiver's link.
                  if (result != SendResult::kFatal) {
                    const SendResult parity_result = send_datagram(
                        parity_packet.data(), packet_fec_wire::kHeaderSize + parity.payload_len);
                    if (parity_result != SendResult::kFatal) ++parity_sent;
                    if (parity_result == SendResult::kFatal) fatal_socket_error = true;
                  }
                }
              } else {
                result = send_datagram(packet, data_packet_bytes);
                profile_send_active = false;
              }
            }
          } else if (cfg.connected_udp && destination_count == 1 && pending_parity_count != 0) {
            std::array<std::array<uint8_t, udp_batch_wire::kMaxDatagram>, 8> parity_packets{};
            std::array<const uint8_t*, 9> datagram_bytes{};
            std::array<size_t, 9> datagram_lengths{};
            datagram_bytes[0] = packet;
            datagram_lengths[0] = used;
            for (size_t i = 0; i < pending_parity_count; ++i) {
              const PendingParity& saved = pending_parities[i];
              uint8_t* parity_packet = parity_packets[i].data();
              fec_wire::encode(parity_packet, fec_wire::Kind::kParity, cfg.session_id,
                               saved.group_id, saved.shard_index, saved.shard_count,
                               saved.frame_len, cfg.compact_wire);
              std::memcpy(parity_packet + fec_wire::kHeaderSize, saved.bytes.data(),
                          saved.frame_len);
              datagram_bytes[i + 1] = parity_packet;
              datagram_lengths[i + 1] = fec_wire::kHeaderSize + saved.frame_len;
            }
            const int sent_datagrams = send_connected_datagrams(
                datagram_bytes, datagram_lengths, pending_parity_count + 1);
            if (sent_datagrams < 0) {
              result = SendResult::kFatal;
            } else {
              result = sent_datagrams != 0 ? SendResult::kSent : SendResult::kDropped;
              if (sent_datagrams > 1) {
                parity_sent += static_cast<uint64_t>(sent_datagrams - 1);
              }
            }
            profile_send_active = false;
          } else if (!cfg.fec_packet_xor4) {
            result = send_datagram(packet, used);
            profile_send_active = false;
          }
          if (result != SendResult::kFatal &&
              !(cfg.connected_udp && destination_count == 1 && pending_parity_count != 0)) {
            for (size_t i = 0; i < pending_parity_count; ++i) {
              const PendingParity& saved = pending_parities[i];
              fec_wire::encode(packet, fec_wire::Kind::kParity, cfg.session_id,
                               saved.group_id, saved.shard_index, saved.shard_count,
                               saved.frame_len, cfg.compact_wire);
              std::memcpy(packet + fec_wire::kHeaderSize, saved.bytes.data(), saved.frame_len);
              const SendResult parity_result = send_datagram(
                  packet, fec_wire::kHeaderSize + saved.frame_len);
              if (parity_result != SendResult::kFatal) ++parity_sent;
              if (parity_result == SendResult::kFatal) fatal_socket_error = true;
              if (fatal_socket_error) break;
            }
          }
        }
    } else if (!cfg.fec_xor8 && !cfg.fec_rs8_2) {
      const uint8_t* wire_frame = frame;
      uint32_t wire_len = frame_len;
      udp_wire::Format format = udp_wire::Format::kFull;
      if (cfg.compact_wire) {
        if (!compact_wire::encode(frame, frame_len, compact_frame, &wire_len)) {
          std::fprintf(stderr, "compact wire encoding failed\n");
          fatal_socket_error = true;
          break;
        }
        wire_frame = compact_frame;
        format = udp_wire::Format::kCompact;
      }
      udp_wire::encode(packet, cfg.session_id, fixed_header.seq_id, wire_len, format);
      std::memcpy(packet + udp_wire::kHeaderSize, wire_frame, wire_len);
      data_packet_bytes = udp_wire::kHeaderSize + wire_len;
      result = send_datagram(packet, data_packet_bytes);
    } else if (cfg.fec_xor8) {
      const uint8_t* wire_frame = frame;
      uint32_t wire_len = frame_len;
      if (cfg.compact_wire) {
        if (!compact_wire::encode(frame, frame_len, compact_frame, &wire_len)) {
          std::fprintf(stderr, "compact wire encoding failed\n");
          fatal_socket_error = true;
          break;
        }
        wire_frame = compact_frame;
      }
      const size_t stream = fec_stream_index(wire_len);
      if (stream >= fec_encoders.size()) {
        std::fprintf(stderr, "unsupported frame size for XOR FEC: %u\n", frame_len);
        fatal_socket_error = true;
        break;
      }
      fec_xor::Encoder::DataInfo data{};
      fec_xor::Encoder::ParityInfo completed{};
      const bool group_complete = fec_encoders[stream].add(
          wire_frame, wire_len, &next_fec_group_id, &data, &completed);
      fec_wire::encode(packet, fec_wire::Kind::kData, cfg.session_id,
                       data.group_id, data.shard_index, cfg.fec_group_size,
                       wire_len, cfg.compact_wire);
      std::memcpy(packet + fec_wire::kHeaderSize, wire_frame, wire_len);
      data_packet_bytes = fec_wire::kHeaderSize + wire_len;
      result = send_datagram(packet, data_packet_bytes);
      profile_send_active = false;
      if (group_complete) send_parity(completed);
    } else {
      const uint8_t* wire_frame = frame;
      uint32_t wire_len = frame_len;
      if (cfg.compact_wire) {
        if (!compact_wire::encode(frame, frame_len, compact_frame, &wire_len)) {
          std::fprintf(stderr, "compact wire encoding failed\n");
          fatal_socket_error = true;
          break;
        }
        wire_frame = compact_frame;
      }
      const size_t stream = fec_stream_index(wire_len);
      fec_rs::Encoder::DataInfo data{}; fec_rs::Encoder::ParityInfo completed{};
      const bool complete = rs_encoders[stream].add(wire_frame, wire_len, &next_fec_group_id, &data, &completed);
      fec_wire::encode(packet, fec_wire::Kind::kData, cfg.session_id, data.group_id, data.shard_index, cfg.fec_group_size, wire_len, cfg.compact_wire);
      std::memcpy(packet + fec_wire::kHeaderSize, wire_frame, wire_len);
      data_packet_bytes = fec_wire::kHeaderSize + wire_len;
      result = send_datagram(packet, data_packet_bytes);
      profile_send_active = false;
      if (complete) send_rs_parity(completed);
    }
    if (result == SendResult::kFatal || fatal_socket_error) {
      profile_operation_active = false;
      profile_send_active = false;
      fatal_socket_error = true;
      break;
    }
    if (result == SendResult::kSent) {
      if (first_sent_seq == 0) first_sent_seq = fixed_header.seq_id;
      last_sent_seq = batch_last_seq;
      sent += batch_frames;
      ++data_datagrams;
      data_payload_bytes += data_packet_bytes;
      if (data_packet_bytes > data_payload_bytes_max) data_payload_bytes_max = data_packet_bytes;
      ++data_frame_hist[batch_frames < data_frame_hist.size() ? batch_frames : data_frame_hist.size() - 1];
    }
    if (profile_operation) {
      const uint64_t operation_elapsed_ns = util::steady_now_ns() - operation_started_ns;
      sender_operation_samples.record(operation_elapsed_ns, fixed_header.seq_id);
      if (operation_elapsed_ns >= profile_operation_send_elapsed_ns) {
        sender_prepare_samples.record(operation_elapsed_ns - profile_operation_send_elapsed_ns,
                                      fixed_header.seq_id);
      }
    }
    profile_operation_active = false;
    profile_send_active = false;
  }
  if (cfg.fec_xor8 && !fatal_socket_error) {
    for (auto& encoder : fec_encoders) {
      fec_xor::Encoder::ParityInfo partial{};
      if (encoder.flush(&partial)) send_parity(partial);
      if (fatal_socket_error) break;
    }
  }
  if (cfg.fec_rs8_2 && !fatal_socket_error) {
    for (auto& encoder : rs_encoders) {
      fec_rs::Encoder::ParityInfo partial{};
      if (encoder.flush(&partial)) send_rs_parity(partial);
      if (fatal_socket_error) break;
    }
  }
  if (cfg.fec_packet_xor4 && !fatal_socket_error) {
    packet_fec_wire::Encoder::ParityInfo parity{};
    if (packet_fec_encoder.flush(&parity)) {
      alignas(64) std::array<uint8_t, udp_batch_wire::kMaxDatagram> parity_packet{};
      packet_fec_wire::encode(parity_packet.data(), packet_fec_wire::Kind::kParity,
                              cfg.session_id, parity.group_id, parity.shard_count,
                              parity.shard_count, parity.payload_len);
      std::memcpy(parity_packet.data() + packet_fec_wire::kHeaderSize,
                  parity.bytes, parity.payload_len);
      const SendResult parity_result = send_datagram(
          parity_packet.data(), packet_fec_wire::kHeaderSize + parity.payload_len);
      if (parity_result == SendResult::kSent) ++parity_sent;
      if (parity_result == SendResult::kFatal) fatal_socket_error = true;
    }
  }
  for (int fd : socket_fds) if (fd >= 0) close(fd);
  munmap(input_base, input_size);
  close(input_fd);
  rusage usage_end{};
  (void)getrusage(RUSAGE_SELF, &usage_end);
  std::fprintf(stderr,
               "udp_sender: session=%llu fec=%s wire_format=%s udp_mode=%s fanout_mode=%s destinations=%zu frames_read=%llu sent=%llu data_datagrams=%llu parity_sent=%llu first_seq=%llu last_seq=%llu "
               "data_payload_bytes=%llu data_payload_bytes_max=%llu batch_frames_1=%llu batch_frames_2=%llu batch_frames_3=%llu batch_frames_4=%llu batch_frames_5=%llu batch_frames_6=%llu batch_frames_7plus=%llu "
               "input_lapped=%llu invalid=%llu socket_drops=%llu destination_drops=%llu,%llu,%llu cpu_user_ns=%llu cpu_system_ns=%llu voluntary_cs=%ld involuntary_cs=%ld minor_faults=%ld major_faults=%ld\n",
               static_cast<unsigned long long>(cfg.session_id),
               cfg.fec_packet_xor4 ? "packet_xor4" : (cfg.fec_xor8 ? (cfg.fec_group_size == 4 ? "xor4" : "xor8") : (cfg.fec_rs8_2 ? (cfg.fec_group_size == 4 ? "rs4_2" : "rs8_2") : "none")),
               cfg.compact_wire ? "compact" : "full",
               cfg.connected_udp ? "connected" : "sendto",
               effective_sendmmsg ? "sendmmsg" : "loop",
               destination_count,
               static_cast<unsigned long long>(frames_read),
               static_cast<unsigned long long>(sent),
               static_cast<unsigned long long>(data_datagrams),
               static_cast<unsigned long long>(parity_sent),
               static_cast<unsigned long long>(first_sent_seq),
               static_cast<unsigned long long>(last_sent_seq),
               static_cast<unsigned long long>(data_payload_bytes),
               static_cast<unsigned long long>(data_payload_bytes_max),
               static_cast<unsigned long long>(data_frame_hist[1]),
               static_cast<unsigned long long>(data_frame_hist[2]),
               static_cast<unsigned long long>(data_frame_hist[3]),
               static_cast<unsigned long long>(data_frame_hist[4]),
               static_cast<unsigned long long>(data_frame_hist[5]),
               static_cast<unsigned long long>(data_frame_hist[6]),
               static_cast<unsigned long long>(data_frame_hist[7]),
               static_cast<unsigned long long>(input_lapped),
               static_cast<unsigned long long>(invalid_frames),
               static_cast<unsigned long long>(socket_drops),
               static_cast<unsigned long long>(destination_drops[0]),
               static_cast<unsigned long long>(destination_drops[1]),
               static_cast<unsigned long long>(destination_drops[2]),
               static_cast<unsigned long long>(timeval_ns(usage_end.ru_utime) - timeval_ns(usage_start.ru_utime)),
               static_cast<unsigned long long>(timeval_ns(usage_end.ru_stime) - timeval_ns(usage_start.ru_stime)),
               usage_end.ru_nvcsw - usage_start.ru_nvcsw,
               usage_end.ru_nivcsw - usage_start.ru_nivcsw,
               usage_end.ru_minflt - usage_start.ru_minflt,
               usage_end.ru_majflt - usage_start.ru_majflt);
  sender_operation_samples.print(stderr, "udp_sender_profile_operation");
  sender_send_samples.print(stderr, "udp_sender_profile_send");
  sender_prepare_samples.print(stderr, "udp_sender_profile_prepare");
  sender_ingress_samples.print(stderr, "udp_sender_profile_ingress");
  return fatal_socket_error ? 1 : 0;
}
