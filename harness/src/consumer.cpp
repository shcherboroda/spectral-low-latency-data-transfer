// Consumer: reads events from the shared-memory broadcast ring, stamps a receive
// timestamp, and computes delivery metrics (latency percentiles, drop rate) from
// the per-message send timestamp and sequence id. Fixed measurement end of the
// harness.
//
// Usage: consumer [--shm NAME] [--slots N] [--count N] [--from-edge]
//                 [--drop N] [--drop-through-seq SEQ] [--csv FILE] [--idle-ms MS]
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "message.h"
#include "metrics.h"
#include "shm_ring.h"
#include "shm_segment.h"
#include "stage_metrics.h"
#include "util.h"

namespace {

struct Config {
  std::string shm_name = "/fanout_ring";
  uint32_t slots = 1024;
  uint64_t count = 0;
  uint64_t drop = 0;
  uint64_t drop_through_seq = 0;
  bool from_edge = false;
  std::string csv;
  uint64_t idle_ms = 2000;
  uint32_t profile_sample_every = 0;
  uint64_t profile_from_seq = 0;
};

struct Sample {
  uint64_t seq_id = 0;
  uint64_t latency_ns = 0;
};

Config parse_args(int argc, char** argv) {
  Config c;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    auto next = [&]() -> std::string {
      if (i + 1 >= argc) {
        fprintf(stderr, "missing value for %s\n", a.c_str());
        std::exit(2);
      }
      return argv[++i];
    };
    if (a == "--shm") c.shm_name = next();
    else if (a == "--slots") c.slots = static_cast<uint32_t>(std::stoul(next()));
    else if (a == "--count") c.count = std::stoull(next());
    else if (a == "--drop") c.drop = std::stoull(next());
    else if (a == "--drop-through-seq") c.drop_through_seq = std::stoull(next());
    else if (a == "--from-edge") c.from_edge = true;
    else if (a == "--csv") c.csv = next();
    else if (a == "--idle-ms") c.idle_ms = std::stoull(next());
    else if (a == "--profile-sample-every") c.profile_sample_every = static_cast<uint32_t>(std::stoul(next()));
    else if (a == "--profile-from-seq") c.profile_from_seq = std::stoull(next());
    else {
      fprintf(stderr, "unknown arg: %s\n", a.c_str());
      std::exit(2);
    }
  }
  return c;
}

void print_report(const metrics::Report& r) {
  printf("---- delivery metrics ----\n");
  printf("received     : %llu\n", (unsigned long long)r.received);
  printf("expected     : %llu\n", (unsigned long long)r.expected);
  printf("dropped      : %llu\n", (unsigned long long)r.dropped);
  printf("drop_rate    : %.4f%%\n", r.drop_rate * 100.0);
  printf("latency (ns) : min=%llu mean=%.0f max=%llu\n",
         (unsigned long long)r.lat_min, r.lat_mean,
         (unsigned long long)r.lat_max);
  printf("  p01        : %llu\n", (unsigned long long)r.p01);
  printf("  p50        : %llu\n", (unsigned long long)r.p50);
  printf("  p99        : %llu\n", (unsigned long long)r.p99);
  printf("  p99.9      : %llu\n", (unsigned long long)r.p999);
  printf("  p99.99     : %llu\n", (unsigned long long)r.p9999);
}

}  // namespace

int main(int argc, char** argv) {
  Config cfg = parse_args(argc, argv);

  shm::Segment seg =
      shm::Segment::open(cfg.shm_name, shm::region_size(cfg.slots), /*create=*/false);
  shm::Ring ring;
  ring.attach(seg.base(), cfg.slots, /*init=*/false);

  metrics::Accumulator acc(cfg.count ? cfg.count : 1u << 20);

  std::vector<Sample> samples;
  if (!cfg.csv.empty()) {
    const size_t capacity = cfg.count ? static_cast<size_t>(cfg.count) : (1u << 20);
    samples.resize(capacity);
    for (Sample& sample : samples) sample = {};
    samples.clear();
  }

  uint64_t read_index = cfg.from_edge ? ring.live_edge() : 0;
  uint64_t received = 0;
  uint64_t skipped = 0;
  uint64_t lapped_events = 0;
  const uint64_t idle_ns = cfg.idle_ms * 1000000ull;
  uint64_t last_progress = util::now_ns();
  stage_metrics::Samples consumer_output_shm_samples;

  uint8_t frame[shm::kFrameCap];
  while (cfg.count == 0 || received < cfg.count) {
    uint32_t len = 0;
    uint64_t resume = 0;
    uint64_t published_ns = 0;
    auto st = ring.read(read_index, frame, &len, &resume, &published_ns);

    if (st == shm::Ring::FrameStatus::kOk) {
      const uint64_t recv_ts = util::now_ns();
      const auto* hdr = reinterpret_cast<const msg::Header*>(frame);
      if (cfg.profile_sample_every != 0 && consumer_output_shm_samples.can_record() &&
          hdr->seq_id > cfg.profile_from_seq && hdr->seq_id % cfg.profile_sample_every == 0 &&
          recv_ts >= published_ns && published_ns != 0) {
        consumer_output_shm_samples.record(recv_ts - published_ns, hdr->seq_id);
      }
      if (skipped < cfg.drop || hdr->seq_id <= cfg.drop_through_seq) {
        ++skipped;
        ++read_index;
        last_progress = recv_ts;
        continue;
      }
      const uint64_t latency =
          recv_ts > hdr->send_ts_ns ? recv_ts - hdr->send_ts_ns : 0;
      acc.record(hdr->seq_id, latency);
      if (!cfg.csv.empty()) samples.push_back({hdr->seq_id, latency});
      ++received;
      ++read_index;
      last_progress = recv_ts;
    } else if (st == shm::Ring::FrameStatus::kLapped) {
      ++lapped_events;
      read_index = resume;  // skip the gap; drops show up as seq gaps in metrics
    } else {  // kEmpty
      if (util::now_ns() - last_progress > idle_ns) break;  // producer done
    }
  }

  if (!cfg.csv.empty()) {
    FILE* csv = std::fopen(cfg.csv.c_str(), "w");
    if (csv == nullptr) {
      std::fprintf(stderr, "consumer: cannot open csv output: %s\n", cfg.csv.c_str());
      return 1;
    }
    std::fprintf(csv, "seq,latency_ns\n");
    for (const Sample& sample : samples) {
      std::fprintf(csv, "%llu,%llu\n", (unsigned long long)sample.seq_id,
                   (unsigned long long)sample.latency_ns);
    }
    std::fclose(csv);
  }

  fprintf(stderr, "consumer: lapped %llu times\n",
          (unsigned long long)lapped_events);
  fprintf(stderr, "consumer: skipped %llu warm-up messages\n",
          (unsigned long long)skipped);
  consumer_output_shm_samples.print(stderr, "consumer_profile_output_shm");
  print_report(acc.report());
  return 0;
}
