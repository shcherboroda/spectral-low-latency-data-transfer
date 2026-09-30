# Fan-out benchmark harness — producer & consumer

Two small, standalone C++17 binaries that form the **fixed ends** of the
benchmark: a **producer** that generates timestamped, sequence-numbered
events, and a **consumer** that receives them and reports delivery latency and
drop rate.

## What's here

```
harness/
  include/
    message.h      fixed-size frame header (seq_id + send_ts_ns) + payload
    shm_ring.h     shared-memory broadcast ring (single producer, N readers)
    shm_segment.h  POSIX shm_open + mmap RAII wrapper
    metrics.h      latency-percentile + drop-rate accumulator
    util.h         now_ns() — nanoseconds since the epoch (std::chrono::system_clock)
  src/
    producer.cpp   generates events, stamps seq_id + send_ts_ns, publishes to ring
    consumer.cpp   reads ring, stamps recv_ts, computes metrics
  test/
    test_harness.cpp  assertion tests for the ring and the metrics accumulator
  Makefile
```

## Message format

Every message is a fixed-size, 64-byte-aligned struct beginning with a common
`Header`, followed by type-specific market-data fields (`message.h`):

```
Header: seq_id (u64), send_ts_ns (u64), type (u16), version (u16), body_len (u32)

Trade      symbol/venue/currencies, ids, price, quantity, aggressor side, flags, ...
Bbo        symbol/venue, best bid/ask price+size, order counts, flags, ...
OrderBook  symbol/venue, update ids, 5 bid levels + 5 ask levels, checksum, ...
```

`seq_id` is a monotonic counter starting at 1; `send_ts_ns` is stamped
immediately before publish. Those two `Header` fields are all the consumer needs
to measure latency (recv_ts − send_ts_ns) and detect drops (gaps in seq_id) — it
never has to interpret the body.

## Basic transport: shared-memory broadcast ring

The provided transport is an in-process / same-host **shared-memory ring** (`shm_ring.h`).
It is deliberately the simplest thing that behaves like a realistic market-data fan-out:

- A slow subscriber cannot stall the producer or other readers.
- A reader that falls too far behind is **lapped**: it detects the overwrite via
  the per-slot sequence number and skips ahead.
- Publication is one release-store of the slot sequence; readers busy-spin on an
  acquire-load.

## Build & test

```bash
make          # builds producer, consumer, udp_sender, and udp_receiver
make test     # builds and runs the assertion tests
make test-integration  # loopback producer -> UDP spine -> consumer smoke test
make test-fanout-integration  # one sender -> two independent UDP receivers
make test-fec-integration  # Linux/netns check of XOR(8+1) recovery (needs root)
```

## Isolated UDP loss benchmark (WSL/Linux)

`bench/run_udp_netem.sh` runs the UDP spine through a fresh pair of Linux
network namespaces joined by a private veth pair. It applies `tc netem` loss
only to the sender-side private veth; it never changes `eth0`, `lo`, Docker
interfaces, or an existing qdisc. The script records a manifest, exact process
logs, network setup log, final `qdisc.txt` counters, hashes, and parsed
counters in a new result directory. `netem_qdisc_dropped_datagrams` is the
physical loss injected by the emulator; it must not be confused with residual
message gaps after FEC and resequencing.
It removes only the namespaces, veth endpoints, processes, and POSIX SHM names
that it generated, including after interruption.

It needs `ip`, `tc`, `taskset`, and non-interactive sudo from inside WSL/Linux:

```bash
make -C harness
harness/bench/run_udp_netem.sh --loss 0 --count 100000 --rate 200000 \
  --slots 65536 --cpu-producer 2 --cpu-sender 3 --cpu-receiver 4 --cpu-consumer 5

# A short controlled-loss validation and a per-message diagnostic CSV:
harness/bench/run_udp_netem.sh --loss 0.1 --count 100000 --rate 200000 \
  --slots 65536 --consumer-csv
```

Use `--help` for all parameters and `--dry-run` to inspect the private topology
without privileged changes. `--output /new/path` selects a **new** result
directory; otherwise a unique directory is created below `harness/results/`.
The harness fails on sender input/socket errors, receiver invalid/session
errors, or output-SHM lapping. Packet loss injected by netem is expected and is
reported, not itself a harness failure.

For a diagnosis of an unexpected gap with zero configured loss, add
`--socket-drop-counters`. It enables Linux `SO_RXQ_OVFL` only for that run and
records `receiver_socket_drops`. A nonzero value proves receiver UDP-queue
overflow; zero means no overflow was observed on a subsequently received
datagram, not that end-of-stream loss is impossible. The paired
`receiver_socket_drop_counter` states whether the facility was enabled.

`--warmup-ms` converts the requested duration to a nominal `seq_id` boundary
using the configured rate. It is a reproducible stream-count warm-up, not a
guarantee of wall-clock quiet time when the host pauses the producer.
When `--profile-sample-every` is enabled, the runner passes this same boundary
to every stage profiler and each report records its sampled `first_seq` and
`last_seq`.

`bench/run_shm_baseline.sh` is the corresponding scheduler/noise-floor control.
It warms an unbounded producer, then starts the consumer at the live edge and
collects an exact steady-state sample without UDP or network namespaces:

```bash
harness/bench/run_shm_baseline.sh --count 1000000 --rate 50000 --slots 65536 \
  --cpu-producer 0 --cpu-consumer 4 --consumer-csv
```

Compare its repeated p99–p99.99 range with the UDP runs before attributing a
tail change to the transport.

Both benchmark scripts accept `--rt-priority N` to run only their benchmark
processes under `SCHED_FIFO` priority `N` (`1` through `99`; default `0`). This
experimental control requires passwordless sudo and one distinct physical core
for every busy-spinning process plus one controller core; the scripts reject an
unsafe layout. FIFO can starve normal kernel/network work and cannot eliminate
pauses imposed by the Windows host. Use the same value for every compared
variant and retain normal-scheduler results as the reference.

`bench/run_udp_fanout_loopback.sh` is the local replication control for one,
two, or three receiver/consumer pairs. It creates unique SHM edges and ports,
stores one directory per receiver, and emits a compact `result.txt` suitable
for an A/B matrix:

```bash
harness/bench/run_udp_fanout_loopback.sh --receivers 3 --fec none \
  --count 1000000 --rate 50000 --slots 65536
```

Pass `--sender-workers auto` to make the sender use at most one worker per
receiver, bounded by the physical CPUs available to its current affinity mask.
This removes serial fan-out from the sender's fast path; it does not create
extra CPUs, so interpret three-receiver loopback tails cautiously on a laptop.

It uses loopback and deliberately does **not** inject packet loss. Its value is
isolating sender replication and local receiver cost; it must not be presented
as an AWS ENA latency measurement. On this laptop, three receiver/consumer
pairs outnumber available physical cores, so only delivery correctness and
large regressions are comparable at fan-out three.

`bench/run_udp_matrix.sh` is a guard-railed single-receiver raw/FEC matrix
planner. It is dry-run by default, so an unattended browser or background load
cannot accidentally produce misleading tail measurements. When the machine is
idle, run a short comparable series explicitly:

```bash
harness/bench/run_udp_matrix.sh --profile short --count 500000 --repeats 3 \
  --cpus 0,2,4,6 --output /tmp/udp-matrix --execute
```

The top-level `matrix.tsv` joins each run's percentile values, network loss,
and FEC recovery counter. Keep the individual run directories: they contain
the immutable manifests and raw logs needed to audit an apparent improvement.

## Candidate transport: UDP unicast, with an optional recovery experiment

The repository also builds `bin/udp_sender` and `bin/udp_receiver`. The default
data path (`--fec none`) is one unchanged harness frame per IPv4 UDP datagram,
with a small network-byte-order envelope for session, sequence, and length
validation. It deliberately has no retransmission, multicast, or batching.
Because the enclosed frame is the existing native C++ object representation,
this first version requires producer and receiver hosts with the same message
ABI and byte order; only the UDP envelope itself is architecture-independent.

`--fec xor4` and `--fec xor8` are systematic XOR(k+1) recovery experiments.
Data datagrams are delivered immediately; after every group of up to k
same-sized frames the sender transmits one parity datagram. The receiver can
reconstruct exactly one missing frame from a group, then sends it to the output
ring through the normal de-duplication path. `xor4` adds 25% network traffic
for full groups and `xor8` adds 12.5%; neither can repair two missing data
frames, or a data loss combined with a lost parity frame.

`--fec rs4_2` and `--fec rs8_2` are systematic Reed-Solomon variants with two
parity datagrams per group. They can recover up to two missing shards when the
remaining data and parity shards are sufficient, at the cost of more network
traffic and finite-field computation. Groups for every FEC mode are kept
separate for the 192-byte Trade/BBO frames and the 576-byte OrderBook frames,
so a parity datagram is never inflated to the largest mixed-stream payload
size. Treat every FEC mode as a separately benchmarked delivery/latency
profile; none is assumed to be free.

The receiver publishes only a strictly increasing sequence. Its
`--reorder-wait-us` deadline is therefore a delivery-versus-tail control: a
larger value gives late parity and reordered data more time to repair a gap,
but holds later messages behind that gap. The binary's static low-latency
default is `25` us. The provided benchmark runners use `auto`, which derives
six producer timestamp intervals and clamps the result to 25–100 us; retain
both profiles in any comparison against the target network.

The provided UDP benchmark runners use full-frame packing up to `1472` bytes
by default. Packing combines only frames already available in the input ring;
it does not wait for a packet to fill. Pass `--batch-bytes 0` for the raw
one-frame-per-datagram control.

`--wire-format compact` is a separate raw-UDP payload experiment. It preserves
the exact fixed `Header` and reconstructs the fixed full frame before the
consumer, while omitting duplicated timestamps, derived fields, and known-zero
reserved fields from the network representation. Its encoded sizes are Trade
`145` bytes (from `192`), BBO `137` (from `192`), and OrderBook `430` (from
`576`). This profile is valid only for the supplied producer workload: it
reconstructs producer-derived fields and rejects an input frame that violates
its checked assumptions. It can be combined with frame-level XOR or
Reed–Solomon FEC, but every such combination remains a separately benchmarked
delivery/latency profile.
The sender fail-fast rejects an input frame that violates those producer-profile
assumptions rather than silently rewriting its data.

`--fec packet_xor4 --wire-format compact --batch-bytes 1472` is an
experimental packet-level XOR(4+1) profile. It protects a whole packed UDP
data datagram, rather than its individual frames, with one independent parity
datagram. Thus one lost data datagram can be restored as a unit without the
same loss erasing several related frame-level shards. The final partial group
also emits parity. The profile uses a distinct UDB2 inner header with an exact
payload length so recovered zero-padded XOR data can be parsed safely. It is
not the default: compare delivery and tail latency against frame-level `xor4`
under the same loss seed and traffic profile before selecting it.

### Profile selection

The selected speed-and-tail candidate is `xor4` with the full wire format,
connected UDP sockets, and opportunistic 1472-byte packing:

```text
--fec xor4 --wire-format full --udp-mode connected --batch-bytes 1472 \
--reorder-wait-us auto
```

It is the lowest-overhead recovery profile: one parity datagram for four data
frames. Packing can place several frame-level FEC shards in one UDP datagram,
so it is a speed/tail candidate rather than a universal loss-delivery choice.
Compare it with `--batch-bytes 0` under the same loss model before claiming
delivery resilience. Do not select a profile from a single percentile run. Run
the recovery matrix with at least
three repetitions and retain its `matrix.tsv`, manifests, logs, and qdisc
counters:

```bash
harness/bench/run_udp_matrix.sh --profile recovery --count 1000000 \
  --warmup-ms 500 --repeats 3 --cpus 0,2,4,6 \
  --output /tmp/udp-recovery-matrix --execute
```

Replace the CPU list with four otherwise idle CPUs on the actual host, or omit
`--cpus` if isolation is unavailable. The threshold for acceptable residual
message loss belongs to the downstream trading use case; it must be stated in
the result rather than silently assumed by the transport.

Use these explicit alternatives only when the observed loss model justifies
their cost:

- `--fec rs4_2 --wire-format full --udp-mode connected --batch-bytes 1472`
  adds two parity datagrams per four data frames (50% overhead) and is the
  profile for short loss bursts or two losses within one group.
- `--fec packet_xor4 --wire-format compact --udp-mode connected --batch-bytes 1472`
  adds 25% packet-level parity and is the profile for independently scattered
  packet loss. It is valid only for the supplied compact-codec producer
  profile, as described above; it is not a generic payload codec.

The sender and receiver must use the same `--fec`, `--wire-format`, and
`--batch-bytes` settings. Record the selected command line and the resulting
`measurement_missing_total`, `receiver_fec_recovered`, percentiles, and qdisc
loss for every receiver. A zero `receiver_socket_drops` value only means no
overflow was observed on a later received datagram; it does not exclude an
end-of-stream UDP queue loss.

Start the processes in this order so both shared-memory edges exist before data
flows (use a large enough ring to absorb setup jitter):

```bash
# terminal A: network receiver creates the consumer-facing ring
./bin/udp_receiver --out-shm /udp_out --out-slots 65536 \
    --bind 127.0.0.1 --port 9000 --count 100000 --idle-ms 5000

# terminal B: fixed consumer attaches to the output ring
./bin/consumer --shm /udp_out --slots 65536 --count 100000 --idle-ms 5000

# terminal C: sender waits for the producer-facing ring
./bin/udp_sender --in-shm /udp_in --in-slots 65536 \
    --host 127.0.0.1 --port 9000 --count 100000 --wait-ms 0 --idle-ms 5000

# terminal D: fixed producer creates the input ring and starts the stream
./bin/producer --shm /udp_in --slots 65536 --count 100000 \
    --rate 50000 --type mixed
```

`udp_sender` also accepts `--from-edge`, `--session-id`, `--socket-buffer`, and
`--fec none|xor4|xor8|rs4_2|rs8_2|packet_xor4`. `udp_receiver` accepts `--session-id`,
`--dedupe-window` (a power of two), `--socket-buffer`, and the matching FEC
mode. Both
print transport counters on exit; FEC mode also prints parity, recovered, and
bounded-state eviction counters. The receiver's
`apparent_missing` covers gaps between the first and last valid datagrams it
observed; like the fixed consumer metric, it cannot infer a missing prefix or
suffix without an external run manifest.

### Sender-side unicast fan-out

For one receiver, keep using the compatible `--host IP --port PORT` form. For
two or three receivers, use a repeated `--target IPv4:PORT`; it cannot be
mixed with `--host` or `--port` in the same invocation:

```bash
./bin/udp_sender --in-shm /udp_in --in-slots 65536 --count 100000 \
  --target 10.0.1.20:9000 \
  --target 10.0.1.21:9000 \
  --target 10.0.1.22:9000
```

The sender performs one nonblocking `sendto` per destination for every data
and parity datagram. `sent` counts frames accepted by all destination socket
calls; `socket_drops` and `destination_drops` expose local send-queue failures.
There is intentionally no per-receiver retransmission or sender-side queue in
this first fan-out variant: adding either changes the fast-path tail and must
be benchmarked separately.

For Linux comparison only, `--fanout-mode sendmmsg` sends the destination set
through one `sendmmsg` syscall per frame. The portable default is `loop`.
`sendmmsg` is an experiment, not an assumed improvement: use the loopback
matrix first, then confirm any winner on the target Ubuntu/ENA hosts.

## Two-host Ubuntu/ENA run

The WSL netem runner is a correctness and controlled-loss tool, not a model of
the AWS evaluation path. For a real two-host run, build the same commit on a TX
host and one RX host per receiver, then use the following roles. Record the
commit, `uname -a`, instance type, ENA driver (`ethtool -i IFACE`), MTU,
addresses, CPU pinning, and every command in the result manifest.

On each RX host, start its receiver and consumer before the sender starts:

```bash
./bin/udp_receiver --out-shm /udp_out --out-slots 65536 --bind 0.0.0.0 \
  --port 9000 --count 1000000 --idle-ms 10000
./bin/consumer --shm /udp_out --slots 65536 --from-edge --count 1000000 \
  --idle-ms 10000 --csv consumer.csv
```

On the TX host, start the sender with one `--target` for every RX host, then
start the fixed producer. Do not compare absolute one-way timestamps across
hosts until the clock treatment has been recorded; compare delivery and local
transport counters first. A remote orchestration script will be added only
after the actual AWS access and SSH assumptions are known, so it cannot hide
environment-specific setup behind untested defaults.

## Run the baseline (one host)

```bash
# terminal A — start the producer (creates the shm segment)
./bin/producer --count 500000 --rate 200000 --type mixed

# terminal B — consumer tracks the live edge and reports on idle
./bin/consumer --from-edge
```

Or scripted, one host (pin to isolated cores for a clean tail — see
[Core isolation & pinning](#core-isolation--pinning)):

```bash
taskset -c 2 ./bin/producer --count 500000 --rate 200000 --type mixed &
sleep 0.02
taskset -c 4 ./bin/consumer --from-edge --csv latencies.csv
```

Example output (single host, cores 2/4 isolated):

```
---- delivery metrics ----
received     : 495904
expected     : 495904
dropped      : 0
drop_rate    : 0.0000%
latency (ns) : min=141 mean=235 max=15217
  p01        : 152
  p50        : 218
  p99        : 457
  p99.9      : 808
  p99.99     : 6935
```

`received` is below the producer's `--count 500000` because `--from-edge` makes
the consumer start at the producer's live edge and skip whatever was published
before it attached; `expected` is counted from the first seq_id actually seen, so
`dropped` stays 0 and the skipped prefix is not mistaken for loss.

The `--csv` file (`seq,latency_ns` per row) is what the analysis notebook reads
to build percentile plots.

### Producer flags

| flag | default | meaning |
|------|---------|---------|
| `--shm NAME`     | `/fanout_ring` | shared-memory segment name |
| `--slots N`      | `1024` | ring slot count (power of two) |
| `--count N`      | `1000000` | messages to send (0 = unlimited) |
| `--rate R`       | `0` | target msgs/sec (0 = as fast as possible) |
| `--type T`       | `mixed` | message type: `trade`, `bbo`, `book`, or `mixed` |

### Consumer flags

| flag | default | meaning |
|------|---------|---------|
| `--shm NAME`     | `/fanout_ring` | segment name (must match producer) |
| `--slots N`      | `1024` | slot count (must match producer) |
| `--count N`      | `0` | stop after N messages (0 = run until idle) |
| `--from-edge`    | off | start at the producer's live edge (skip startup catch-up) |
| `--csv FILE`     | — | write per-message `seq,latency_ns` rows |
| `--idle-ms MS`   | `2000` | exit after this long with no new messages |

> Without `--from-edge` a consumer that starts after the producer has a
> head start will "catch up" through a backlog, inflating the early tail. Use
> `--from-edge` for clean steady-state latency, or start the consumer first.

## Measuring latency honestly

Both timestamps come from the same helper, `util::now_ns()`, which reads
`std::chrono::system_clock`. On **one machine** that means `send_ts_ns` and
`recv_ts` share a clock, so the reported latency is a usable one-way delivery
time.

**Across machines the clocks are not synchronized** — `recv_ts − send_ts_ns`
would mostly measure clock offset, not latency. Choosing and justifying a
correct cross-host methodology (RTT/2, PTP, loopback, a shared time source) is
part of the task.

## Core isolation & pinning

Both binaries **busy-spin** — the producer optionally busy-waits for its next
send slot, and the consumer busy-polls the ring. That gives the lowest,
most consistent latency, but only if each spinning thread owns a physical core
with nothing else scheduled on it. Without isolation the tail is dominated by
the scheduler migrating the thread, another task sharing the core, and timer
interrupts — not by the transport you are trying to measure.

Recommended setup on a Linux benchmark host:

1. **Reserve cores from the scheduler at boot.** Add to the kernel command line
   (e.g. in GRUB) and reboot — pick core ids that are real, distinct physical
   cores (avoid a hyperthread sibling pair):

   ```
   isolcpus=2,4 nohz_full=2,4 rcu_nocbs=2,4
   ```

   `isolcpus` keeps the general scheduler off cores 2 and 4; `nohz_full` stops
   the periodic scheduler tick on them; `rcu_nocbs` moves RCU callbacks away.

2. **Pin each binary to an isolated core** with `taskset`:

   ```bash
   taskset -c 2 ./bin/producer --count 500000 --rate 200000 --type mixed &
   sleep 0.02
   taskset -c 4 ./bin/consumer --from-edge --csv latencies.csv
   ```

3. **Optional, sharpens the tail further:**
   - Move IRQs off the isolated cores (`/proc/irq/*/smp_affinity`).
   - Disable frequency scaling / set the `performance` cpufreq governor so the
     core does not clock down between spins.
   - Keep producer and consumer on the **same NUMA node** as the shared-memory
     segment (`numactl --cpunodebind=0 --membind=0 ...`).

Record which cores you isolated and how you pinned them alongside your results.
