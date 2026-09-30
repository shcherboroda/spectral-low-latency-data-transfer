# Spectral low-latency data-transfer solution

## Goal and scope

This solution implements the mutable middle of the supplied pipeline:

```text
producer -> input shared-memory ring -> udp_sender -> UDP unicast ->
udp_receiver -> output shared-memory ring -> consumer
```

The supplied `producer` and `consumer` remain the latency endpoints. The
consumer-facing framing fields `seq_id` and `send_ts_ns` are preserved. The
receiver emits only a strictly increasing sequence; a sequence gap is a
delivery failure, and a late repair is bounded by the resequencing deadline.

The primary objective is low end-to-end latency, especially high percentiles,
without treating material message loss as acceptable. This follows the task
description and the organizers' clarification that data gaps can affect a
trading decision.

## Primary speed-and-tail candidate

The initial profile for an AWS/ENA evaluation is:

```text
--fec xor4 --wire-format full --udp-mode connected --batch-bytes 1472 \
--reorder-wait-us auto
```

It uses UDP unicast and XOR(4+1) forward-error correction (FEC): four data
frames are followed by one parity frame. Any single lost FEC shard in that
group can be reconstructed without a request/response round trip. The 25%
traffic overhead is lower than Reed-Solomon 4+2, keeping the candidate focused
on sender cost and tail latency. Packing is opportunistic: it combines frames
already available in the input ring and never waits for a datagram to fill.
Because a packed UDP datagram can contain more than one frame-level FEC shard,
this profile is not claimed as the final loss-resilience choice until compared
with `--batch-bytes 0` and the alternatives below under the target loss model.

`connected` UDP uses a connected socket per unicast destination. It keeps UDP
semantics while avoiding a destination-address lookup on each send. There is
no TCP retransmission path, because a retransmission round trip would make a
late packet and its dependent sequence tail worse.

For multiple receivers, the sender performs unicast fan-out: one `--target
IPv4:PORT` per receiver. This matches the requirement that receivers may have
different physical links and loss patterns. Every receiver has its own
receiver and consumer process and its own delivery counters.

## Deliberate loss-profile alternatives

There is no universal FEC winner. Select one profile from measured loss and
tail results, using the same traffic and fan-out as the intended deployment.
The acceptable residual-loss threshold must be defined by the downstream
strategy or operator; the transport does not invent one.

| Observed loss pattern | Configuration | Cost and reason |
| --- | --- | --- |
| Main speed/tail profile | `xor4`, full frames | 25% parity overhead; repairs one lost frame per four-frame group. |
| Independent, scattered packet loss | `packet_xor4`, compact frames | 25% packet-level parity; a lost packed UDP datagram is repaired as one unit. Valid only for the supplied producer profile checked by the compact codec. |
| Short bursts or two losses in one four-frame group | `rs4_2`, full frames | 50% parity overhead and more computation; repairs up to two missing shards when enough shards arrive. |

`packet_xor4` is not selected by default merely because its payload is smaller:
the compact codec intentionally removes only fields that are derived,
duplicated, or known zero in the supplied producer workload, and rejects input
that violates those assumptions. Full frames remain the conservative primary
format.

## Build and run

Build the harness binaries:

```bash
make -C harness
```

For one receiver, start components in this order. Substitute the receiver's
private IPv4 address for `RX_IP`; use a distinct shared-memory name per host.

```bash
# RX host: receiver, then consumer
./harness/bin/udp_receiver --out-shm /udp_out --out-slots 65536 \
  --bind 0.0.0.0 --port 9000 --count 1000000 --idle-ms 10000 \
  --fec xor4 --wire-format full --batch-bytes 1472 --reorder-wait-us auto
./harness/bin/consumer --shm /udp_out --slots 65536 --from-edge \
  --count 1000000 --idle-ms 10000 --csv consumer.csv

# TX host: sender, then the supplied producer
./harness/bin/udp_sender --in-shm /udp_in --in-slots 65536 \
  --host RX_IP --port 9000 --count 1000000 --wait-ms 10000 --idle-ms 10000 \
  --fec xor4 --wire-format full --udp-mode connected --batch-bytes 1472
./harness/bin/producer --shm /udp_in --slots 65536 --count 1000000 \
  --rate 100000 --type mixed
```

For fan-out, replace `--host RX_IP --port 9000` with one repeated target per
receiver; all listed receivers receive the same ordered stream:

```bash
./harness/bin/udp_sender --in-shm /udp_in --in-slots 65536 \
  --target 10.0.1.20:9000 --target 10.0.1.21:9000 --target 10.0.1.22:9000 \
  --count 1000000 --wait-ms 10000 --idle-ms 10000 \
  --fec xor4 --wire-format full --udp-mode connected --batch-bytes 1472
```

To switch to an alternative, replace the matching FEC/wire flags on *both*
sender and receiver:

```bash
# Burst-loss option
--fec rs4_2 --wire-format full --batch-bytes 1472

# Independently scattered-loss option
--fec packet_xor4 --wire-format compact --batch-bytes 1472
```

The `packet_xor4` option requires nonzero packing and compact wire format;
both binaries reject an invalid combination.

## Reproducible controlled-loss comparison

The Linux/WSL runner creates private network namespaces and applies `tc netem`
only to its private virtual link. It is appropriate for correctness and
comparative loss experiments; it is not a substitute for a two-host AWS/ENA
latency result.

```bash
# Requires passwordless sudo for the narrowly scoped ip netns commands.
harness/bench/run_udp_matrix.sh --profile recovery --count 1000000 \
  --warmup-ms 500 --repeats 3 --cpus 0,2,4,6 \
  --output /tmp/udp-recovery-matrix --execute
```

The recovery matrix compares the three profiles at 50k messages/s under
independent loss and the Gilbert-Elliott burst-loss model. It uses a new netem
seed for every repetition and rotates scenario order. `matrix.tsv` joins
percentiles, measured delivery, and FEC recovery; each run directory contains
the command manifest, component logs, and `qdisc.txt`.

For a tail-focused zero-loss sweep, run at least one million measured messages
per rate and keep the host otherwise idle:

```bash
harness/bench/run_udp_netem.sh --loss 0 --fec xor4 --wire-format full \
  --udp-mode connected --batch-bytes 1472 --reorder-wait-us auto \
  --count 1000000 --warmup-ms 500 --rate 100000 --slots 65536 \
  --socket-drop-counters --output /tmp/udp-tail-xor4-100k
```

Repeat this command at each candidate message rate and fan-out. Do not
conclude from `p99.99` with only a short sample: one million observations
contains about 100 observations beyond that percentile.

## What to record for an organizer run

For every profile, rate, and receiver count, retain:

- exact Git commit and all command lines;
- instance type, Ubuntu version, `uname -a`, ENA driver, MTU, addresses, and
  CPU pinning/isolation;
- `p50`, `p99`, `p99.9`, and `p99.99` from the consumer;
- `consumer_received`, `measurement_missing_total`,
  `consumer_internal_seq_gaps`, `receiver_fec_recovered`, sender drops, and
  receiver socket-drop counters;
- `qdisc.txt` for injected-loss tests. Its dropped datagrams are link loss,
  not residual application message gaps;
- raw consumer CSV and each run's immutable manifest/log directory in the
  environment where the measurement is performed.

On a two-host test, record clock treatment before interpreting absolute
one-way timestamps. First establish delivery and local transport counters,
then compare end-to-end timestamps only with documented clock synchronization.

## Local evidence and limitation

On the development WSL laptop, three controlled 50k messages/s recovery runs
with 100,000 measured messages per run found the following mean residual
message gaps. The comparison includes an unpacked XOR control because packing
can put several frame-level shards in the same UDP datagram:

| Loss model | xor4 packed | xor4 unpacked | rs4_2 packed | packet_xor4 compact |
| --- | ---: | ---: | ---: | ---: |
| Independent 0.1% loss | 21.3 | 16.3 | 12.7 | 1.0 |
| Independent 0.5% loss | 108.3 | 96.0 | 91.3 | 15.0 |
| Gilbert-Elliott burst model | 125.0 | 115.7 | 74.3 | 134.7 |

This supports the conditional selection above: packet-level FEC was strongest
for the tested scattered loss, while Reed-Solomon was strongest for the tested
burst model. Three zero-loss, 1,000,000-message runs at 100k messages/s gave
packed XOR p99.99 of 1.40–1.52 ms; the unpacked control ranged from 1.36 ms to
33.38 ms. Compact per-run summaries for these results, plus zero-loss and
lossy three-receiver fan-out checks, are versioned under `results/` and
analysed in `analysis.ipynb`. Full raw logs remain measurement artifacts rather
than public source files.

It does **not** establish AWS latency or a universal loss model. WSL scheduling
produced millisecond-scale outliers even when median local latency was a few
microseconds; those absolute tail numbers are excluded from the deployment
claim. The final comparison must run on the organizers' Ubuntu AWS instances
and use the recorded methodology above.

## Verification before submission

```bash
make -C harness test
make -C harness test-integration
make -C harness test-fanout-integration
make -C harness test-fec-integration
```

The checked-in `results/*.csv` summaries are the inputs for the notebook. To
render it in a fresh Python environment, install its explicit dependencies and
open the notebook from the repository root:

```bash
python3 -m pip install -r requirements.txt
jupyter lab analysis.ipynb
```

The organizer-facing requirements are in
`how_to_submit_solution_en.md`.
