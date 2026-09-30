# Low-Latency UDP Data Transfer

A C++20 experimental transport for a producer-to-many-consumers market-data
pipeline. It was built for the [Spectral Technologies Low-Latency Data Transfer
Challenge](https://spectral.tech/).

The project focuses on the trade-off between end-to-end tail latency and
ordered delivery when UDP links lose packets. It retains the provided
shared-memory producer/consumer model and implements the mutable transport
between them.

```text
producer -> input shared-memory ring -> udp_sender -> UDP unicast
         -> udp_receiver -> output shared-memory ring -> consumer
```

## What is included

- UDP sender and receiver with one-to-many unicast fan-out;
- packet framing that preserves `seq_id` and `send_ts_ns` for measurement;
- bounded resequencing: consumers only observe a strictly increasing sequence;
- three forward-error-correction (FEC) profiles:
  - XOR(4+1) for low overhead,
  - Reed-Solomon(4+2) for two losses per FEC group,
  - packet-level XOR for scattered whole-datagram loss;
- controlled packet-loss benchmarks using Linux network namespaces and `tc netem`;
- unit/integration tests, compact benchmark summaries, and an executed
  analysis notebook.

## Quick start

```bash
make -C harness
make -C harness test
make -C harness test-integration
make -C harness test-fanout-integration
make -C harness test-fec-integration
```

The design, profiles, exact benchmark commands, results, and limitations are
documented in [SOLUTION.md](SOLUTION.md). The executed analysis is in
[analysis.ipynb](analysis.ipynb); its dependencies are listed in
[requirements.txt](requirements.txt).

## Results and limitations

The repository contains comparative experiments for loss recovery and up to
three unicast receivers. They were run on a development laptop under WSL using
isolated virtual links. They establish relative behavior and correctness, not
absolute real-network latency. In particular, WSL scheduling introduced
millisecond-scale outliers, so those tail figures must not be generalized to
bare-metal or cloud NICs.

The implementation is therefore a useful experimental baseline rather than a
claim of a production-ready kernel-bypass transport. A real deployment should
be benchmarked on its target NICs and links; DPDK or AF_XDP are natural
next-step candidates where platform and operational constraints allow them.

## Acknowledgements

Built for the [Spectral Technologies](https://spectral.tech/) Low-Latency Data
Transfer Challenge. The challenge materials and evaluation framework were
provided by Spectral Technologies.
