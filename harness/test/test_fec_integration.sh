#!/usr/bin/env bash
# Root-capable integration tests for the sender/receiver XOR FEC CLI path.
set -euo pipefail
cd "$(dirname "$0")/.."

root_dir=$(mktemp -d /tmp/spectral-fec-integration.XXXXXX)
cleanup() { rm -rf -- "$root_dir"; }
trap cleanup EXIT

run=./bench/run_udp_netem.sh
common=(--fec xor8 --udp-mode connected --netem-seed 424242 --rate 50000 --slots 65536
        --cpu-producer 0 --cpu-sender 2 --cpu-receiver 4 --cpu-consumer 6)

# 17 mixed frames leave incomplete short and order-book groups; sender must
# flush parity for both so this validates the partial-group CLI path.
"$run" "${common[@]}" --loss 0 --count 17 --output "$root_dir/partial"
grep -qx 'status=pass' "$root_dir/partial/result.txt"
grep -qx 'sender_sent=17' "$root_dir/partial/result.txt"
grep -qx 'network_missing_total=0' "$root_dir/partial/result.txt"
grep -Eq '^sender_parity_sent=[1-9][0-9]*$' "$root_dir/partial/result.txt"

# The fixed loss seed removes data packets. Packet scheduling can also affect
# which parity packets are lost, so full delivery is not an XOR(8,1)
# invariant; require that the live recovery path is exercised instead.
"$run" "${common[@]}" --loss 0.1 --count 100000 --output "$root_dir/recovery"
grep -qx 'status=pass' "$root_dir/recovery/result.txt"
grep -Eq '^receiver_accepted=[1-9][0-9]*$' "$root_dir/recovery/result.txt"
grep -Eq '^receiver_fec_recovered=[1-9][0-9]*$' "$root_dir/recovery/result.txt"

# RS must emit two parity datagrams per group, recover erasures, and flush
# incomplete type-specific groups before sender exit.
rs_common=(--fec rs8_2 --udp-mode connected --netem-seed 424242 --rate 50000 --slots 65536
           --cpu-producer 0 --cpu-sender 2 --cpu-receiver 4 --cpu-consumer 6)
"$run" "${rs_common[@]}" --loss 0 --count 17 --output "$root_dir/rs-partial"
grep -qx 'status=pass' "$root_dir/rs-partial/result.txt"
grep -qx 'sender_sent=17' "$root_dir/rs-partial/result.txt"
grep -qx 'network_missing_total=0' "$root_dir/rs-partial/result.txt"
grep -Eq '^sender_parity_sent=[1-9][0-9]*$' "$root_dir/rs-partial/result.txt"
grep -q 'fec=rs8_2' "$root_dir/rs-partial/sender.log"
grep -q 'fec=rs8_2' "$root_dir/rs-partial/receiver.log"

"$run" "${rs_common[@]}" --loss 0.1 --count 100000 --output "$root_dir/rs-recovery"
grep -qx 'status=pass' "$root_dir/rs-recovery/result.txt"
# Independent loss can erase more than two shards (or parity) in a group, so
# 100% delivery is not a valid RS(8,2) invariant.  This fixture verifies the
# live recovery path without claiming it provides reliability beyond its code.
grep -Eq '^receiver_accepted=[1-9][0-9]*$' "$root_dir/rs-recovery/result.txt"
grep -Eq '^receiver_fec_recovered=[1-9][0-9]*$' "$root_dir/rs-recovery/result.txt"

# Packet-level XOR protects a complete compact UDP batch.  Its recovered group
# must be retired, otherwise repeated packet groups eventually exhaust the
# receiver's fixed group table.
packet_common=(--fec packet_xor4 --wire-format compact --udp-mode connected
               --netem-seed 424242 --rate 50000 --slots 65536
               --cpu-producer 0 --cpu-sender 2 --cpu-receiver 4 --cpu-consumer 6)
"$run" "${packet_common[@]}" --loss 0 --count 17 --output "$root_dir/packet-partial"
grep -qx 'status=pass' "$root_dir/packet-partial/result.txt"
grep -qx 'sender_sent=17' "$root_dir/packet-partial/result.txt"
grep -qx 'network_missing_total=0' "$root_dir/packet-partial/result.txt"
grep -Eq '^sender_parity_sent=[1-9][0-9]*$' "$root_dir/packet-partial/result.txt"

"$run" "${packet_common[@]}" --loss 0.1 --count 100000 --output "$root_dir/packet-recovery"
grep -qx 'status=pass' "$root_dir/packet-recovery/result.txt"
grep -Eq '^receiver_fec_recovered=[1-9][0-9]*$' "$root_dir/packet-recovery/result.txt"

echo "test_fec_integration OK"
