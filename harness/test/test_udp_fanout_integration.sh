#!/usr/bin/env bash
# Loopback test for sender-side unicast fan-out to two independent receivers.
set -euo pipefail
cd "$(dirname "$0")/.."

suffix="$$"
fanout_mode=${FANOUT_MODE:-loop}
udp_mode=${UDP_MODE:-sendto}
sender_workers=${SENDER_WORKERS:-1}
fec_mode=${FEC_MODE:-none}
case "$fanout_mode" in loop|sendmmsg) ;; *) echo 'invalid FANOUT_MODE' >&2; exit 2 ;; esac
case "$udp_mode" in sendto|connected) ;; *) echo 'invalid UDP_MODE' >&2; exit 2 ;; esac
case "$fec_mode" in none|xor4|rs4_2) ;; *) echo 'invalid FEC_MODE' >&2; exit 2 ;; esac
[[ $sender_workers == auto || $sender_workers =~ ^[1-9][0-9]*$ ]] || { echo 'invalid SENDER_WORKERS' >&2; exit 2; }
in_shm="/udp_fanout_in_${suffix}"
out_shm_a="/udp_fanout_a_${suffix}"
out_shm_b="/udp_fanout_b_${suffix}"
port_a=$((20000 + ($$ % 10000)))
port_b=$((port_a + 1))
tmp="/tmp/udp_fanout_${suffix}"
mkdir -p "$tmp"

cleanup() {
  status=$?
  jobs -pr | xargs -r kill 2>/dev/null || true
  if ((status != 0)); then
    for log in "$tmp"/*.log; do [[ -f "$log" ]] && { echo "===== $log =====" >&2; cat "$log" >&2; }; done
  fi
  rm -f "/dev/shm/${in_shm#/}" "/dev/shm/${out_shm_a#/}" "/dev/shm/${out_shm_b#/}"
  rm -rf -- "$tmp"
}
trap cleanup EXIT

./bin/udp_receiver --out-shm "$out_shm_a" --out-slots 1024 --port "$port_a" --count 100 --idle-ms 3000 --fec "$fec_mode" >"$tmp/receiver_a.log" 2>&1 &
receiver_a=$!
./bin/udp_receiver --out-shm "$out_shm_b" --out-slots 1024 --port "$port_b" --count 100 --idle-ms 3000 --fec "$fec_mode" >"$tmp/receiver_b.log" 2>&1 &
receiver_b=$!
for _ in {1..300}; do
  grep -q 'udp_receiver: ready' "$tmp/receiver_a.log" 2>/dev/null &&
    grep -q 'udp_receiver: ready' "$tmp/receiver_b.log" 2>/dev/null && break
  sleep 0.01
done
grep -q 'udp_receiver: ready' "$tmp/receiver_a.log"
grep -q 'udp_receiver: ready' "$tmp/receiver_b.log"

for suffix_out in a b; do
  out_shm_var="out_shm_${suffix_out}"
  ./bin/consumer --shm "${!out_shm_var}" --slots 1024 --from-edge --count 100 --idle-ms 3000 >"$tmp/consumer_${suffix_out}.log" 2>&1 &
done
./bin/udp_sender --in-shm "$in_shm" --in-slots 1024 --target "127.0.0.1:$port_a" \
  --target "127.0.0.1:$port_b" --udp-mode "$udp_mode" --fanout-mode "$fanout_mode" --sender-workers "$sender_workers" --fec "$fec_mode" --count 100 --wait-ms 3000 --idle-ms 3000 >"$tmp/sender.log" 2>&1 &
sender=$!
# Multi-process fan-out workers attach independently. Keep the creator alive
# briefly before the short test stream starts so every worker observes the
# input ring rather than racing producer teardown on a loaded host.
./bin/producer --shm "$in_shm" --slots 1024 --count 100 --rate 10000 --ready-delay-ms 100 --type mixed >"$tmp/producer.log" 2>&1 &
producer=$!

wait "$producer"
wait "$sender"
wait "$receiver_a"
wait "$receiver_b"
wait
for suffix_out in a b; do
  grep -q 'received     : 100' "$tmp/consumer_${suffix_out}.log"
  grep -q 'expected     : 100' "$tmp/consumer_${suffix_out}.log"
  grep -q 'dropped      : 0' "$tmp/consumer_${suffix_out}.log"
done
if [[ $sender_workers == 1 ]]; then
  grep -Eq "fec=$fec_mode wire_format=full udp_mode=$udp_mode fanout_mode=$fanout_mode destinations=2 frames_read=100 sent=100 .*first_seq=1 last_seq=100 .*input_lapped=0 invalid=0 socket_drops=0 destination_drops=0,0,0" "$tmp/sender.log"
  if [[ $fec_mode == none ]]; then
    grep -Eq 'parity_sent=0' "$tmp/sender.log"
  else
    grep -Eq 'parity_sent=[1-9][0-9]*' "$tmp/sender.log"
  fi
else
  grep -Eq "udp_sender: worker_mode=$sender_workers workers=2 .* status=pass" "$tmp/sender.log"
fi
if ./bin/udp_sender --target "127.0.0.1:$port_a" --target "127.0.0.1:$port_a" >"$tmp/duplicate.log" 2>&1; then
  echo 'duplicate --target unexpectedly accepted' >&2
  exit 1
fi
grep -q 'duplicate --target' "$tmp/duplicate.log"
echo 'test_udp_fanout_integration OK'
