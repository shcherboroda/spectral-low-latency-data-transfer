#!/usr/bin/env bash
# End-to-end loopback smoke test.  It intentionally starts the receiver and
# fixed consumer before the producer so producer/consumer semantics stay intact.
set -euo pipefail
cd "$(dirname "$0")/.."

suffix="$$"
wire_format=${WIRE_FORMAT:-full}
fec_mode=${FEC_MODE:-none}
count=${COUNT:-100}
batch_bytes=${BATCH_BYTES:-0}
rate=${RATE:-10000}
udp_mode=${UDP_MODE:-sendto}
case "$wire_format" in full|compact) ;; *) echo 'invalid WIRE_FORMAT' >&2; exit 2 ;; esac
case "$fec_mode" in none|xor4|xor8|rs4_2|rs8_2|packet_xor4) ;; *) echo 'invalid FEC_MODE' >&2; exit 2 ;; esac
case "$udp_mode" in sendto|connected) ;; *) echo 'invalid UDP_MODE' >&2; exit 2 ;; esac
[[ "$count" =~ ^[1-9][0-9]*$ ]] || { echo 'invalid COUNT' >&2; exit 2; }
[[ "$batch_bytes" =~ ^[0-9]+$ && ( "$batch_bytes" == 0 || ( "$batch_bytes" -ge 632 && "$batch_bytes" -le 1472 ) ) ]] || { echo 'invalid BATCH_BYTES' >&2; exit 2; }
[[ "$rate" =~ ^[0-9]+$ ]] || { echo 'invalid RATE' >&2; exit 2; }
in_shm="/udp_in_${suffix}"
out_shm="/udp_out_${suffix}"
port=$((20000 + ($$ % 10000)))
receiver_log="/tmp/udp_receiver_${suffix}.out"
consumer_log="/tmp/udp_consumer_${suffix}.out"
producer_log="/tmp/udp_producer_${suffix}.out"
sender_log="/tmp/udp_sender_${suffix}.out"
consumer_csv="/tmp/udp_consumer_${suffix}.csv"
cleanup() {
  status=$?
  jobs -pr | xargs -r kill 2>/dev/null || true
  if ((status != 0)); then
    for log in "$receiver_log" "$consumer_log" "$producer_log" "$sender_log"; do
      if [[ -f "$log" ]]; then
        echo "===== $log =====" >&2
        cat "$log" >&2
      fi
    done
  fi
  rm -f "/dev/shm/${in_shm#/}" "/dev/shm/${out_shm#/}"
  rm -f "$receiver_log" "$consumer_log" "$producer_log" "$sender_log" "$consumer_csv"
}
trap cleanup EXIT

./bin/udp_receiver --out-shm "$out_shm" --out-slots 1024 --port "$port" --count "$count" --idle-ms 3000 --fec "$fec_mode" --wire-format "$wire_format" --batch-bytes "$batch_bytes" >"$receiver_log" 2>&1 &
receiver=$!
for _ in {1..300}; do
  grep -q "udp_receiver: ready" "$receiver_log" 2>/dev/null && break
  sleep 0.01
done
grep -q "udp_receiver: ready" "$receiver_log"
./bin/consumer --shm "$out_shm" --slots 1024 --from-edge --count "$count" \
  --idle-ms 3000 --csv "$consumer_csv" >"$consumer_log" 2>&1 &
consumer=$!
for _ in {1..300}; do
  grep -q "/dev/shm/${out_shm#/}" "/proc/$consumer/maps" 2>/dev/null && break
  sleep 0.01
done
grep -q "/dev/shm/${out_shm#/}" "/proc/$consumer/maps"
./bin/udp_sender --in-shm "$in_shm" --in-slots 1024 --host 127.0.0.1 \
  --port "$port" --count "$count" --wait-ms 3000 --idle-ms 3000 --fec "$fec_mode" --wire-format "$wire_format" --udp-mode "$udp_mode" --batch-bytes "$batch_bytes" >"$sender_log" 2>&1 &
sender=$!
./bin/producer --shm "$in_shm" --slots 1024 --count "$count" --rate "$rate" \
  --type mixed >"$producer_log" 2>&1 &
producer=$!
wait "$producer"
wait "$sender"
wait "$receiver"
wait "$consumer"
grep -q "received     : $count" "$consumer_log"
grep -q "expected     : $count" "$consumer_log"
grep -q "dropped      : 0" "$consumer_log"
grep -q "consumer: lapped 0 times" "$consumer_log"
grep -Eq "fec=$fec_mode wire_format=$wire_format udp_mode=$udp_mode fanout_mode=loop destinations=1 frames_read=$count sent=$count data_datagrams=[0-9]+ parity_sent=" "$sender_log"
grep -Eq "fec=$fec_mode wire_format=$wire_format recv_batch=[0-9]+ accepted=$count first_seq=1 last_seq=$count apparent_missing=0 invalid=0 duplicates=0 too_old=0 session_mismatch=0" "$receiver_log"
if [[ "$fec_mode" == none ]]; then
  grep -q 'parity_sent=0' "$sender_log"
else
  grep -Eq 'parity_sent=[1-9][0-9]*' "$sender_log"
  grep -Eq 'fec_parity=[1-9][0-9]*' "$receiver_log"
fi
if [[ "$batch_bytes" != 0 ]]; then
  datagrams=$(sed -n 's/.*data_datagrams=\([0-9][0-9]*\).*/\1/p' "$sender_log" | tail -n 1)
  [[ "$datagrams" =~ ^[0-9]+$ && "$datagrams" -le "$count" ]] || {
    echo 'batching emitted more data datagrams than messages' >&2; exit 1;
  }
fi
awk -F, 'NR == 1 { if ($1 != "seq" || $2 != "latency_ns") exit 1 }
           NR == 2 { first = $1; previous = $1; next }
           NR > 2 { if ($1 != previous + 1) exit 1; previous = $1 }
           END { if (NR != expected + 1 || first != 1 || previous != expected) exit 1 }' expected="$count" "$consumer_csv"
echo "test_udp_integration OK"
