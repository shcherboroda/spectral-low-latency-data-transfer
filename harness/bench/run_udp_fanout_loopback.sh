#!/usr/bin/env bash
# Controlled one-host fan-out benchmark. It measures replication and receiver
# cost without pretending that loopback is an AWS/ENA network result.
set -Eeuo pipefail
IFS=$'\n\t'

usage() {
  cat <<'EOF'
Usage: harness/bench/run_udp_fanout_loopback.sh [options]
  --receivers N       1, 2, or 3 (default: 1)
  --fec MODE          none|xor4|xor8|rs4_2|rs8_2 (default: none)
  --fanout-mode MODE  loop|sendmmsg (default: loop)
  --udp-mode MODE     sendto|connected sender socket mode (default: sendto)
  --sender-workers N  1, a worker count, or auto (default: 1)
  --recv-batch N      Receiver recvmmsg batch, 1 through 16 (default: 1)
  --batch-bytes N     Pack full frames into UDP datagrams up to N; 0 disables (default: 1472)
  --reorder-wait-us N Maximum wait for a missing sequence number, or auto (default: auto)
  --profile-sample-every N  Sample every Nth sender/receiver operation; 0 disables (default: 0)
  --count N           Producer messages (default: 100000)
  --rate R            Producer messages/sec (default: 50000)
  --type T            trade|bbo|book|mixed (default: mixed)
  --slots N           Power-of-two SHM slots (default: 65536)
  --idle-ms N         Process idle timeout (default: 5000)
  --output DIR        New result directory (default: harness/results/ unique)
  --help              Show this help

This is a local correctness/replication benchmark. It uses loopback, never
injects loss, and must not be reported as an ENA network-latency result.
EOF
}
die() { printf 'fanout benchmark: %s\n' "$*" >&2; exit 2; }

receivers=1
fec=none
fanout_mode=loop
udp_mode=sendto
sender_workers=1
recv_batch=1
batch_bytes=1472
reorder_wait_us=auto
profile_sample_every=0
count=100000
rate=50000
kind=mixed
slots=65536
idle_ms=5000
output=
while (($#)); do
  case "$1" in
    --receivers) receivers=${2-}; shift 2 ;;
    --fec) fec=${2-}; shift 2 ;;
    --fanout-mode) fanout_mode=${2-}; shift 2 ;;
    --udp-mode) udp_mode=${2-}; shift 2 ;;
    --sender-workers) sender_workers=${2-}; shift 2 ;;
    --recv-batch) recv_batch=${2-}; shift 2 ;;
    --batch-bytes) batch_bytes=${2-}; shift 2 ;;
    --reorder-wait-us) reorder_wait_us=${2-}; shift 2 ;;
    --profile-sample-every) profile_sample_every=${2-}; shift 2 ;;
    --count) count=${2-}; shift 2 ;;
    --rate) rate=${2-}; shift 2 ;;
    --type) kind=${2-}; shift 2 ;;
    --slots) slots=${2-}; shift 2 ;;
    --idle-ms) idle_ms=${2-}; shift 2 ;;
    --output) output=${2-}; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done
[[ $receivers =~ ^[1-3]$ ]] || die '--receivers must be 1, 2, or 3'
[[ $count =~ ^[1-9][0-9]*$ && $rate =~ ^[0-9]+$ && $idle_ms =~ ^[0-9]+$ ]] || die 'invalid count, rate, or idle-ms'
[[ $batch_bytes =~ ^[0-9]+$ ]] && ((batch_bytes == 0 || (batch_bytes >= 632 && batch_bytes <= 1472))) || die '--batch-bytes must be 0 or from 632 through 1472'
[[ $reorder_wait_us == auto || $reorder_wait_us =~ ^[0-9]+$ ]] || die '--reorder-wait-us must be a non-negative integer or auto'
[[ $profile_sample_every =~ ^[0-9]+$ ]] || die '--profile-sample-every must be a non-negative integer'
[[ $slots =~ ^[0-9]+$ ]] && ((slots > 0 && (slots & (slots - 1)) == 0)) || die '--slots must be a power of two'
case "$fec" in none|xor4|xor8|rs4_2|rs8_2) ;; *) die '--fec must be none, xor4, xor8, rs4_2, or rs8_2' ;; esac
case "$fanout_mode" in loop|sendmmsg) ;; *) die '--fanout-mode must be loop or sendmmsg' ;; esac
case "$udp_mode" in sendto|connected) ;; *) die '--udp-mode must be sendto or connected' ;; esac
[[ $sender_workers == auto || $sender_workers =~ ^[1-9][0-9]*$ ]] || die '--sender-workers must be a positive integer or auto'
[[ $recv_batch =~ ^[1-9][0-9]*$ ]] && ((recv_batch <= 16)) || die '--recv-batch must be from 1 through 16'
case "$kind" in trade|bbo|book|mixed) ;; *) die '--type must be trade|bbo|book|mixed' ;; esac

repo_root=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
harness=$repo_root/harness
for binary in producer consumer udp_sender udp_receiver; do [[ -x $harness/bin/$binary ]] || die "missing $binary; run make -C harness"; done
if [[ -n $output ]]; then
  [[ ! -e $output ]] || die "--output already exists: $output"
  mkdir -p -- "$output"
else
  mkdir -p -- "$harness/results"
  output=$(mktemp -d "$harness/results/udp-fanout-loopback.XXXXXX")
fi
output=$(CDPATH= cd -- "$output" && pwd)
tag="$(date -u +%Y%m%dT%H%M%SZ)-$$"
in_shm="/sc_fanout_in_$tag"
base_port=$((25000 + ($$ % 10000)))
declare -a receiver_pid consumer_pid out_shm port target created_out_shm
producer_pid= sender_pid= watchdog_pid=
created_in_shm=0

cleanup() {
  rc=$?
  set +e
  [[ -n $watchdog_pid ]] && kill "$watchdog_pid" 2>/dev/null
  for pid in "${producer_pid:-}" "${sender_pid:-}" "${receiver_pid[@]:-}" "${consumer_pid[@]:-}"; do
    [[ -n $pid ]] && kill "$pid" 2>/dev/null
  done
  for pid in "${producer_pid:-}" "${sender_pid:-}" "${receiver_pid[@]:-}" "${consumer_pid[@]:-}"; do
    [[ -n $pid ]] && wait "$pid" 2>/dev/null
  done
  ((created_in_shm)) && rm -f -- "/dev/shm/${in_shm#/}"
  for ((i = 0; i < receivers; ++i)); do
    [[ ${created_out_shm[i]:-0} == 1 ]] && rm -f -- "/dev/shm/${out_shm[i]#/}"
  done
  exit "$rc"
}
trap cleanup EXIT INT TERM

{
  printf 'git_commit=%s\n' "$(git -C "$repo_root" rev-parse HEAD)"
  printf 'kernel=%q\nreceivers=%s\nfec=%q\nudp_mode=%q\nfanout_mode=%q\nsender_workers=%q\nrecv_batch=%s\nbatch_bytes=%s\nreorder_wait_us=%s\nprofile_sample_every=%s\ncount=%s\nrate=%s\ntype=%q\nslots=%s\nidle_ms=%s\n' "$(uname -a)" "$receivers" "$fec" "$udp_mode" "$fanout_mode" "$sender_workers" "$recv_batch" "$batch_bytes" "$reorder_wait_us" "$profile_sample_every" "$count" "$rate" "$kind" "$slots" "$idle_ms"
} >"$output/manifest.txt"

reserve_shm() {
  local path=$1
  (set -o noclobber; : >"$path") 2>/dev/null ||
    die "refusing to reuse existing SHM object: $path"
}
wait_for_mapping() {
  local pid=$1 shm_name=$2 timeout_ms=$3 label=$4
  local path="/dev/shm/${shm_name#/}" deadline=$((SECONDS * 1000 + timeout_ms))
  while ((SECONDS * 1000 < deadline)); do
    kill -0 "$pid" 2>/dev/null || die "$label exited before mapping $shm_name"
    grep -Fq -- "$path" "/proc/$pid/maps" 2>/dev/null && return 0
    sleep 0.01
  done
  die "timed out waiting for $label to map $shm_name"
}

reserve_shm "/dev/shm/${in_shm#/}"
created_in_shm=1

for ((i = 0; i < receivers; ++i)); do
  out_shm[i]="/sc_fanout_out_${tag}_$i"
  reserve_shm "/dev/shm/${out_shm[i]#/}"
  created_out_shm[i]=1
  port[i]=$((base_port + i))
  target[i]="127.0.0.1:${port[i]}"
  printf 'target_%s=%q\n' "$((i + 1))" "${target[i]}" >>"$output/manifest.txt"
  mkdir -p "$output/rx$((i + 1))"
  "$harness/bin/udp_receiver" --out-shm "${out_shm[i]}" --out-slots "$slots" --port "${port[i]}" --count "$count" --idle-ms "$idle_ms" --fec "$fec" --batch-bytes "$batch_bytes" --reorder-wait-us "$reorder_wait_us" --recv-batch "$recv_batch" --profile-sample-every "$profile_sample_every" >"$output/rx$((i + 1))/receiver.log" 2>&1 &
  receiver_pid[i]=$!
done
for _ in {1..500}; do
  ready=0
  for ((i = 0; i < receivers; ++i)); do grep -q 'udp_receiver: ready' "$output/rx$((i + 1))/receiver.log" 2>/dev/null && ((++ready)); done
  ((ready == receivers)) && break
  sleep 0.01
done
for ((i = 0; i < receivers; ++i)); do grep -q 'udp_receiver: ready' "$output/rx$((i + 1))/receiver.log" || die 'receiver startup timeout'; done
for ((i = 0; i < receivers; ++i)); do
  "$harness/bin/consumer" --shm "${out_shm[i]}" --slots "$slots" --from-edge --count "$count" --idle-ms "$idle_ms" --profile-sample-every "$profile_sample_every" --csv "$output/rx$((i + 1))/consumer.csv" >"$output/rx$((i + 1))/consumer.log" 2>&1 &
  consumer_pid[i]=$!
  wait_for_mapping "${consumer_pid[i]}" "${out_shm[i]}" "$idle_ms" "consumer $((i + 1))"
done
sender_args=(--in-shm "$in_shm" --in-slots "$slots" --count "$count" --wait-ms "$idle_ms" --idle-ms "$idle_ms" --fec "$fec" --batch-bytes "$batch_bytes" --profile-sample-every "$profile_sample_every" --udp-mode "$udp_mode" --fanout-mode "$fanout_mode" --sender-workers "$sender_workers")
for ((i = 0; i < receivers; ++i)); do sender_args+=(--target "${target[i]}"); done
"$harness/bin/udp_sender" "${sender_args[@]}" >"$output/sender.log" 2>&1 & sender_pid=$!
"$harness/bin/producer" --shm "$in_shm" --slots "$slots" --count "$count" --rate "$rate" --type "$kind" >"$output/producer.log" 2>&1 & producer_pid=$!

# A bounded watchdog prevents an unattended stalled run from surviving forever.
effective_rate=$rate
((effective_rate == 0)) && effective_rate=100000
timeout_s=$((idle_ms / 1000 + count / effective_rate + 30))
((timeout_s < 60)) && timeout_s=60
( sleep "$timeout_s"; kill "$producer_pid" "$sender_pid" "${receiver_pid[@]}" "${consumer_pid[@]}" 2>/dev/null || true ) & watchdog_pid=$!
wait "$producer_pid"; wait "$sender_pid"
for pid in "${receiver_pid[@]}"; do wait "$pid"; done
for pid in "${consumer_pid[@]}"; do wait "$pid"; done
kill "$watchdog_pid" 2>/dev/null || true; watchdog_pid=

status=pass
if [[ $sender_workers == 1 ]]; then
  grep -q "frames_read=$count sent=$count" "$output/sender.log" || status=fail
else
  grep -q "udp_sender: worker_mode=$sender_workers " "$output/sender.log" || status=fail
fi
for ((i = 0; i < receivers; ++i)); do
  log="$output/rx$((i + 1))/consumer.log"
  grep -q "received     : $count" "$log" && grep -q 'dropped      : 0' "$log" || status=fail
done
{
  printf 'status=%s\n' "$status"
  sed -n 's/^udp_sender: /sender_/p' "$output/sender.log"
  for ((i = 0; i < receivers; ++i)); do
    printf 'receiver_%s_' "$((i + 1))"; sed -n 's/^udp_receiver: //p' "$output/rx$((i + 1))/receiver.log" | tail -n 1
    awk -v prefix="receiver_$((i + 1))" '
      /^  p50        : / { print prefix "_p50_ns=" $3 }
      /^  p99        : / { print prefix "_p99_ns=" $3 }
      /^  p99.9      : / { print prefix "_p999_ns=" $3 }
      /^  p99.99     : / { print prefix "_p9999_ns=" $3 }
    ' "$output/rx$((i + 1))/consumer.log"
  done
} >"$output/result.txt"
printf 'results=%s\n' "$output"
[[ $status == pass ]] || die "run failed; inspect $output"
