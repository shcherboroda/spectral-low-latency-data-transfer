#!/usr/bin/env bash
# Run the raw-UDP spine through a private veth pair with loss injected only on
# the sender-side veth.  This script deliberately never changes a pre-existing
# interface, qdisc, namespace, process, or POSIX SHM object.
set -Eeuo pipefail
IFS=$'\n\t'

usage() {
  cat <<'EOF'
Usage: harness/bench/run_udp_netem.sh [options]

Runs: producer -> input SHM -> udp_sender (tx netns) -> veth/netem ->
      udp_receiver (rx netns) -> output SHM -> consumer.

Options:
  --loss PCT              UDP loss injected at tx-veth egress (default: 0)
  --loss-model MODE       random|gemodel (default: random)
  --loss-ge-r PCT         Gilbert-Elliott bad-to-good transition probability
  --loss-ge-bad PCT       Gilbert-Elliott loss probability in bad state
  --loss-ge-good PCT      Gilbert-Elliott loss probability in good state
  --delay-us N            One-way delay added on the private virtual link (default: 0)
  --delay-jitter-us N     Random variation around --delay-us (default: 0)
  --reorder PCT           Probability of packet reordering; requires delay (default: 0)
  --fec MODE              none|xor4|xor8|rs4_2|rs8_2|packet_xor4 recovery mode (default: none)
  --wire-format MODE      full|compact payload on UDP data path (default: full)
  --udp-mode MODE         sendto|connected sender socket mode (default: sendto)
  --recv-batch N          Receiver recvmmsg batch, 1 through 16 (default: 1)
  --socket-drop-counters  Record receiver kernel UDP queue overflows
  --reorder-wait-us N     Maximum wait for a missing sequence number, or auto (default: auto)
  --batch-bytes N         Pack ready raw frames into UDP datagrams up to N bytes;
                          0 disables packing (default: 1472, maximum: 1472)
  --profile-sample-every N  Sample every Nth data operation for stage timing;
                          0 disables diagnostic timing (default: 0)
  --netem-seed N          Fixed netem PRNG seed for comparable loss runs
  --count N               Number of producer messages (default: 100000)
  --rate MSGS_PER_SEC     Producer rate; 0 means unrestricted (default: 200000)
  --warmup-ms MS          Discard this much initial stream time before measuring
                          (default: 0; use 500 or more for comparative runs)
  --producer-ready-delay-ms MS  Keep producer SHM alive before its first frame
                          (default: 10; startup only, outside measured latency)
  --type TYPE             trade|bbo|book|mixed (default: mixed)
  --slots N               Power-of-two SHM slots on each edge (default: 65536)
  --idle-ms MS            Sender/receiver/consumer idle stop interval (default: 3000)
  --cpu-producer CPUSET   taskset CPU set for producer (optional)
  --cpu-sender CPUSET     taskset CPU set for sender (optional)
  --cpu-receiver CPUSET   taskset CPU set for receiver (optional)
  --cpu-consumer CPUSET   taskset CPU set for consumer (optional)
  --rt-priority N         SCHED_FIFO priority 1..99; 0 disables it (default: 0)
  --consumer-csv          Write consumer.csv (per-message diagnostic data)
  --output DIR            New directory for logs and results (default: unique
                          directory below harness/results)
  --startup-timeout-s N   Readiness deadline (default: 10)
  --run-timeout-s N       Whole-run deadline after producer starts (default: 60)
  --dry-run               Validate arguments and print the planned topology only
  --help                  Show this help

The run needs Linux iproute2 (ip, tc), taskset, and passwordless sudo
(sudo -n). It is intended for WSL/Linux, not Windows PowerShell directly.
EOF
}

die() { printf 'netem benchmark: %s\n' "$*" >&2; exit 2; }
note() { printf 'netem benchmark: %s\n' "$*" >&2; }

loss=0
loss_model=random
loss_ge_r=25
loss_ge_good=0
loss_ge_bad=50
delay_us=0
delay_jitter_us=0
reorder_pct=0
fec_mode=none
wire_format=full
udp_mode=sendto
recv_batch=1
socket_drop_counters=0
reorder_wait_us=auto
batch_bytes=1472
profile_sample_every=0
netem_seed=
count=100000
rate=200000
warmup_ms=0
producer_ready_delay_ms=10
kind=mixed
slots=65536
idle_ms=3000
cpu_producer=
cpu_sender=
cpu_receiver=
cpu_consumer=
rt_priority=0
consumer_csv=0
output_dir=
startup_timeout_s=10
run_timeout_s=60
dry_run=0

while (($#)); do
  case "$1" in
    --loss) loss=${2-}; shift 2 ;;
    --loss-model) loss_model=${2-}; shift 2 ;;
    --loss-ge-r) loss_ge_r=${2-}; shift 2 ;;
    --loss-ge-good) loss_ge_good=${2-}; shift 2 ;;
    --loss-ge-bad) loss_ge_bad=${2-}; shift 2 ;;
    --delay-us) delay_us=${2-}; shift 2 ;;
    --delay-jitter-us) delay_jitter_us=${2-}; shift 2 ;;
    --reorder) reorder_pct=${2-}; shift 2 ;;
    --fec) fec_mode=${2-}; shift 2 ;;
    --wire-format) wire_format=${2-}; shift 2 ;;
    --udp-mode) udp_mode=${2-}; shift 2 ;;
    --recv-batch) recv_batch=${2-}; shift 2 ;;
    --socket-drop-counters) socket_drop_counters=1; shift ;;
    --reorder-wait-us) reorder_wait_us=${2-}; shift 2 ;;
    --batch-bytes) batch_bytes=${2-}; shift 2 ;;
    --profile-sample-every) profile_sample_every=${2-}; shift 2 ;;
    --netem-seed) netem_seed=${2-}; shift 2 ;;
    --count) count=${2-}; shift 2 ;;
    --rate) rate=${2-}; shift 2 ;;
    --warmup-ms) warmup_ms=${2-}; shift 2 ;;
    --producer-ready-delay-ms) producer_ready_delay_ms=${2-}; shift 2 ;;
    --type) kind=${2-}; shift 2 ;;
    --slots) slots=${2-}; shift 2 ;;
    --idle-ms) idle_ms=${2-}; shift 2 ;;
    --cpu-producer) cpu_producer=${2-}; shift 2 ;;
    --cpu-sender) cpu_sender=${2-}; shift 2 ;;
    --cpu-receiver) cpu_receiver=${2-}; shift 2 ;;
    --cpu-consumer) cpu_consumer=${2-}; shift 2 ;;
    --rt-priority) rt_priority=${2-}; shift 2 ;;
    --consumer-csv) consumer_csv=1; shift ;;
    --output) output_dir=${2-}; shift 2 ;;
    --startup-timeout-s) startup_timeout_s=${2-}; shift 2 ;;
    --run-timeout-s) run_timeout_s=${2-}; shift 2 ;;
    --dry-run) dry_run=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done

is_uint() { [[ $1 =~ ^[0-9]+$ ]]; }
is_number() { [[ $1 =~ ^([0-9]+([.][0-9]*)?|[.][0-9]+)$ ]]; }
is_uint "$count" && ((count > 0)) || die "--count must be a positive integer"
is_number "$rate" || die "--rate must be a non-negative number"
is_uint "$warmup_ms" || die "--warmup-ms must be a non-negative integer"
is_uint "$producer_ready_delay_ms" || die "--producer-ready-delay-ms must be a non-negative integer"
if ((warmup_ms > 0)); then
  awk -v rate="$rate" 'BEGIN { exit !(rate > 0) }' || die "--warmup-ms requires --rate greater than zero"
fi
is_number "$loss" || die "--loss must be a number from 0 through 100"
awk -v v="$loss" 'BEGIN { exit !(v >= 0 && v <= 100) }' || die "--loss must be from 0 through 100"
case "$loss_model" in random|gemodel) ;; *) die "--loss-model must be random or gemodel" ;; esac
for ge_value in "$loss_ge_r" "$loss_ge_good" "$loss_ge_bad"; do
  is_number "$ge_value" || die "Gilbert-Elliott probabilities must be numbers from 0 through 100"
  awk -v v="$ge_value" 'BEGIN { exit !(v >= 0 && v <= 100) }' || die "Gilbert-Elliott probabilities must be from 0 through 100"
done
is_uint "$delay_us" || die "--delay-us must be a non-negative integer"
is_uint "$delay_jitter_us" || die "--delay-jitter-us must be a non-negative integer"
is_number "$reorder_pct" || die "--reorder must be a number from 0 through 100"
awk -v v="$reorder_pct" 'BEGIN { exit !(v >= 0 && v <= 100) }' || die "--reorder must be from 0 through 100"
((delay_jitter_us == 0 || delay_us > 0)) || die "--delay-jitter-us requires --delay-us greater than zero"
awk -v reorder="$reorder_pct" -v delay="$delay_us" 'BEGIN { exit !(reorder == 0 || delay > 0) }' || die "--reorder requires --delay-us greater than zero"
case "$fec_mode" in none|xor4|xor8|rs4_2|rs8_2|packet_xor4) ;; *) die "--fec must be none, xor4, xor8, rs4_2, rs8_2, or packet_xor4" ;; esac
case "$wire_format" in full|compact) ;; *) die "--wire-format must be full or compact" ;; esac
case "$udp_mode" in sendto|connected) ;; *) die "--udp-mode must be sendto or connected" ;; esac
is_uint "$recv_batch" && ((recv_batch >= 1 && recv_batch <= 16)) || die "--recv-batch must be from 1 through 16"
[[ $reorder_wait_us == auto ]] || is_uint "$reorder_wait_us" || die "--reorder-wait-us must be a non-negative integer or auto"
is_uint "$batch_bytes" && ((batch_bytes == 0 || (batch_bytes >= 632 && batch_bytes <= 1472))) || die "--batch-bytes must be 0 or from 632 through 1472"
is_uint "$profile_sample_every" || die "--profile-sample-every must be a non-negative integer"
[[ "$fec_mode" != packet_xor4 || ( "$wire_format" == compact && "$batch_bytes" != 0 ) ]] || die "packet_xor4 requires compact wire format and packet packing"
[[ -z "$netem_seed" ]] || { is_uint "$netem_seed" && ((netem_seed > 0)); } || die "--netem-seed must be a positive integer"
case "$kind" in trade|bbo|book|mixed) ;; *) die "--type must be trade|bbo|book|mixed" ;; esac
is_uint "$slots" && ((slots > 0 && (slots & (slots - 1)) == 0)) || die "--slots must be a power of two"
is_uint "$idle_ms" || die "--idle-ms must be a non-negative integer"
is_uint "$startup_timeout_s" && ((startup_timeout_s > 0)) || die "--startup-timeout-s must be positive"
is_uint "$run_timeout_s" && ((run_timeout_s > 0)) || die "--run-timeout-s must be positive"
is_uint "$rt_priority" && ((rt_priority <= 99)) || die "--rt-priority must be an integer from 0 through 99"
awk -v rate="$rate" -v idle_ms="$idle_ms" 'BEGIN {
  exit !(rate == 0 || rate * idle_ms >= 2000)
}' || die "--idle-ms must span at least two message intervals at the configured --rate"

repo_root=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
harness_dir="$repo_root/harness"
for tool in ip tc taskset runuser awk grep sed date mktemp sha256sum sleep uname git; do
  command -v "$tool" >/dev/null 2>&1 || die "required tool is missing: $tool"
done
if ((rt_priority > 0)); then
  for tool in chrt lscpu nproc ps sudo tr; do
    command -v "$tool" >/dev/null 2>&1 || die "--rt-priority requires: $tool"
  done
fi
for binary in producer consumer udp_sender udp_receiver; do
  [[ -x "$harness_dir/bin/$binary" ]] || die "missing executable: $harness_dir/bin/$binary (run: make -C harness)"
done

run_tag="$(date -u +%Y%m%dT%H%M%SZ)-$$"
tx_ns="sc-tx-$run_tag"
rx_ns="sc-rx-$run_tag"
# Interface names are constrained to IFNAMSIZ (15 visible characters).
short_tag=$(printf '%x' "$$")
tx_if="stx$short_tag"
rx_if="srx$short_tag"
in_shm="/sc_udp_in_$run_tag"
out_shm="/sc_udp_out_$run_tag"
port=$((20000 + ($$ % 20000)))
session_id=$((($(date +%s) << 20) + $$))
tx_ip=198.18.0.1
rx_ip=198.18.0.2
controller_cpu=
warmup_messages=$(awk -v rate="$rate" -v ms="$warmup_ms" 'BEGIN { printf "%d", int((rate * ms + 999) / 1000) }')
total_count=$((count + warmup_messages))

if ((dry_run)); then
  cat <<EOF
dry run: no privileged operation will be performed
  tx namespace: $tx_ns
  rx namespace: $rx_ns
  veth:         $tx_if ($tx_ip) -> $rx_if ($rx_ip)
  netem:        tx egress loss $loss%
  netem delay:  ${delay_us}us +/- ${delay_jitter_us}us
  netem reorder:${reorder_pct}%
  FEC:          $fec_mode
  Wire format:  $wire_format
  netem seed:   ${netem_seed:-kernel-random}
  input SHM:    $in_shm
  output SHM:   $out_shm
  UDP port:     $port
EOF
  exit 0
fi

sudo -n ip -Version >/dev/null 2>&1 || die "passwordless sudo permission for ip is required"
for existing in "$tx_ns" "$rx_ns"; do
  ! sudo -n ip netns list | awk '{print $1}' | grep -Fqx -- "$existing" || die "namespace already exists: $existing"
done
for existing in "$tx_if" "$rx_if"; do
  ! ip link show dev "$existing" >/dev/null 2>&1 || die "interface already exists: $existing"
done

if [[ -n "$output_dir" ]]; then
  [[ ! -e "$output_dir" ]] || die "--output must not already exist: $output_dir"
  mkdir -p -- "$output_dir"
else
  mkdir -p -- "$harness_dir/results"
  output_dir=$(mktemp -d "$harness_dir/results/udp-netem.XXXXXX")
fi
output_dir=$(CDPATH= cd -- "$output_dir" && pwd)
manifest="$output_dir/manifest.txt"
setup_log="$output_dir/setup.log"
producer_log="$output_dir/producer.log"
sender_log="$output_dir/sender.log"
receiver_log="$output_dir/receiver.log"
consumer_log="$output_dir/consumer.log"
qdisc_stats="$output_dir/qdisc.txt"
result_file="$output_dir/result.txt"

producer_pid=
sender_pid=
receiver_pid=
consumer_pid=
created_tx_ns=0
created_rx_ns=0
created_veth=0
created_in_shm=0
created_out_shm=0

cleanup() {
  local rc=$?
  set +e
  for pid in "$producer_pid" "$sender_pid" "$receiver_pid" "$consumer_pid"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill -TERM "$pid" 2>/dev/null
    fi
  done
  for pid in "$producer_pid" "$sender_pid" "$receiver_pid" "$consumer_pid"; do
    [[ -n "$pid" ]] && wait "$pid" 2>/dev/null
  done
  if ((created_tx_ns)); then sudo -n ip netns del "$tx_ns" >>"$setup_log" 2>&1; fi
  if ((created_rx_ns)); then sudo -n ip netns del "$rx_ns" >>"$setup_log" 2>&1; fi
  # Covers a failure between veth creation and moving both endpoints.
  if ((created_veth)) && ip link show dev "$tx_if" >/dev/null 2>&1; then
    sudo -n ip link del dev "$tx_if" >>"$setup_log" 2>&1
  fi
  ((created_in_shm)) && rm -f -- "/dev/shm/${in_shm#/}"
  ((created_out_shm)) && rm -f -- "/dev/shm/${out_shm#/}"
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

wait_for_log() {
  local file=$1 pattern=$2 timeout_s=$3 label=$4
  local deadline=$((SECONDS + timeout_s))
  while ((SECONDS < deadline)); do
    grep -Fq -- "$pattern" "$file" 2>/dev/null && return 0
    [[ -s "$file" ]] && grep -E '(failed|error|cannot|mismatch)' "$file" >/dev/null 2>&1 && {
      cat "$file" >&2; return 1;
    }
    sleep 0.01
  done
  note "timed out waiting for $label readiness; see $file"
  return 1
}

wait_for_mapping() {
  local pid=$1 shm_name=$2 timeout_s=$3 label=$4
  local shm_path="/dev/shm/${shm_name#/}" deadline=$((SECONDS + timeout_s))
  while ((SECONDS < deadline)); do
    kill -0 "$pid" 2>/dev/null || { note "$label exited before becoming ready"; return 1; }
    grep -Fq -- "$shm_path" "/proc/$pid/maps" 2>/dev/null && return 0
    sleep 0.01
  done
  note "timed out waiting for $label to map $shm_name"
  return 1
}

wait_for_exit() {
  local pid=$1 label=$2 deadline=$3
  while kill -0 "$pid" 2>/dev/null; do
    # A completed child remains visible to kill(0) until its parent reaps it.
    # Treat that zombie state as exited so a short loss run cannot consume the
    # whole deadline before the subsequent wait() reaps it.
    local state
    state=$(awk '{ print $3 }' "/proc/$pid/stat" 2>/dev/null || true)
    [[ "$state" == Z ]] && break
    ((SECONDS < deadline)) || { note "$label exceeded run deadline"; return 1; }
    sleep 0.05
  done
  if ! wait "$pid"; then
    note "$label exited unsuccessfully"
    return 1
  fi
}

reserve_shm() {
  local path=$1
  (set -o noclobber; : >"$path") 2>/dev/null ||
    die "refusing to reuse existing SHM object: $path"
}

run_with_cpu() {
  local cpu=$1
  shift
  if [[ -n "$cpu" ]]; then
    exec taskset -c "$cpu" "$@"
  else
    exec "$@"
  fi
}

reserve_rt_layout() {
  ((rt_priority == 0)) && return 0
  local cpu_count candidate cpu core other
  local -a cpus=("$@") cores=()
  cpu_count=$(nproc)
  ((cpu_count > ${#cpus[@]})) || die "--rt-priority needs one additional CPU for the controller"
  for cpu in "${cpus[@]}"; do
    is_uint "$cpu" && ((10#$cpu < cpu_count)) || die "--rt-priority requires one valid single CPU number per benchmark process"
    core=$(lscpu -p=CPU,CORE | awk -F, -v wanted="$((10#$cpu))" '$1 == wanted { print $2; exit }')
    [[ -n "$core" ]] || die "could not determine the physical core for CPU $cpu"
    for other in "${cores[@]}"; do
      [[ "$core" != "$other" ]] || die "--rt-priority requires benchmark CPUs on distinct physical cores"
    done
    cores+=("$core")
  done
  for ((candidate = 0; candidate < cpu_count; ++candidate)); do
    core=$(lscpu -p=CPU,CORE | awk -F, -v wanted="$candidate" '$1 == wanted { print $2; exit }')
    [[ -n "$core" ]] || continue
    for other in "${cores[@]}"; do [[ "$core" != "$other" ]] || break; done
    [[ "$core" == "$other" ]] && continue
    controller_cpu=$candidate
    break
  done
  [[ -n "$controller_cpu" ]] || die "could not reserve a controller CPU for --rt-priority"
  taskset -pc "$controller_cpu" "$$" >/dev/null || die "could not pin benchmark controller"
}

# Reserve the exact regular files used by shm_open(O_CREAT) atomically. This
# proves ownership before any process may truncate them and makes cleanup safe.
reserve_shm "/dev/shm/${in_shm#/}"
created_in_shm=1
reserve_shm "/dev/shm/${out_shm#/}"
created_out_shm=1
reserve_rt_layout "$cpu_producer" "$cpu_sender" "$cpu_receiver" "$cpu_consumer"

{
  printf 'started_utc=%s\n' "$(date -u +%FT%TZ)"
  printf 'repo_root=%q\n' "$repo_root"
  printf 'git_revision=%s\n' "$(git -c safe.directory="$repo_root" -C "$repo_root" rev-parse HEAD 2>/dev/null || printf unknown)"
  printf 'kernel=%q\n' "$(uname -a)"
  printf 'loss_pct=%q\nloss_model=%q\nloss_ge_r_pct=%q\nloss_ge_good_pct=%q\nloss_ge_bad_pct=%q\ndelay_us=%q\ndelay_jitter_us=%q\nreorder_pct=%q\nfec_mode=%q\nwire_format=%q\nudp_mode=%q\nrecv_batch=%q\nsocket_drop_counters=%q\nreorder_wait_us=%q\nbatch_bytes=%q\nprofile_sample_every=%q\nnetem_seed=%q\nmeasurement_count=%q\nwarmup_ms=%q\nwarmup_messages=%q\nproducer_ready_delay_ms=%q\ntotal_stream_count=%q\nrate=%q\ntype=%q\nslots=%q\nidle_ms=%q\n' "$loss" "$loss_model" "$loss_ge_r" "$loss_ge_good" "$loss_ge_bad" "$delay_us" "$delay_jitter_us" "$reorder_pct" "$fec_mode" "$wire_format" "$udp_mode" "$recv_batch" "$socket_drop_counters" "$reorder_wait_us" "$batch_bytes" "$profile_sample_every" "${netem_seed:-kernel-random}" "$count" "$warmup_ms" "$warmup_messages" "$producer_ready_delay_ms" "$total_count" "$rate" "$kind" "$slots" "$idle_ms"
  printf 'cpu_producer=%q\ncpu_sender=%q\ncpu_receiver=%q\ncpu_consumer=%q\n' "$cpu_producer" "$cpu_sender" "$cpu_receiver" "$cpu_consumer"
  printf 'tx_ns=%q\nrx_ns=%q\ntx_if=%q\nrx_if=%q\ntx_ip=%q\nrx_ip=%q\nport=%q\nsession_id=%q\n' "$tx_ns" "$rx_ns" "$tx_if" "$rx_if" "$tx_ip" "$rx_ip" "$port" "$session_id"
  printf 'in_shm=%q\nout_shm=%q\nconsumer_csv=%q\n' "$in_shm" "$out_shm" "$consumer_csv"
  printf 'ip_version=%q\n' "$(ip -V 2>&1)"
  printf 'tc_version=%q\n' "$(tc -V 2>&1)"
} >"$manifest"

note "creating isolated namespaces and veth pair (results: $output_dir)"
sudo -n ip netns add "$tx_ns" >>"$setup_log" 2>&1
created_tx_ns=1
sudo -n ip netns add "$rx_ns" >>"$setup_log" 2>&1
created_rx_ns=1
sudo -n ip link add "$tx_if" type veth peer name "$rx_if" >>"$setup_log" 2>&1
created_veth=1
sudo -n ip link set "$tx_if" netns "$tx_ns" >>"$setup_log" 2>&1
sudo -n ip link set "$rx_if" netns "$rx_ns" >>"$setup_log" 2>&1
sudo -n ip -n "$tx_ns" addr add "$tx_ip/30" dev "$tx_if" >>"$setup_log" 2>&1
sudo -n ip -n "$rx_ns" addr add "$rx_ip/30" dev "$rx_if" >>"$setup_log" 2>&1
sudo -n ip -n "$tx_ns" link set lo up >>"$setup_log" 2>&1
sudo -n ip -n "$rx_ns" link set lo up >>"$setup_log" 2>&1
sudo -n ip -n "$tx_ns" link set "$tx_if" up >>"$setup_log" 2>&1
sudo -n ip -n "$rx_ns" link set "$rx_if" up >>"$setup_log" 2>&1
# This is the only qdisc created by the benchmark, and it is on its private
# sender-side veth. No host, Docker, eth0, or loopback interface is touched.
if [[ "$loss_model" == random ]]; then
  netem_args=(loss random "${loss}%")
else
  netem_args=(loss gemodel "${loss}%" "${loss_ge_r}%" "${loss_ge_bad}%" "${loss_ge_good}%")
fi
if ((delay_us > 0)); then
  netem_args+=(delay "${delay_us}us")
  ((delay_jitter_us > 0)) && netem_args+=("${delay_jitter_us}us")
fi
awk -v reorder="$reorder_pct" 'BEGIN { exit !(reorder > 0) }' && netem_args+=(reorder "${reorder_pct}%")
if [[ -n "$netem_seed" ]]; then netem_args+=(seed "$netem_seed"); fi
sudo -n ip netns exec "$tx_ns" tc qdisc add dev "$tx_if" root netem "${netem_args[@]}" >>"$setup_log" 2>&1
sudo -n ip -n "$tx_ns" addr show dev "$tx_if" >>"$setup_log" 2>&1
sudo -n ip -n "$rx_ns" addr show dev "$rx_if" >>"$setup_log" 2>&1
sudo -n ip netns exec "$tx_ns" tc qdisc show dev "$tx_if" >>"$setup_log" 2>&1

run_user=$(id -un)
run_uid=$(id -u)
run_gid=$(id -g)
run_in_namespace() {
  local ns=$1 cpu=$2
  shift 2
  if [[ -n "$cpu" ]]; then
    exec sudo -n ip netns exec "$ns" runuser -u "$run_user" -- taskset -c "$cpu" "$@"
  else
    exec sudo -n ip netns exec "$ns" runuser -u "$run_user" -- "$@"
  fi
}
apply_rt_priority() {
  local pid=$1 label=$2 actual
  ((rt_priority == 0)) && return 0
  sudo -n chrt -f --pid "$rt_priority" "$pid" >/dev/null ||
    die "could not set SCHED_FIFO priority $rt_priority for $label"
  actual=$(ps -o rtprio= -p "$pid" | tr -d '[:space:]')
  [[ "$actual" == "$rt_priority" ]] || die "$label does not have requested SCHED_FIFO priority $rt_priority (actual: ${actual:-none})"
}
printf 'run_user=%q\nrun_uid=%q\nrun_gid=%q\nrt_priority=%q\ncontroller_cpu=%q\n' "$run_user" "$run_uid" "$run_gid" "$rt_priority" "$controller_cpu" >>"$manifest"

# Receiver first: its ready line is emitted only after binding UDP and creating
# the output ring. Then wait until the fixed consumer has actually mmap'ed it.
receiver_args=("$harness_dir/bin/udp_receiver"
  --out-shm "$out_shm" --out-slots "$slots" --bind "$rx_ip" --port "$port"
  --count "$total_count" --idle-ms "$idle_ms" --session-id "$session_id"
  --fec "$fec_mode" --recv-batch "$recv_batch"
  --reorder-wait-us "$reorder_wait_us" --batch-bytes "$batch_bytes"
  --profile-sample-every "$profile_sample_every" --wire-format "$wire_format")
((profile_sample_every)) && receiver_args+=(--profile-from-seq "$warmup_messages")
((socket_drop_counters)) && receiver_args+=(--socket-drop-counters)
run_in_namespace "$rx_ns" "$cpu_receiver" "${receiver_args[@]}" \
  >"$receiver_log" 2>&1 & receiver_pid=$!
wait_for_log "$receiver_log" 'udp_receiver: ready ' "$startup_timeout_s" receiver
apply_rt_priority "$receiver_pid" receiver
start_consumer() {
  consumer_args=("$harness_dir/bin/consumer" --shm "$out_shm" --slots "$slots"
    --count "$count" --idle-ms "$idle_ms" --profile-sample-every "$profile_sample_every")
  ((profile_sample_every)) && consumer_args+=(--profile-from-seq "$warmup_messages")
  # Drop by sequence boundary, not delivered-message count: packet loss in
  # warm-up must not leak pre-warm-up samples into the measured window.
  ((warmup_messages > 0)) && consumer_args+=(--drop-through-seq "$warmup_messages")
  if ((consumer_csv)); then
    consumer_args+=(--csv "$output_dir/consumer.csv")
  fi
  run_with_cpu "$cpu_consumer" "${consumer_args[@]}" >"$consumer_log" 2>&1 & consumer_pid=$!
  wait_for_mapping "$consumer_pid" "$out_shm" "$startup_timeout_s" consumer
  apply_rt_priority "$consumer_pid" consumer
}
start_consumer

# Sender is intentionally started before the producer; it waits for a fully
# initialized input ring rather than relying on a timing sleep.
run_in_namespace "$tx_ns" "$cpu_sender" "$harness_dir/bin/udp_sender" \
  --in-shm "$in_shm" --in-slots "$slots" --host "$rx_ip" --port "$port" \
  --count "$total_count" --wait-ms "$((startup_timeout_s * 1000))" --idle-ms "$idle_ms" \
  --session-id "$session_id" --fec "$fec_mode" --wire-format "$wire_format" --udp-mode "$udp_mode" --batch-bytes "$batch_bytes" --profile-sample-every "$profile_sample_every" --profile-from-seq "$warmup_messages" >"$sender_log" 2>&1 & sender_pid=$!
apply_rt_priority "$sender_pid" sender
run_with_cpu "$cpu_producer" "$harness_dir/bin/producer" --shm "$in_shm" --slots "$slots" \
  --count "$total_count" --rate "$rate" --type "$kind" --ready-delay-ms "$producer_ready_delay_ms" >"$producer_log" 2>&1 & producer_pid=$!
apply_rt_priority "$producer_pid" producer
run_deadline=$((SECONDS + run_timeout_s))

note "measuring $count messages after discarding $warmup_messages warm-up messages at $rate msg/s with $loss% configured loss"
wait_for_exit "$producer_pid" producer "$run_deadline"
wait_for_exit "$sender_pid" sender "$run_deadline"
wait_for_exit "$receiver_pid" receiver "$run_deadline"
wait_for_exit "$consumer_pid" consumer "$run_deadline"

# Preserve the qdisc counters before teardown.  These count packets discarded
# by the configured link emulator, which is distinct from residual message
# loss after FEC and resequencing.
sudo -n ip netns exec "$tx_ns" tc -s qdisc show dev "$tx_if" >"$qdisc_stats" 2>&1 ||
  die "could not collect netem qdisc statistics"

extract_counter() {
  local file=$1 key=$2
  sed -n "s/.*[[:space:]]${key}=\\([0-9][0-9]*\\).*/\\1/p" "$file" | tail -n 1
}
extract_counter_or_zero() {
  local value
  value=$(extract_counter "$1" "$2")
  [[ $value =~ ^[0-9]+$ ]] && printf '%s\n' "$value" || printf '0\n'
}
sender_lapped=$(extract_counter "$sender_log" input_lapped)
sender_invalid=$(extract_counter "$sender_log" invalid)
sender_socket_drops=$(extract_counter "$sender_log" socket_drops)
sender_sent=$(extract_counter "$sender_log" sent)
sender_data_datagrams=$(extract_counter "$sender_log" data_datagrams)
sender_parity_sent=$(extract_counter "$sender_log" parity_sent)
sender_data_payload_bytes=$(extract_counter "$sender_log" data_payload_bytes)
sender_data_payload_bytes_max=$(extract_counter "$sender_log" data_payload_bytes_max)
sender_batch_frames_1=$(extract_counter "$sender_log" batch_frames_1)
sender_batch_frames_2=$(extract_counter "$sender_log" batch_frames_2)
sender_batch_frames_3=$(extract_counter "$sender_log" batch_frames_3)
sender_batch_frames_4=$(extract_counter "$sender_log" batch_frames_4)
sender_batch_frames_5=$(extract_counter "$sender_log" batch_frames_5)
sender_batch_frames_6=$(extract_counter "$sender_log" batch_frames_6)
sender_batch_frames_7plus=$(extract_counter "$sender_log" batch_frames_7plus)
sender_profile_operation_p50=$(extract_counter_or_zero "$sender_log" udp_sender_profile_operation_p50_ns)
sender_profile_operation_p99=$(extract_counter_or_zero "$sender_log" udp_sender_profile_operation_p99_ns)
sender_profile_send_p50=$(extract_counter_or_zero "$sender_log" udp_sender_profile_send_p50_ns)
sender_profile_send_p99=$(extract_counter_or_zero "$sender_log" udp_sender_profile_send_p99_ns)
sender_cpu_user_ns=$(extract_counter "$sender_log" cpu_user_ns)
sender_cpu_system_ns=$(extract_counter "$sender_log" cpu_system_ns)
sender_involuntary_cs=$(extract_counter "$sender_log" involuntary_cs)
receiver_invalid=$(extract_counter "$receiver_log" invalid)
receiver_session_mismatch=$(extract_counter "$receiver_log" session_mismatch)
receiver_missing=$(extract_counter "$receiver_log" apparent_missing)
receiver_accepted=$(extract_counter "$receiver_log" accepted)
receiver_first_seq=$(extract_counter "$receiver_log" first_seq)
receiver_fec_parity=$(extract_counter "$receiver_log" fec_parity)
receiver_fec_recovered=$(extract_counter "$receiver_log" fec_recovered)
receiver_fec_evicted=$(extract_counter "$receiver_log" fec_evicted)
receiver_socket_drops=$(extract_counter "$receiver_log" receiver_socket_drops)
receiver_socket_drop_counter=$(sed -n 's/.*[[:space:]]receiver_socket_drop_counter=\([a-z][a-z]*\).*/\1/p' "$receiver_log" | tail -n 1)
receiver_cpu_user_ns=$(extract_counter "$receiver_log" cpu_user_ns)
receiver_cpu_system_ns=$(extract_counter "$receiver_log" cpu_system_ns)
receiver_involuntary_cs=$(extract_counter "$receiver_log" involuntary_cs)
receiver_profile_recvmmsg_p50=$(extract_counter_or_zero "$receiver_log" udp_receiver_profile_recvmmsg_p50_ns)
receiver_profile_recvmmsg_p99=$(extract_counter_or_zero "$receiver_log" udp_receiver_profile_recvmmsg_p99_ns)
receiver_profile_process_p50=$(extract_counter_or_zero "$receiver_log" udp_receiver_profile_process_p50_ns)
receiver_profile_process_p99=$(extract_counter_or_zero "$receiver_log" udp_receiver_profile_process_p99_ns)
consumer_lapped=$(sed -n 's/^consumer: lapped \([0-9][0-9]*\) times$/\1/p' "$consumer_log" | tail -n 1)
consumer_received=$(sed -n 's/^received     : \([0-9][0-9]*\)$/\1/p' "$consumer_log" | tail -n 1)
consumer_expected=$(sed -n 's/^expected     : \([0-9][0-9]*\)$/\1/p' "$consumer_log" | tail -n 1)
consumer_dropped=$(sed -n 's/^dropped      : \([0-9][0-9]*\)$/\1/p' "$consumer_log" | tail -n 1)
netem_qdisc_dropped=$(sed -n 's/.*dropped \([0-9][0-9]*\),.*/\1/p' "$qdisc_stats" | tail -n 1)
for value in sender_lapped sender_invalid sender_socket_drops sender_sent sender_data_datagrams sender_parity_sent sender_data_payload_bytes sender_data_payload_bytes_max sender_batch_frames_1 sender_batch_frames_2 sender_batch_frames_3 sender_batch_frames_4 sender_batch_frames_5 sender_batch_frames_6 sender_batch_frames_7plus sender_cpu_user_ns sender_cpu_system_ns sender_involuntary_cs receiver_invalid receiver_session_mismatch receiver_accepted receiver_first_seq receiver_fec_parity receiver_fec_recovered receiver_fec_evicted receiver_socket_drops receiver_cpu_user_ns receiver_cpu_system_ns receiver_involuntary_cs consumer_lapped consumer_received consumer_expected consumer_dropped netem_qdisc_dropped; do
  [[ ${!value} =~ ^[0-9]+$ ]] || die "could not parse $value; inspect $output_dir"
done
[[ $receiver_socket_drop_counter =~ ^(disabled|enabled|unavailable)$ ]] || die "could not parse receiver socket-drop-counter status; inspect $output_dir"

failure=
((sender_lapped == 0)) || failure="sender input SHM lapped ($sender_lapped)"
((sender_invalid == 0)) || failure="sender rejected invalid input frame(s) ($sender_invalid)"
((sender_socket_drops == 0)) || failure="sender socket dropped datagram(s) ($sender_socket_drops)"
((sender_sent == total_count)) || failure="sender sent $sender_sent of $total_count messages"
((receiver_invalid == 0)) || failure="receiver saw invalid datagram(s) ($receiver_invalid)"
((receiver_session_mismatch == 0)) || failure="receiver saw session mismatch(es) ($receiver_session_mismatch)"
((receiver_first_seq <= 64)) || failure="receiver started too late at seq_id $receiver_first_seq"
((consumer_lapped == 0)) || failure="consumer output SHM lapped ($consumer_lapped)"
if [[ "$fec_mode" != none ]]; then
  ((sender_parity_sent > 0)) || failure="FEC sender emitted no parity"
else
  ((sender_parity_sent == 0 && receiver_fec_parity == 0 && receiver_fec_recovered == 0)) ||
    failure="raw mode reported FEC activity"
fi
if awk -v loss="$loss" -v reorder="$reorder_pct" -v jitter="$delay_jitter_us" 'BEGIN { exit !(loss == 0 && reorder == 0 && jitter == 0) }'; then
  ((receiver_accepted == total_count)) || failure="0%-loss run received $receiver_accepted of $total_count messages"
  ((consumer_received == count)) || failure="0%-loss consumer measured $consumer_received of $count messages"
fi
total_network_missing=$((total_count - receiver_accepted))
measurement_missing_total=$((count - consumer_received))
netem_qdisc_offered=$(awk '/Sent [0-9]+ bytes [0-9]+ pkt/ {
  for (i = 1; i <= NF; ++i) if ($i == "bytes") { print $(i + 1); exit }
}' "$qdisc_stats")
[[ $netem_qdisc_offered =~ ^[0-9]+$ ]] || die "could not parse netem offered datagrams; inspect $output_dir"
netem_qdisc_drop_pct=$(awk -v dropped="$netem_qdisc_dropped" -v offered="$netem_qdisc_offered" 'BEGIN {
  printf "%.6f", offered == 0 ? 0 : 100 * dropped / offered
}')

{
  if [[ -n "$failure" ]]; then printf 'status=fail\nreason=%s\n' "$failure"; else printf 'status=pass\n'; fi
  printf 'measurement_count=%s\nwarmup_messages=%s\ntotal_stream_count=%s\nmeasurement_missing_total=%s\n' "$count" "$warmup_messages" "$total_count" "$measurement_missing_total"
  printf 'sender_sent=%s\nsender_data_datagrams=%s\nsender_parity_sent=%s\nsender_data_payload_bytes=%s\nsender_data_payload_bytes_max=%s\nsender_batch_frames_1=%s\nsender_batch_frames_2=%s\nsender_batch_frames_3=%s\nsender_batch_frames_4=%s\nsender_batch_frames_5=%s\nsender_batch_frames_6=%s\nsender_batch_frames_7plus=%s\nsender_input_lapped=%s\nsender_invalid=%s\nsender_socket_drops=%s\n' "$sender_sent" "$sender_data_datagrams" "$sender_parity_sent" "$sender_data_payload_bytes" "$sender_data_payload_bytes_max" "$sender_batch_frames_1" "$sender_batch_frames_2" "$sender_batch_frames_3" "$sender_batch_frames_4" "$sender_batch_frames_5" "$sender_batch_frames_6" "$sender_batch_frames_7plus" "$sender_lapped" "$sender_invalid" "$sender_socket_drops"
  printf 'sender_cpu_user_ns=%s\nsender_cpu_system_ns=%s\nsender_involuntary_cs=%s\n' "$sender_cpu_user_ns" "$sender_cpu_system_ns" "$sender_involuntary_cs"
  printf 'sender_profile_operation_p50_ns=%s\nsender_profile_operation_p99_ns=%s\nsender_profile_send_p50_ns=%s\nsender_profile_send_p99_ns=%s\n' "$sender_profile_operation_p50" "$sender_profile_operation_p99" "$sender_profile_send_p50" "$sender_profile_send_p99"
  printf 'receiver_first_seq=%s\nreceiver_accepted=%s\nnetwork_missing_total=%s\nreceiver_fec_parity=%s\nreceiver_fec_recovered=%s\nreceiver_fec_evicted=%s\nreceiver_socket_drop_counter=%s\nreceiver_socket_drops=%s\nreceiver_invalid=%s\nreceiver_session_mismatch=%s\nreceiver_apparent_missing=%s\n' "$receiver_first_seq" "$receiver_accepted" "$total_network_missing" "$receiver_fec_parity" "$receiver_fec_recovered" "$receiver_fec_evicted" "$receiver_socket_drop_counter" "$receiver_socket_drops" "$receiver_invalid" "$receiver_session_mismatch" "${receiver_missing:-unavailable}"
  printf 'receiver_cpu_user_ns=%s\nreceiver_cpu_system_ns=%s\nreceiver_involuntary_cs=%s\n' "$receiver_cpu_user_ns" "$receiver_cpu_system_ns" "$receiver_involuntary_cs"
  printf 'receiver_profile_recvmmsg_p50_ns=%s\nreceiver_profile_recvmmsg_p99_ns=%s\nreceiver_profile_process_p50_ns=%s\nreceiver_profile_process_p99_ns=%s\n' "$receiver_profile_recvmmsg_p50" "$receiver_profile_recvmmsg_p99" "$receiver_profile_process_p50" "$receiver_profile_process_p99"
  printf 'consumer_received=%s\nconsumer_expected=%s\nconsumer_internal_seq_gaps=%s\nconsumer_lapped=%s\n' "$consumer_received" "$consumer_expected" "$consumer_dropped" "$consumer_lapped"
  printf 'netem_qdisc_offered_datagrams=%s\nnetem_qdisc_dropped_datagrams=%s\nnetem_qdisc_drop_pct=%s\n' "$netem_qdisc_offered" "$netem_qdisc_dropped" "$netem_qdisc_drop_pct"
  sha256sum "$manifest" "$setup_log" "$producer_log" "$sender_log" "$receiver_log" "$consumer_log" "$qdisc_stats"
} >"$result_file"

[[ -z "$failure" ]] || die "$failure"

note "PASS — expected network loss is reported by receiver/consumer, not treated as a harness failure"
printf 'results=%s\n' "$output_dir"
