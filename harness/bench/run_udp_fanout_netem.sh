#!/usr/bin/env bash
# Run one sender to several receivers over independent, private veth links.
# Each receiver has its own netem qdisc, so a loss on one link cannot be
# mistaken for a loss shared by every consumer.
set -Eeuo pipefail
IFS=$'\n\t'

usage() {
  cat <<'EOF'
Usage: harness/bench/run_udp_fanout_netem.sh [options]

Runs: producer -> input SHM -> udp_sender (one tx netns) -> N private
veth/netem links -> N udp_receivers (separate rx netnses) -> N consumers.

Options:
  --receivers N          2 or 3 receivers (default: 2)
  --losses PCTS          One loss percentage or one comma-separated percentage
                         per receiver, e.g. 0.1,1,0.1 (default: 0)
  --loss-model MODE      random|gemodel (default: random)
  --loss-ge-r PCT        Gilbert-Elliott bad-to-good probability (default: 25)
  --loss-ge-bad PCT      Gilbert-Elliott bad-state loss probability (default: 50)
  --loss-ge-good PCT     Gilbert-Elliott good-state loss probability (default: 0)
  --delay-us N           One-way delay on every private link (default: 0)
  --delay-jitter-us N    Random delay variation (default: 0)
  --reorder PCT          Reordering probability; requires delay (default: 0)
  --netem-seed N         Fixed base seed; link i uses N+i
  --fec MODE             none|xor4|xor8|rs4_2|rs8_2|packet_xor4 (default: none)
  --wire-format MODE     full|compact (default: full)
  --sender-topology MODE shared|per-receiver (default: shared)
  --sender-workers N     Worker count for per-receiver topology (default: receivers)
  --fanout-mode MODE     loop|sendmmsg (default: loop)
  --udp-mode MODE        sendto|connected (default: sendto)
  --recv-batch N         Receiver recvmmsg batch, 1 through 16 (default: 1)
  --socket-buffer BYTES  Sender and receiver socket buffer request (default: 4194304)
  --batch-bytes N        Pack full frames up to N bytes; 0 disables (default: 1472)
  --reorder-wait-us N    Receiver wait for a missing sequence number, or auto (default: auto)
  --profile-sample-every N  Sample every Nth transport operation (default: 0)
  --producer-burst-size N  Publish N messages per producer pacing interval (default: 1)
  --cpu-producer CPU     Optional CPU for producer
  --cpu-sender CPU       Optional CPU for sender
  --cpu-senders CPUS     CPU per sender in per-receiver mode
  --cpu-receivers CPUS   Comma-separated CPU per receiver, exactly N entries
  --cpu-consumers CPUS   Comma-separated CPU per consumer, exactly N entries
  --count N              Producer messages (default: 100000)
  --rate N               Producer messages/sec; 0 is unrestricted (default: 50000)
  --type T               trade|bbo|book|mixed (default: mixed)
  --slots N              Power-of-two SHM slots (default: 65536)
  --idle-ms N            Process idle stop interval (default: 5000)
  --output DIR           New results directory (default: harness/results unique)
  --dry-run              Validate and show the topology without privileged work
  --help                 Show this help

This is a correctness and loss-recovery experiment on one host. It is not an
ENA or multi-host latency result. It needs iproute2 and passwordless sudo.
EOF
}

die() { printf 'fanout netem benchmark: %s\n' "$*" >&2; exit 2; }
note() { printf 'fanout netem benchmark: %s\n' "$*" >&2; }
is_uint() { [[ $1 =~ ^[0-9]+$ ]]; }
is_number() { [[ $1 =~ ^([0-9]+([.][0-9]*)?|[.][0-9]+)$ ]]; }

receivers=2
losses=0
loss_model=random
loss_ge_r=25
loss_ge_bad=50
loss_ge_good=0
delay_us=0
delay_jitter_us=0
reorder_pct=0
netem_seed=
fec=none
wire_format=full
sender_topology=shared
sender_workers=0
sender_workers_auto=0
fanout_mode=loop
udp_mode=sendto
recv_batch=1
socket_buffer=$((4 * 1024 * 1024))
batch_bytes=1472
reorder_wait_us=auto
profile_sample_every=0
producer_burst_size=1
cpu_producer=
cpu_sender=
cpu_senders=
cpu_receivers=
cpu_consumers=
count=100000
rate=50000
kind=mixed
slots=65536
idle_ms=5000
output=
dry_run=0

while (($#)); do
  case "$1" in
    --receivers) receivers=${2-}; shift 2 ;;
    --losses) losses=${2-}; shift 2 ;;
    --loss-model) loss_model=${2-}; shift 2 ;;
    --loss-ge-r) loss_ge_r=${2-}; shift 2 ;;
    --loss-ge-bad) loss_ge_bad=${2-}; shift 2 ;;
    --loss-ge-good) loss_ge_good=${2-}; shift 2 ;;
    --delay-us) delay_us=${2-}; shift 2 ;;
    --delay-jitter-us) delay_jitter_us=${2-}; shift 2 ;;
    --reorder) reorder_pct=${2-}; shift 2 ;;
    --netem-seed) netem_seed=${2-}; shift 2 ;;
    --fec) fec=${2-}; shift 2 ;;
    --wire-format) wire_format=${2-}; shift 2 ;;
    --sender-topology) sender_topology=${2-}; shift 2 ;;
    --sender-workers) sender_workers=${2-}; shift 2 ;;
    --sender-workers-auto) sender_workers_auto=1; shift ;;
    --fanout-mode) fanout_mode=${2-}; shift 2 ;;
    --udp-mode) udp_mode=${2-}; shift 2 ;;
    --recv-batch) recv_batch=${2-}; shift 2 ;;
    --socket-buffer) socket_buffer=${2-}; shift 2 ;;
    --batch-bytes) batch_bytes=${2-}; shift 2 ;;
    --reorder-wait-us) reorder_wait_us=${2-}; shift 2 ;;
    --profile-sample-every) profile_sample_every=${2-}; shift 2 ;;
    --producer-burst-size) producer_burst_size=${2-}; shift 2 ;;
    --cpu-producer) cpu_producer=${2-}; shift 2 ;;
    --cpu-sender) cpu_sender=${2-}; shift 2 ;;
    --cpu-senders) cpu_senders=${2-}; shift 2 ;;
    --cpu-receivers) cpu_receivers=${2-}; shift 2 ;;
    --cpu-consumers) cpu_consumers=${2-}; shift 2 ;;
    --count) count=${2-}; shift 2 ;;
    --rate) rate=${2-}; shift 2 ;;
    --type) kind=${2-}; shift 2 ;;
    --slots) slots=${2-}; shift 2 ;;
    --idle-ms) idle_ms=${2-}; shift 2 ;;
    --output) output=${2-}; shift 2 ;;
    --dry-run) dry_run=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done

[[ $receivers =~ ^[23]$ ]] || die '--receivers must be 2 or 3'
is_uint "$count" && ((count > 0)) || die '--count must be a positive integer'
is_number "$rate" || die '--rate must be a non-negative number'
is_uint "$idle_ms" || die '--idle-ms must be a non-negative integer'
is_uint "$slots" && ((slots > 0 && (slots & (slots - 1)) == 0)) || die '--slots must be a power of two'
is_uint "$delay_us" || die '--delay-us must be a non-negative integer'
is_uint "$delay_jitter_us" || die '--delay-jitter-us must be a non-negative integer'
is_number "$reorder_pct" || die '--reorder must be a number from 0 through 100'
awk -v v="$reorder_pct" 'BEGIN { exit !(v >= 0 && v <= 100) }' || die '--reorder must be from 0 through 100'
((delay_jitter_us == 0 || delay_us > 0)) || die '--delay-jitter-us requires --delay-us greater than zero'
awk -v r="$reorder_pct" -v d="$delay_us" 'BEGIN { exit !(r == 0 || d > 0) }' || die '--reorder requires --delay-us greater than zero'
case "$loss_model" in random|gemodel) ;; *) die '--loss-model must be random or gemodel' ;; esac
for value in "$loss_ge_r" "$loss_ge_bad" "$loss_ge_good"; do
  is_number "$value" || die 'Gilbert-Elliott probabilities must be numbers'
  awk -v v="$value" 'BEGIN { exit !(v >= 0 && v <= 100) }' || die 'Gilbert-Elliott probabilities must be from 0 through 100'
done
case "$fec" in none|xor4|xor8|rs4_2|rs8_2|packet_xor4) ;; *) die '--fec has an invalid value' ;; esac
case "$wire_format" in full|compact) ;; *) die '--wire-format must be full or compact' ;; esac
case "$sender_topology" in shared|per-receiver) ;; *) die '--sender-topology must be shared or per-receiver' ;; esac
is_uint "$sender_workers" || die '--sender-workers must be a non-negative integer'
if [[ $sender_topology == per-receiver && $sender_workers == 0 ]]; then sender_workers=$receivers; fi
[[ $sender_topology == per-receiver || $sender_workers == 0 ]] || die '--sender-workers requires per-receiver topology'
[[ $sender_topology == shared || $sender_workers_auto == 0 ]] || die '--sender-workers-auto requires shared topology'
((sender_workers == 0 || sender_workers <= receivers)) || die '--sender-workers must not exceed receiver count'
case "$fanout_mode" in loop|sendmmsg) ;; *) die '--fanout-mode must be loop or sendmmsg' ;; esac
case "$udp_mode" in sendto|connected) ;; *) die '--udp-mode must be sendto or connected' ;; esac
is_uint "$recv_batch" && ((recv_batch >= 1 && recv_batch <= 16)) || die '--recv-batch must be from 1 through 16'
is_uint "$socket_buffer" && ((socket_buffer > 0)) || die '--socket-buffer must be positive'
[[ "$udp_mode:$fanout_mode" != connected:sendmmsg ]] || die 'connected mode uses one socket per receiver and cannot use sendmmsg'
[[ "$sender_topology:$fanout_mode" != per-receiver:sendmmsg ]] || die 'per-receiver topology cannot use sendmmsg'
is_uint "$batch_bytes" && ((batch_bytes == 0 || (batch_bytes >= 632 && batch_bytes <= 1472))) || die '--batch-bytes must be 0 or from 632 through 1472'
[[ "$fec" != packet_xor4 || ( "$wire_format" == compact && "$batch_bytes" != 0 ) ]] || die 'packet_xor4 requires compact wire format and packet packing'
[[ $reorder_wait_us == auto ]] || is_uint "$reorder_wait_us" || die '--reorder-wait-us must be a non-negative integer or auto'
is_uint "$profile_sample_every" || die '--profile-sample-every must be a non-negative integer'
is_uint "$producer_burst_size" && ((producer_burst_size > 0)) || die '--producer-burst-size must be positive'
case "$kind" in trade|bbo|book|mixed) ;; *) die '--type must be trade|bbo|book|mixed' ;; esac
[[ -z "$netem_seed" ]] || { is_uint "$netem_seed" && ((netem_seed > 0)); } || die '--netem-seed must be positive'

declare -a link_loss
IFS=, read -r -a supplied_losses <<<"$losses"
if ((${#supplied_losses[@]} == 1)); then
  for ((i = 0; i < receivers; ++i)); do link_loss[i]=${supplied_losses[0]}; done
elif ((${#supplied_losses[@]} == receivers)); then
  link_loss=("${supplied_losses[@]}")
else
  die "--losses requires one value or exactly $receivers comma-separated values"
fi
for value in "${link_loss[@]}"; do
  is_number "$value" || die '--losses values must be numbers from 0 through 100'
  awk -v v="$value" 'BEGIN { exit !(v >= 0 && v <= 100) }' || die '--losses values must be from 0 through 100'
done
for cpu in "$cpu_producer" "$cpu_sender"; do [[ -z $cpu || $cpu =~ ^[0-9]+$ ]] || die 'producer and sender CPU ids must be non-negative integers'; done
declare -a sender_cpu receiver_cpu consumer_cpu
if [[ -n $cpu_senders ]]; then
  IFS=, read -r -a sender_cpu <<<"$cpu_senders"
  ((${#sender_cpu[@]} == sender_workers)) || die "--cpu-senders requires one CPU id per sender worker"
  for cpu in "${sender_cpu[@]}"; do [[ $cpu =~ ^[0-9]+$ ]] || die '--cpu-senders CPU ids must be non-negative integers'; done
fi
[[ $sender_topology == shared || -z $cpu_sender ]] || die '--cpu-sender is only valid with shared topology'
if [[ -n $cpu_receivers ]]; then
  IFS=, read -r -a receiver_cpu <<<"$cpu_receivers"
  ((${#receiver_cpu[@]} == receivers)) || die "--cpu-receivers requires exactly $receivers CPU ids"
  for cpu in "${receiver_cpu[@]}"; do [[ $cpu =~ ^[0-9]+$ ]] || die '--cpu-receivers CPU ids must be non-negative integers'; done
fi
if [[ -n $cpu_consumers ]]; then
  IFS=, read -r -a consumer_cpu <<<"$cpu_consumers"
  ((${#consumer_cpu[@]} == receivers)) || die "--cpu-consumers requires exactly $receivers CPU ids"
  for cpu in "${consumer_cpu[@]}"; do [[ $cpu =~ ^[0-9]+$ ]] || die '--cpu-consumers CPU ids must be non-negative integers'; done
fi

repo_root=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
harness="$repo_root/harness"
for tool in ip tc runuser taskset awk grep sed date mktemp sha256sum sleep sudo; do command -v "$tool" >/dev/null || die "missing tool: $tool"; done
for binary in producer consumer udp_sender udp_receiver; do [[ -x "$harness/bin/$binary" ]] || die "missing $binary; run make -C harness"; done

tag="$(date -u +%Y%m%dT%H%M%SZ)-$$"
short_tag=$(printf '%x' "$$")
tx_ns="sc-ftx-$tag"
in_shm="/sc_fnet_in_$tag"
base_port=$((28000 + ($$ % 5000)))
session_id=$((($(date +%s) << 20) + $$))
declare -a rx_ns tx_if rx_if tx_ip rx_ip port out_shm target receiver_pid consumer_pid sender_pid made_rx_ns made_veth made_out_shm
producer_pid= watchdog_pid=
made_tx_ns=0
made_in_shm=0

if ((dry_run)); then
  printf 'dry run: sender namespace: %s\n' "$tx_ns"
  for ((i = 0; i < receivers; ++i)); do
    printf '  link %d: 198.18.%d.1 -> 198.18.%d.2, loss %s%%\n' "$((i + 1))" "$((i + 1))" "$((i + 1))" "${link_loss[i]}"
  done
  exit 0
fi
sudo -n ip -Version >/dev/null 2>&1 || die 'passwordless sudo permission for ip is required'
if [[ -n "$output" ]]; then
  [[ ! -e "$output" ]] || die "--output already exists: $output"
  mkdir -p -- "$output"
else
  mkdir -p -- "$harness/results"
  output=$(mktemp -d "$harness/results/udp-fanout-netem.XXXXXX")
fi
output=$(CDPATH= cd -- "$output" && pwd)
setup_log="$output/setup.log"
manifest="$output/manifest.txt"

cleanup() {
  local rc=$?
  set +e
  [[ -n ${watchdog_pid:-} ]] && kill "$watchdog_pid" 2>/dev/null
  for pid in "${producer_pid:-}" "${sender_pid[@]:-}" "${receiver_pid[@]:-}" "${consumer_pid[@]:-}"; do [[ -n $pid ]] && kill -TERM "$pid" 2>/dev/null; done
  for pid in "${producer_pid:-}" "${sender_pid[@]:-}" "${receiver_pid[@]:-}" "${consumer_pid[@]:-}"; do [[ -n $pid ]] && wait "$pid" 2>/dev/null; done
  for ((i = 0; i < receivers; ++i)); do
    [[ ${made_rx_ns[i]:-0} == 1 ]] && sudo -n ip netns del "${rx_ns[i]}" >>"$setup_log" 2>&1
  done
  ((made_tx_ns)) && sudo -n ip netns del "$tx_ns" >>"$setup_log" 2>&1
  ((made_in_shm)) && rm -f -- "/dev/shm/${in_shm#/}"
  for ((i = 0; i < receivers; ++i)); do [[ ${made_out_shm[i]:-0} == 1 ]] && rm -f -- "/dev/shm/${out_shm[i]#/}"; done
  exit "$rc"
}
trap cleanup EXIT INT TERM

reserve_shm() { (set -o noclobber; : >"$1") 2>/dev/null || die "refusing to reuse SHM object: $1"; }
wait_ready() {
  local file=$1 pattern=$2 label=$3 deadline=$((SECONDS + 10))
  while ((SECONDS < deadline)); do grep -Fq -- "$pattern" "$file" 2>/dev/null && return 0; sleep .01; done
  die "$label did not become ready; inspect $file"
}
wait_mapping() {
  local pid=$1 name=$2 label=$3 path="/dev/shm/${2#/}" deadline=$((SECONDS + 10))
  while ((SECONDS < deadline)); do
    kill -0 "$pid" 2>/dev/null || die "$label exited before mapping $name"
    grep -Fq -- "$path" "/proc/$pid/maps" 2>/dev/null && return 0
    sleep .01
  done
  die "$label did not map $name"
}
wait_exit() {
  local pid=$1 label=$2 deadline=$3
  while kill -0 "$pid" 2>/dev/null; do ((SECONDS < deadline)) || die "$label exceeded run deadline"; sleep .05; done
  wait "$pid" || die "$label exited unsuccessfully"
}
run_host() {
  local cpu=$1; shift
  [[ -n $cpu ]] && exec taskset -c "$cpu" "$@"
  exec "$@"
}
run_netns() {
  local ns=$1 cpu=$2; shift 2
  if [[ -n $cpu ]]; then
    exec sudo -n ip netns exec "$ns" runuser -u "$(id -un)" -- taskset -c "$cpu" "$@"
  fi
  exec sudo -n ip netns exec "$ns" runuser -u "$(id -un)" -- "$@"
}

reserve_shm "/dev/shm/${in_shm#/}"
made_in_shm=1
{
  printf 'started_utc=%s\ngit_revision=%s\nkernel=%q\nreceivers=%s\nloss_model=%q\nloss_ge_r_pct=%q\nloss_ge_bad_pct=%q\nloss_ge_good_pct=%q\ndelay_us=%q\ndelay_jitter_us=%q\nreorder_pct=%q\nfec=%q\nwire_format=%q\nsender_topology=%q\nfanout_mode=%q\nudp_mode=%q\nrecv_batch=%q\nsocket_buffer=%q\nbatch_bytes=%q\nreorder_wait_us=%q\nprofile_sample_every=%q\ncount=%q\nrate=%q\ntype=%q\nslots=%q\nidle_ms=%q\nnetem_seed_base=%q\n' "$(date -u +%FT%TZ)" "$(git -c safe.directory="$repo_root" -C "$repo_root" rev-parse HEAD)" "$(uname -a)" "$receivers" "$loss_model" "$loss_ge_r" "$loss_ge_bad" "$loss_ge_good" "$delay_us" "$delay_jitter_us" "$reorder_pct" "$fec" "$wire_format" "$sender_topology" "$fanout_mode" "$udp_mode" "$recv_batch" "$socket_buffer" "$batch_bytes" "$reorder_wait_us" "$profile_sample_every" "$count" "$rate" "$kind" "$slots" "$idle_ms" "${netem_seed:-kernel-random}"
} >"$manifest"
printf 'cpu_producer=%q\ncpu_sender=%q\ncpu_senders=%q\ncpu_receivers=%q\ncpu_consumers=%q\n' "$cpu_producer" "$cpu_sender" "$cpu_senders" "$cpu_receivers" "$cpu_consumers" >>"$manifest"

note "creating $receivers independent sender-to-receiver links (results: $output)"
sudo -n ip netns add "$tx_ns" >>"$setup_log" 2>&1
made_tx_ns=1
sudo -n ip -n "$tx_ns" link set lo up >>"$setup_log" 2>&1
for ((i = 0; i < receivers; ++i)); do
  n=$((i + 1))
  rx_ns[i]="sc-frx-${tag}-${n}"
  tx_if[i]="ft${short_tag}${n}"
  rx_if[i]="fr${short_tag}${n}"
  tx_ip[i]="198.18.${n}.1"
  rx_ip[i]="198.18.${n}.2"
  port[i]=$((base_port + i))
  out_shm[i]="/sc_fnet_out_${tag}_${n}"
  target[i]="${rx_ip[i]}:${port[i]}"
  reserve_shm "/dev/shm/${out_shm[i]#/}"
  made_out_shm[i]=1
  mkdir -p "$output/rx$n"
  printf 'link_%s_tx_ip=%q\nlink_%s_rx_ip=%q\nlink_%s_port=%q\nlink_%s_loss_pct=%q\nlink_%s_netem_seed=%q\n' "$n" "${tx_ip[i]}" "$n" "${rx_ip[i]}" "$n" "${port[i]}" "$n" "${link_loss[i]}" "$n" "$([[ -n $netem_seed ]] && printf '%s' "$((netem_seed + i))" || printf kernel-random)" >>"$manifest"
  sudo -n ip netns add "${rx_ns[i]}" >>"$setup_log" 2>&1; made_rx_ns[i]=1
  sudo -n ip link add "${tx_if[i]}" type veth peer name "${rx_if[i]}" >>"$setup_log" 2>&1
  sudo -n ip link set "${tx_if[i]}" netns "$tx_ns" >>"$setup_log" 2>&1
  sudo -n ip link set "${rx_if[i]}" netns "${rx_ns[i]}" >>"$setup_log" 2>&1
  sudo -n ip -n "$tx_ns" addr add "${tx_ip[i]}/30" dev "${tx_if[i]}" >>"$setup_log" 2>&1
  sudo -n ip -n "${rx_ns[i]}" addr add "${rx_ip[i]}/30" dev "${rx_if[i]}" >>"$setup_log" 2>&1
  sudo -n ip -n "$tx_ns" link set "${tx_if[i]}" up >>"$setup_log" 2>&1
  sudo -n ip -n "${rx_ns[i]}" link set lo up >>"$setup_log" 2>&1
  sudo -n ip -n "${rx_ns[i]}" link set "${rx_if[i]}" up >>"$setup_log" 2>&1
  if [[ $loss_model == random ]]; then netem=(loss random "${link_loss[i]}%"); else netem=(loss gemodel "${link_loss[i]}%" "${loss_ge_r}%" "${loss_ge_bad}%" "${loss_ge_good}%"); fi
  ((delay_us > 0)) && netem+=(delay "${delay_us}us")
  ((delay_jitter_us > 0)) && netem+=("${delay_jitter_us}us")
  awk -v r="$reorder_pct" 'BEGIN { exit !(r > 0) }' && netem+=(reorder "${reorder_pct}%")
  [[ -n $netem_seed ]] && netem+=(seed "$((netem_seed + i))")
  sudo -n ip netns exec "$tx_ns" tc qdisc add dev "${tx_if[i]}" root netem "${netem[@]}" >>"$setup_log" 2>&1
  sudo -n ip netns exec "$tx_ns" tc qdisc show dev "${tx_if[i]}" >>"$setup_log" 2>&1
done

for ((i = 0; i < receivers; ++i)); do
  n=$((i + 1))
  run_netns "${rx_ns[i]}" "${receiver_cpu[i]:-}" "$harness/bin/udp_receiver" --out-shm "${out_shm[i]}" --out-slots "$slots" --bind "${rx_ip[i]}" --port "${port[i]}" --count "$count" --idle-ms "$idle_ms" --session-id "$session_id" --fec "$fec" --wire-format "$wire_format" --batch-bytes "$batch_bytes" --reorder-wait-us "$reorder_wait_us" --recv-batch "$recv_batch" --socket-buffer "$socket_buffer" --profile-sample-every "$profile_sample_every" >"$output/rx$n/receiver.log" 2>&1 & receiver_pid[i]=$!
  wait_ready "$output/rx$n/receiver.log" 'udp_receiver: ready ' "receiver $n"
  run_host "${consumer_cpu[i]:-}" "$harness/bin/consumer" --shm "${out_shm[i]}" --slots "$slots" --count "$count" --idle-ms "$idle_ms" --from-edge --profile-sample-every "$profile_sample_every" --csv "$output/rx$n/consumer.csv" >"$output/rx$n/consumer.log" 2>&1 & consumer_pid[i]=$!
  wait_mapping "${consumer_pid[i]}" "${out_shm[i]}" "consumer $n"
done
sender_common=(--in-shm "$in_shm" --in-slots "$slots" --count "$count" --wait-ms 10000 --idle-ms "$idle_ms" --session-id "$session_id" --fec "$fec" --wire-format "$wire_format" --udp-mode "$udp_mode" --batch-bytes "$batch_bytes" --socket-buffer "$socket_buffer" --profile-sample-every "$profile_sample_every")
((sender_workers_auto)) && sender_common+=(--sender-workers auto)
if [[ $sender_topology == shared ]]; then
  sender_args=("${sender_common[@]}" --fanout-mode "$fanout_mode")
  for ((i = 0; i < receivers; ++i)); do sender_args+=(--target "${target[i]}"); done
  run_netns "$tx_ns" "$cpu_sender" "$harness/bin/udp_sender" "${sender_args[@]}" >"$output/sender.log" 2>&1 & sender_pid[0]=$!
else
  for ((i = 0; i < sender_workers; ++i)); do
    sender_args=("${sender_common[@]}" --fanout-mode loop)
    for ((target_index = i; target_index < receivers; target_index += sender_workers)); do sender_args+=(--target "${target[target_index]}"); done
    run_netns "$tx_ns" "${sender_cpu[i]:-}" "$harness/bin/udp_sender" "${sender_args[@]}" >"$output/sender$((i + 1)).log" 2>&1 & sender_pid[i]=$!
  done
fi
run_host "$cpu_producer" "$harness/bin/producer" --shm "$in_shm" --slots "$slots" --count "$count" --rate "$rate" --burst-size "$producer_burst_size" --type "$kind" >"$output/producer.log" 2>&1 & producer_pid=$!

effective_rate=$rate; ((effective_rate == 0)) && effective_rate=100000
timeout_s=$((idle_ms / 1000 + count / effective_rate + 30)); ((timeout_s < 60)) && timeout_s=60
( sleep "$timeout_s"; kill "$producer_pid" "${sender_pid[@]}" "${receiver_pid[@]}" "${consumer_pid[@]}" 2>/dev/null || true ) & watchdog_pid=$!
deadline=$((SECONDS + timeout_s))
wait_exit "$producer_pid" producer "$deadline"
for ((i = 0; i < ${#sender_pid[@]}; ++i)); do wait_exit "${sender_pid[i]}" "sender $((i + 1))" "$deadline"; done
for ((i = 0; i < receivers; ++i)); do wait_exit "${receiver_pid[i]}" "receiver $((i + 1))" "$deadline"; wait_exit "${consumer_pid[i]}" "consumer $((i + 1))" "$deadline"; done
kill "$watchdog_pid" 2>/dev/null || true; watchdog_pid=

counter() { sed -n "s/.*[[:space:]]$2=\\([0-9][0-9]*\\).*/\\1/p" "$1" | tail -n 1; }
sender_sent=0
sender_drops=0
sender_lapped=0
sender_invalid=0
for ((i = 0; i < ${#sender_pid[@]}; ++i)); do
  sender_log="$output/sender.log"; [[ $sender_topology == per-receiver ]] && sender_log="$output/sender$((i + 1)).log"
  value=$(counter "$sender_log" sent); [[ $value =~ ^[0-9]+$ ]] || value=0; sender_sent=$((sender_sent + value))
  value=$(counter "$sender_log" socket_drops); [[ $value =~ ^[0-9]+$ ]] || value=0; sender_drops=$((sender_drops + value))
  value=$(counter "$sender_log" input_lapped); [[ $value =~ ^[0-9]+$ ]] || value=0; sender_lapped=$((sender_lapped + value))
  value=$(counter "$sender_log" invalid); [[ $value =~ ^[0-9]+$ ]] || value=0; sender_invalid=$((sender_invalid + value))
done
status=pass
reason=
expected_sender_sent=$count; [[ $sender_topology == per-receiver ]] && expected_sender_sent=$((count * sender_workers))
[[ $sender_sent == "$expected_sender_sent" ]] || { status=fail; reason='sender did not send every input frame'; }
[[ $sender_drops =~ ^[0-9]+$ && $sender_drops == 0 && $sender_lapped == 0 && $sender_invalid == 0 ]] || { status=fail; reason='sender reported a local failure'; }
declare -a receiver_first receiver_accepted receiver_recovered receiver_invalid receiver_mismatch consumer_received consumer_dropped consumer_p50 consumer_p99 consumer_p999 consumer_p9999
for ((i = 0; i < receivers; ++i)); do
  n=$((i + 1)); rlog="$output/rx$n/receiver.log"; clog="$output/rx$n/consumer.log"
  receiver_first[i]=$(counter "$rlog" first_seq)
  receiver_accepted[i]=$(counter "$rlog" accepted)
  receiver_recovered[i]=$(counter "$rlog" fec_recovered)
  receiver_invalid[i]=$(counter "$rlog" invalid)
  receiver_mismatch[i]=$(counter "$rlog" session_mismatch)
  consumer_received[i]=$(sed -n 's/^received     : \([0-9][0-9]*\)$/\1/p' "$clog" | tail -n 1)
  consumer_dropped[i]=$(sed -n 's/^dropped      : \([0-9][0-9]*\)$/\1/p' "$clog" | tail -n 1)
  consumer_p50[i]=$(sed -n 's/^  p50        : \([0-9][0-9]*\)$/\1/p' "$clog" | tail -n 1)
  consumer_p99[i]=$(sed -n 's/^  p99        : \([0-9][0-9]*\)$/\1/p' "$clog" | tail -n 1)
  consumer_p999[i]=$(sed -n 's/^  p99.9      : \([0-9][0-9]*\)$/\1/p' "$clog" | tail -n 1)
  consumer_p9999[i]=$(sed -n 's/^  p99.99     : \([0-9][0-9]*\)$/\1/p' "$clog" | tail -n 1)
  [[ ${receiver_invalid[i]} =~ ^[0-9]+$ && ${receiver_invalid[i]} == 0 && ${receiver_mismatch[i]} == 0 ]] || { status=fail; reason="receiver $n reported an invalid packet or session mismatch"; }
  [[ ${receiver_first[i]} =~ ^[0-9]+$ && ${receiver_first[i]} -le 64 ]] || { status=fail; reason="receiver $n started too late at seq_id ${receiver_first[i]:-unparsed}"; }
  if [[ ${link_loss[i]} == 0 && ${receiver_accepted[i]} != "$count" ]]; then
    status=fail; reason="0%-loss receiver $n accepted ${receiver_accepted[i]:-unparsed} of $count"
  fi
done
{
  printf 'status=%s\n' "$status"
  [[ -n $reason ]] && printf 'reason=%s\n' "$reason"
  printf 'sender_sent=%s\nsender_socket_drops=%s\nsender_input_lapped=%s\nsender_invalid=%s\n' "${sender_sent:-unparsed}" "${sender_drops:-unparsed}" "${sender_lapped:-unparsed}" "${sender_invalid:-unparsed}"
  for ((i = 0; i < receivers; ++i)); do
    n=$((i + 1))
    printf 'receiver_%s_loss_pct=%s\nreceiver_%s_first_seq=%s\nreceiver_%s_accepted=%s\nreceiver_%s_residual_missing=%s\nreceiver_%s_fec_recovered=%s\nreceiver_%s_invalid=%s\nreceiver_%s_session_mismatch=%s\nreceiver_%s_consumer_received=%s\nreceiver_%s_consumer_dropped=%s\nreceiver_%s_latency_p50_ns=%s\nreceiver_%s_latency_p99_ns=%s\nreceiver_%s_latency_p999_ns=%s\nreceiver_%s_latency_p9999_ns=%s\n' "$n" "${link_loss[i]}" "$n" "${receiver_first[i]:-unparsed}" "$n" "${receiver_accepted[i]:-unparsed}" "$n" "$([[ ${receiver_accepted[i]} =~ ^[0-9]+$ ]] && printf '%s' "$((count - receiver_accepted[i]))" || printf unparsed)" "$n" "${receiver_recovered[i]:-unparsed}" "$n" "${receiver_invalid[i]:-unparsed}" "$n" "${receiver_mismatch[i]:-unparsed}" "$n" "${consumer_received[i]:-unparsed}" "$n" "${consumer_dropped[i]:-unparsed}" "$n" "${consumer_p50[i]:-unparsed}" "$n" "${consumer_p99[i]:-unparsed}" "$n" "${consumer_p999[i]:-unparsed}" "$n" "${consumer_p9999[i]:-unparsed}"
  done
  sha256sum "$manifest" "$setup_log" "$output/producer.log" "$output"/sender*.log "$output"/rx*/receiver.log "$output"/rx*/consumer.log
} >"$output/result.txt"

if [[ $status != pass ]]; then die "run failed; inspect $output"; fi
note 'PASS — nonzero configured loss is reported per receiver, not treated as a harness failure'
printf 'results=%s\n' "$output"
