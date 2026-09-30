#!/usr/bin/env bash
# Measure the fixed producer->consumer shared-memory path after a warm-up.
# This is a WSL/Linux noise-floor control, not a transport benchmark.
set -Eeuo pipefail
IFS=$'\n\t'

usage() {
  cat <<'EOF'
Usage: harness/bench/run_shm_baseline.sh [options]

Runs an unbounded fixed producer, waits for warm-up, then samples exactly N
messages with a fixed consumer starting at the live edge.

Options:
  --count N             Consumer sample size (default: 1000000)
  --rate MSGS_PER_SEC   Producer rate, > 0 (default: 50000)
  --type TYPE           trade|bbo|book|mixed (default: mixed)
  --slots N             Power-of-two SHM slots (default: 65536)
  --warmup-s N          Whole seconds before sampling (default: 2)
  --idle-ms MS          Consumer idle stop interval (default: 3000)
  --cpu-producer CPUSET taskset CPU set for producer (optional)
  --cpu-consumer CPUSET taskset CPU set for consumer (optional)
  --rt-priority N        SCHED_FIFO priority 1..99; 0 disables it (default: 0)
  --consumer-csv        Write consumer.csv
  --output DIR          New directory for logs and result (default: unique
                        directory below harness/results)
  --run-timeout-s N     Sampling deadline after consumer starts (default: 60)
  --dry-run             Validate arguments and print the planned run
  --help                Show this help
EOF
}

die() { printf 'shm baseline: %s\n' "$*" >&2; exit 2; }
note() { printf 'shm baseline: %s\n' "$*" >&2; }

count=1000000
rate=50000
kind=mixed
slots=65536
warmup_s=2
idle_ms=3000
cpu_producer=
cpu_consumer=
rt_priority=0
consumer_csv=0
output_dir=
run_timeout_s=60
dry_run=0

while (($#)); do
  case "$1" in
    --count) count=${2-}; shift 2 ;;
    --rate) rate=${2-}; shift 2 ;;
    --type) kind=${2-}; shift 2 ;;
    --slots) slots=${2-}; shift 2 ;;
    --warmup-s) warmup_s=${2-}; shift 2 ;;
    --idle-ms) idle_ms=${2-}; shift 2 ;;
    --cpu-producer) cpu_producer=${2-}; shift 2 ;;
    --cpu-consumer) cpu_consumer=${2-}; shift 2 ;;
    --rt-priority) rt_priority=${2-}; shift 2 ;;
    --consumer-csv) consumer_csv=1; shift ;;
    --output) output_dir=${2-}; shift 2 ;;
    --run-timeout-s) run_timeout_s=${2-}; shift 2 ;;
    --dry-run) dry_run=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done

is_uint() { [[ $1 =~ ^[0-9]+$ ]]; }
is_number() { [[ $1 =~ ^([0-9]+([.][0-9]*)?|[.][0-9]+)$ ]]; }
is_uint "$count" && ((count > 0)) || die "--count must be a positive integer"
is_number "$rate" || die "--rate must be a positive number"
awk -v rate="$rate" 'BEGIN { exit !(rate > 0) }' || die "--rate must be positive"
case "$kind" in trade|bbo|book|mixed) ;; *) die "--type must be trade|bbo|book|mixed" ;; esac
is_uint "$slots" && ((slots > 0 && (slots & (slots - 1)) == 0)) || die "--slots must be a power of two"
is_uint "$warmup_s" || die "--warmup-s must be a non-negative integer"
is_uint "$idle_ms" && ((idle_ms > 0)) || die "--idle-ms must be positive"
is_uint "$run_timeout_s" && ((run_timeout_s > 0)) || die "--run-timeout-s must be positive"
is_uint "$rt_priority" && ((rt_priority <= 99)) || die "--rt-priority must be an integer from 0 through 99"
awk -v rate="$rate" -v idle_ms="$idle_ms" 'BEGIN {
  exit !(rate * idle_ms >= 2000)
}' || die "--idle-ms must span at least two message intervals at the configured --rate"

repo_root=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
harness_dir="$repo_root/harness"
for tool in awk date git grep mktemp sed sha256sum sleep taskset; do
  command -v "$tool" >/dev/null 2>&1 || die "required tool is missing: $tool"
done
if ((rt_priority > 0)); then
  for tool in chrt lscpu nproc ps sudo tr; do
    command -v "$tool" >/dev/null 2>&1 || die "--rt-priority requires: $tool"
  done
fi
for binary in producer consumer; do
  [[ -x "$harness_dir/bin/$binary" ]] || die "missing executable: $harness_dir/bin/$binary (run: make -C harness)"
done

run_tag="$(date -u +%Y%m%dT%H%M%SZ)-$$"
shm_name="/sc_shm_baseline_$run_tag"
shm_path="/dev/shm/${shm_name#/}"
controller_cpu=

if ((dry_run)); then
  cat <<EOF
dry run: no processes will be started
  shared memory: $shm_name
  count:         $count
  rate:          $rate msg/s
  warm-up:       $warmup_s s
EOF
  exit 0
fi

if [[ -n "$output_dir" ]]; then
  [[ ! -e "$output_dir" ]] || die "--output must not already exist: $output_dir"
  mkdir -p -- "$output_dir"
else
  mkdir -p -- "$harness_dir/results"
  output_dir=$(mktemp -d "$harness_dir/results/shm-baseline.XXXXXX")
fi
output_dir=$(CDPATH= cd -- "$output_dir" && pwd)
manifest="$output_dir/manifest.txt"
producer_log="$output_dir/producer.log"
consumer_log="$output_dir/consumer.log"
result_file="$output_dir/result.txt"

producer_pid=
consumer_pid=
created_shm=0

cleanup() {
  local rc=$?
  set +e
  for pid in "$producer_pid" "$consumer_pid"; do
    [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null && kill -TERM "$pid" 2>/dev/null
  done
  for pid in "$producer_pid" "$consumer_pid"; do
    [[ -n "$pid" ]] && wait "$pid" 2>/dev/null
  done
  ((created_shm)) && rm -f -- "$shm_path"
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

reserve_shm() {
  (set -o noclobber; : >"$shm_path") 2>/dev/null ||
    die "refusing to reuse existing SHM object: $shm_path"
}

wait_for_producer_ready() {
  local deadline=$((SECONDS + 10))
  while ((SECONDS < deadline)); do
    # producer prints this only after Segment::open() and Ring::attach(init=true)
    # completed, so unlike SHM file size it cannot race slot initialization.
    grep -Fq -- "producer: shm=$shm_name " "$producer_log" 2>/dev/null && return 0
    kill -0 "$producer_pid" 2>/dev/null || { note "producer exited before creating SHM"; return 1; }
    sleep 0.01
  done
  note "timed out waiting for producer initialization"
  return 1
}

wait_for_exit() {
  local pid=$1 label=$2 deadline=$3
  while kill -0 "$pid" 2>/dev/null; do
    ((SECONDS < deadline)) || { note "$label exceeded run deadline"; return 1; }
    sleep 0.05
  done
  if ! wait "$pid"; then
    note "$label exited unsuccessfully"
    return 1
  fi
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

apply_rt_priority() {
  local pid=$1 label=$2 actual
  ((rt_priority == 0)) && return 0
  sudo -n chrt -f --pid "$rt_priority" "$pid" >/dev/null ||
    die "could not set SCHED_FIFO priority $rt_priority for $label"
  actual=$(ps -o rtprio= -p "$pid" | tr -d '[:space:]')
  [[ "$actual" == "$rt_priority" ]] || die "$label does not have requested SCHED_FIFO priority $rt_priority (actual: ${actual:-none})"
}

reserve_shm
created_shm=1
reserve_rt_layout "$cpu_producer" "$cpu_consumer"
{
  printf 'started_utc=%s\n' "$(date -u +%FT%TZ)"
  printf 'repo_root=%q\n' "$repo_root"
  printf 'git_revision=%s\n' "$(git -c safe.directory="$repo_root" -C "$repo_root" rev-parse HEAD 2>/dev/null || printf unknown)"
  printf 'count=%q\nrate=%q\ntype=%q\nslots=%q\nwarmup_s=%q\nidle_ms=%q\n' "$count" "$rate" "$kind" "$slots" "$warmup_s" "$idle_ms"
  printf 'cpu_producer=%q\ncpu_consumer=%q\nrt_priority=%q\ncontroller_cpu=%q\nshm=%q\nconsumer_csv=%q\n' "$cpu_producer" "$cpu_consumer" "$rt_priority" "$controller_cpu" "$shm_name" "$consumer_csv"
} >"$manifest"

note "warming producer for $warmup_s s (results: $output_dir)"
run_with_cpu "$cpu_producer" "$harness_dir/bin/producer" --shm "$shm_name" --slots "$slots" \
  --count 0 --rate "$rate" --type "$kind" >"$producer_log" 2>&1 & producer_pid=$!
wait_for_producer_ready
apply_rt_priority "$producer_pid" producer
sleep "$warmup_s"

consumer_args=("$harness_dir/bin/consumer" --shm "$shm_name" --slots "$slots" --from-edge \
  --count "$count" --idle-ms "$idle_ms")
if ((consumer_csv)); then consumer_args+=(--csv "$output_dir/consumer.csv"); fi
run_with_cpu "$cpu_consumer" "${consumer_args[@]}" >"$consumer_log" 2>&1 & consumer_pid=$!
apply_rt_priority "$consumer_pid" consumer
run_deadline=$((SECONDS + run_timeout_s))
wait_for_exit "$consumer_pid" consumer "$run_deadline"

received=$(sed -n 's/^received     : \([0-9][0-9]*\)$/\1/p' "$consumer_log" | tail -n 1)
dropped=$(sed -n 's/^dropped      : \([0-9][0-9]*\)$/\1/p' "$consumer_log" | tail -n 1)
lapped=$(sed -n 's/^consumer: lapped \([0-9][0-9]*\) times$/\1/p' "$consumer_log" | tail -n 1)
for value in received dropped lapped; do
  [[ ${!value} =~ ^[0-9]+$ ]] || die "could not parse $value; inspect $output_dir"
done

failure=
((received == count)) || failure="consumer received $received of $count requested samples"
((dropped == 0)) || failure="consumer observed sequence drops ($dropped)"
((lapped == 0)) || failure="consumer SHM lapped ($lapped)"
{
  if [[ -n "$failure" ]]; then printf 'status=fail\nreason=%s\n' "$failure"; else printf 'status=pass\n'; fi
  printf 'consumer_received=%s\nconsumer_dropped=%s\nconsumer_lapped=%s\n' "$received" "$dropped" "$lapped"
  sha256sum "$manifest" "$producer_log" "$consumer_log"
} >"$result_file"

[[ -z "$failure" ]] || die "$failure"
note "PASS — steady-state SHM noise-floor sample captured"
printf 'results=%s\n' "$output_dir"
