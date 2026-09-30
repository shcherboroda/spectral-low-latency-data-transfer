#!/usr/bin/env bash
# Plan and, only with --execute, run a comparable single-receiver UDP matrix.
set -Eeuo pipefail
IFS=$'\n\t'

usage() {
  cat <<'EOF'
Usage: harness/bench/run_udp_matrix.sh [options]

Plans a single-receiver UDP recovery matrix through run_udp_netem.sh.
It is dry-run by default; --execute is deliberately required to start load.

  --profile NAME      short|tail|recovery (default: short)
  --count N           Messages per run (default: 500000)
  --warmup-ms N       Stream warm-up before each measurement (default: 500)
  --repeats N         Repetitions per scenario (default: 3)
  --slots N           SHM slots (default: 65536)
  --cpus P,S,R,C      Optional CPU ids for producer,sender,receiver,consumer
  --netem-seed N      First loss seed; each repeat uses the next seed (default: 424242)
  --output DIR        New matrix directory, required with --execute
  --execute           Run the planned matrix (otherwise print it only)
  --help              Show this help

short: raw/FEC at 50k msg/s with 0% and 0.1% loss.
tail:  raw at 20k/50k/100k/200k with 0% loss, plus raw/FEC at 50k with
       0.1% and 1% loss. Use only when the machine is otherwise idle.

recovery: connected, packed 50k msg/s candidate comparison: full-frame XOR4,
          full-frame RS(4+2), and compact packet-XOR4 under independent and
          Gilbert-Elliott burst loss. This is the recommended profile for
          comparing delivery against tail latency.
EOF
}

die() { printf 'udp matrix: %s\n' "$*" >&2; exit 2; }

profile=short
count=500000
warmup_ms=500
repeats=3
slots=65536
cpus=
seed=424242
output=
execute=0
while (($#)); do
  case "$1" in
    --profile) profile=${2-}; shift 2 ;;
    --count) count=${2-}; shift 2 ;;
    --warmup-ms) warmup_ms=${2-}; shift 2 ;;
    --repeats) repeats=${2-}; shift 2 ;;
    --slots) slots=${2-}; shift 2 ;;
    --cpus) cpus=${2-}; shift 2 ;;
    --netem-seed) seed=${2-}; shift 2 ;;
    --output) output=${2-}; shift 2 ;;
    --execute) execute=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done
[[ $count =~ ^[1-9][0-9]*$ && $warmup_ms =~ ^[0-9]+$ && $repeats =~ ^[1-9][0-9]*$ && $seed =~ ^[1-9][0-9]*$ ]] || die 'count, warmup-ms, repeats, and seed must be non-negative/positive integers'
[[ $slots =~ ^[0-9]+$ ]] && ((slots > 0 && (slots & (slots - 1)) == 0)) || die '--slots must be a power of two'
case "$profile" in short|tail|recovery) ;; *) die '--profile must be short, tail, or recovery' ;; esac
((execute == 0)) || [[ -n $output ]] || die '--output is required with --execute'
if [[ -n $cpus ]]; then
  IFS=, read -r cpu_producer cpu_sender cpu_receiver cpu_consumer extra <<<"$cpus"
  [[ -n ${cpu_producer:-} && -n ${cpu_sender:-} && -n ${cpu_receiver:-} && -n ${cpu_consumer:-} && -z ${extra:-} ]] || die '--cpus needs exactly four comma-separated CPU ids'
  for cpu in "$cpu_producer" "$cpu_sender" "$cpu_receiver" "$cpu_consumer"; do [[ $cpu =~ ^[0-9]+$ ]] || die '--cpus entries must be integers'; done
fi

declare -a scenarios=()
if [[ $profile == short ]]; then
  scenarios=('50000 none full 0 random' '50000 xor8 full 0 random' '50000 none full 0.1 random' '50000 xor8 full 0.1 random')
elif [[ $profile == tail ]]; then
  scenarios=('20000 none full 0 random' '50000 none full 0 random' '100000 none full 0 random' '200000 none full 0 random'
             '50000 none full 0.1 random' '50000 xor8 full 0.1 random' '50000 none full 1 random' '50000 xor8 full 1 random')
else
  scenarios=('50000 xor4 full 0.1 random' '50000 rs4_2 full 0.1 random' '50000 packet_xor4 compact 0.1 random'
             '50000 xor4 full 0.5 random' '50000 rs4_2 full 0.5 random' '50000 packet_xor4 compact 0.5 random'
             '50000 xor4 full 0.1 gemodel' '50000 rs4_2 full 0.1 gemodel' '50000 packet_xor4 compact 0.1 gemodel')
fi

repo_root=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
runner="$repo_root/harness/bench/run_udp_netem.sh"
[[ -x $runner ]] || die "missing runner: $runner"

printf 'profile=%s count=%s repeats=%s slots=%s seed=%s cpus=%s mode=%s\n' "$profile" "$count" "$repeats" "$slots" "$seed" "${cpus:-unassigned}" "$([[ $execute == 1 ]] && echo execute || echo dry-run)"
for ((rep = 1; rep <= repeats; ++rep)); do
  for scenario in "${scenarios[@]}"; do
    IFS=' ' read -r rate fec wire_format loss loss_model <<<"$scenario"
    label="rate-${rate}_fec-${fec}_wire-${wire_format}_loss-${loss}_model-${loss_model}_rep-${rep}"
    printf '%s\n' "$label"
  done
done
((execute == 1)) || exit 0

[[ ! -e $output ]] || die '--output must be a new directory'
mkdir -p -- "$output"
output=$(CDPATH= cd -- "$output" && pwd)
printf 'label\trate\tfec\twire_format\tloss_pct\tloss_model\trepeat\tnetem_seed\tstatus\tp50_ns\tp99_ns\tp999_ns\tp9999_ns\tconsumer_received\tmeasurement_missing\tconsumer_internal_seq_gaps\tfec_recovered\n' >"$output/matrix.tsv"
{
  printf 'git_revision=%s\n' "$(git -c safe.directory="$repo_root" -C "$repo_root" rev-parse HEAD)"
  printf 'profile=%q\ncount=%q\nwarmup_ms=%q\nrepeats=%q\nslots=%q\nnetem_seed=%q\ncpus=%q\n' "$profile" "$count" "$warmup_ms" "$repeats" "$slots" "$seed" "$cpus"
} >"$output/manifest.txt"

metric() {
  local log=$1 label=$2
  sed -n "s/^[[:space:]]*$label *: *\([0-9][0-9]*\)$/\1/p" "$log" | tail -n 1
}
value() {
  local result=$1 key=$2
  sed -n "s/^${key}=\(.*\)$/\1/p" "$result" | tail -n 1
}

for ((rep = 1; rep <= repeats; ++rep)); do
  repeat_seed=$((seed + rep - 1))
  # Rotate the deterministic order between repeats so a particular profile is
  # not always measured first on an otherwise changing host.
  for ((offset = 0; offset < ${#scenarios[@]}; ++offset)); do
    scenario=${scenarios[$(((rep - 1 + offset) % ${#scenarios[@]}))]}
    IFS=' ' read -r rate fec wire_format loss loss_model <<<"$scenario"
    label="rate-${rate}_fec-${fec}_wire-${wire_format}_loss-${loss}_model-${loss_model}_rep-${rep}"
    run_dir="$output/$label"
    args=(--loss "$loss" --loss-model "$loss_model" --fec "$fec" --wire-format "$wire_format" --udp-mode connected --batch-bytes 1472 --netem-seed "$repeat_seed" --count "$count" --rate "$rate" --warmup-ms "$warmup_ms" --slots "$slots" --output "$run_dir")
    if [[ $profile == recovery ]]; then
      args+=(--reorder-wait-us auto)
    fi
    if [[ -n $cpus ]]; then
      args+=(--cpu-producer "$cpu_producer" --cpu-sender "$cpu_sender" --cpu-receiver "$cpu_receiver" --cpu-consumer "$cpu_consumer")
    fi
    "$runner" "${args[@]}"
    result="$run_dir/result.txt"
    status=$(value "$result" status || true)
    p50=$(metric "$run_dir/consumer.log" p50 || true)
    p99=$(metric "$run_dir/consumer.log" p99 || true)
    p999=$(metric "$run_dir/consumer.log" p99.9 || true)
    p9999=$(metric "$run_dir/consumer.log" p99.99 || true)
    received=$(value "$result" consumer_received || true)
    missing=$(value "$result" measurement_missing_total || true)
    internal_gaps=$(value "$result" consumer_internal_seq_gaps || true)
    recovered=$(value "$result" receiver_fec_recovered || true)
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$label" "$rate" "$fec" "$wire_format" "$loss" "$loss_model" "$rep" "$repeat_seed" "${status:-unknown}" "${p50:-}" "${p99:-}" "${p999:-}" "${p9999:-}" "${received:-}" "${missing:-}" "${internal_gaps:-}" "${recovered:-}" >>"$output/matrix.tsv"
  done
done
printf 'results=%s\n' "$output"
