#!/usr/bin/env bash
# Execute a small, repeatable fan-out recovery matrix using independent links.
set -Eeuo pipefail
IFS=$'\n\t'

usage() {
  cat <<'EOF'
Usage: harness/bench/run_udp_fanout_matrix.sh [options]

Runs a three-receiver fan-out matrix through run_udp_fanout_netem.sh.
It compares no recovery, XOR(4), and Reed-Solomon(4+2) under random and
Gilbert-Elliott losses.  --execute is required to create load.

  --count N          Messages per run (default: 50000)
  --rate N           Messages/sec per run (default: 50000)
  --repeats N        Repetitions of every scenario (default: 2)
  --losses PCTS      Three independent receiver loss percentages (default: 0.1,0.5,1.0)
  --fec-modes MODES  Comma-separated modes (default: none,xor4,rs4_2)
  --cpus LIST        P,S,R1,R2,R3,C1,C2,C3 CPU ids; optional role pinning
  --auto-workers     Use udp_sender's built-in worker selection
  --seed N           Base netem seed (default: 12000)
  --output DIR       New output directory; required with --execute
  --execute          Execute the matrix (otherwise only print its plan)
  --help             Show this help

All cases use full-frame packing, connected UDP sockets, and a 25us bounded
resequencing wait. Results are written to matrix.tsv and per-run directories.
EOF
}

die() { printf 'fanout matrix: %s\n' "$*" >&2; exit 2; }
value() { sed -n "s/^$2=//p" "$1" | tail -n 1; }

count=50000
rate=50000
repeats=2
losses=0.1,0.5,1.0
fec_modes=none,xor4,rs4_2
cpus=
auto_workers=0
seed=12000
output=
execute=0
while (($#)); do
  case "$1" in
    --count) count=${2-}; shift 2 ;;
    --rate) rate=${2-}; shift 2 ;;
    --repeats) repeats=${2-}; shift 2 ;;
    --losses) losses=${2-}; shift 2 ;;
    --fec-modes) fec_modes=${2-}; shift 2 ;;
    --cpus) cpus=${2-}; shift 2 ;;
    --auto-workers) auto_workers=1; shift ;;
    --seed) seed=${2-}; shift 2 ;;
    --output) output=${2-}; shift 2 ;;
    --execute) execute=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done
[[ $count =~ ^[1-9][0-9]*$ && $rate =~ ^[0-9]+$ && $repeats =~ ^[1-9][0-9]*$ && $seed =~ ^[1-9][0-9]*$ ]] || die 'count, rate, repeats, and seed are invalid'
IFS=, read -r -a loss_values <<<"$losses"
((${#loss_values[@]} == 3)) || die '--losses must contain exactly three comma-separated values'
for loss in "${loss_values[@]}"; do
  [[ $loss =~ ^([0-9]+([.][0-9]*)?|[.][0-9]+)$ ]] || die '--losses values must be numeric'
  awk -v v="$loss" 'BEGIN { exit !(v >= 0 && v <= 100) }' || die '--losses values must be from 0 through 100'
done
IFS=, read -r -a selected_fec <<<"$fec_modes"
((${#selected_fec[@]} > 0)) || die '--fec-modes must not be empty'
for fec in "${selected_fec[@]}"; do
  case "$fec" in none|xor4|xor8|rs4_2|rs8_2) ;; *) die "invalid FEC mode: $fec" ;; esac
done
declare -a cpu_list
if [[ -n $cpus ]]; then
  IFS=, read -r -a cpu_list <<<"$cpus"
  ((${#cpu_list[@]} == 8)) || die '--cpus requires P,S,R1,R2,R3,C1,C2,C3'
  for cpu in "${cpu_list[@]}"; do [[ $cpu =~ ^[0-9]+$ ]] || die '--cpus entries must be non-negative integers'; done
fi
((execute == 0)) || [[ -n $output ]] || die '--output is required with --execute'

repo_root=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
runner="$repo_root/harness/bench/run_udp_fanout_netem.sh"
[[ -x $runner ]] || die "missing runner: $runner"
printf 'count=%s rate=%s repeats=%s losses=%s fec_modes=%s cpus=%s base_seed=%s mode=%s\n' "$count" "$rate" "$repeats" "$losses" "$fec_modes" "${cpus:-unassigned}" "$seed" "$([[ $execute == 1 ]] && printf execute || printf plan)"
for ((rep = 1; rep <= repeats; ++rep)); do
  for model in random gemodel; do
    for fec in "${selected_fec[@]}"; do printf 'model=%s fec=%s repeat=%s seed=%s\n' "$model" "$fec" "$rep" "$((seed + rep * 100))"; done
  done
done
((execute == 1)) || exit 0

[[ ! -e $output ]] || die "--output already exists: $output"
mkdir -p -- "$output"
output=$(CDPATH= cd -- "$output" && pwd)
{
  printf 'git_revision=%s\n' "$(git -c safe.directory="$repo_root" -C "$repo_root" rev-parse HEAD)"
  printf 'count=%s\nrate=%s\nrepeats=%s\nlosses=%s\nfec_modes=%s\ncpus=%s\nauto_workers=%s\nbase_seed=%s\n' "$count" "$rate" "$repeats" "$losses" "$fec_modes" "${cpus:-unassigned}" "$auto_workers" "$seed"
  printf 'udp_mode=connected\nfanout_mode=loop\nbatch_bytes=1472\nreorder_wait_us=auto\n'
} >"$output/manifest.txt"
printf 'model\tfec\trepeat\tseed\tstatus\trx1_missing\trx2_missing\trx3_missing\trx1_recovered\trx2_recovered\trx3_recovered\trx1_p9999_ns\trx2_p9999_ns\trx3_p9999_ns\n' >"$output/matrix.tsv"

for ((rep = 1; rep <= repeats; ++rep)); do
  run_seed=$((seed + rep * 100))
  for model in random gemodel; do
    for fec in "${selected_fec[@]}"; do
      label="model-${model}_fec-${fec}_rep-${rep}"
      runner_args=(--receivers 3 --losses "$losses" --loss-model "$model" --netem-seed "$run_seed"
        --fec "$fec" --batch-bytes 1472 --udp-mode connected --fanout-mode loop
        --reorder-wait-us 25 --count "$count" --rate "$rate" --idle-ms 3000
        --output "$output/$label")
      ((auto_workers)) && runner_args+=(--sender-workers-auto)
      if [[ -n $cpus ]]; then
        runner_args+=(--cpu-producer "${cpu_list[0]}" --cpu-sender "${cpu_list[1]}"
          --cpu-receivers "${cpu_list[2]},${cpu_list[3]},${cpu_list[4]}"
          --cpu-consumers "${cpu_list[5]},${cpu_list[6]},${cpu_list[7]}")
      fi
      "$runner" "${runner_args[@]}"
      result="$output/$label/result.txt"
      printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$model" "$fec" "$rep" "$run_seed" "$(value "$result" status)" \
        "$(value "$result" receiver_1_residual_missing)" "$(value "$result" receiver_2_residual_missing)" "$(value "$result" receiver_3_residual_missing)" \
        "$(value "$result" receiver_1_fec_recovered)" "$(value "$result" receiver_2_fec_recovered)" "$(value "$result" receiver_3_fec_recovered)" \
        "$(value "$result" receiver_1_latency_p9999_ns)" "$(value "$result" receiver_2_latency_p9999_ns)" "$(value "$result" receiver_3_latency_p9999_ns)" >>"$output/matrix.tsv"
    done
  done
done
printf 'results=%s\n' "$output"
