#!/usr/bin/env bash
# Item 1 follow-up experiments on the cloud sandbox (root, 4 cores, fd hard limit 20000).
# Each experiment changes at most one factor against the baseline, records the sysctl values it
# changes and restores them afterwards. Results go to bench/results/cloud with cloud-* labels.
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=bench/results/cloud
LOG=$OUT/experiments.log
N=${N:-19000}
REPEATS=${REPEATS:-5}
mkdir -p "$OUT"

declare -A SAVED
set_sysctl() {
  local key=$1 value=$2 path=/proc/sys/${1//.//}
  SAVED[$key]=$(cat "$path")
  echo "$value" > "$path"
  echo "sysctl $key: ${SAVED[$key]} -> $(cat "$path")" | tee -a "$LOG"
}
restore_sysctls() {
  local key
  for key in "${!SAVED[@]}"; do
    echo "${SAVED[$key]}" > "/proc/sys/${key//.//}"
    echo "sysctl $key restored to $(cat "/proc/sys/${key//.//}")" | tee -a "$LOG"
  done
  unset SAVED
  declare -gA SAVED
}
CONNTRACK_RULE=(INPUT -i lo -m conntrack --ctstate NEW)
cleanup() {
  local status=$?
  trap - EXIT INT TERM
  restore_sysctls
  iptables -D "${CONNTRACK_RULE[@]}" 2>/dev/null && echo "conntrack rule removed" | tee -a "$LOG"
  exit "$status"
}
# Any failure or signal after the first host change must still put the host back.
trap cleanup EXIT INT TERM

run() {
  local variant=$1; shift
  echo "== $variant $(date -u +%FT%TZ) N=$N repeats=$REPEATS args: $*" | tee -a "$LOG"
  python3 bench/run.py --variant "$variant" --out "$OUT" --transports ws --ladder "$N" \
    --repeats "$REPEATS" "$@" 2>&1 | grep -E '^\[run|^\[limit|^\[skip' | tee -a "$LOG"
  local dropped=0 field
  while read -r _ field _; do dropped=$((dropped + 16#$field)); done < /proc/net/softnet_stat
  echo "softnet_stat after: $dropped dropped; conntrack count $(cat /proc/sys/net/netfilter/nf_conntrack_count 2>/dev/null || echo n/a)" | tee -a "$LOG"
}

ulimit -n 20000
echo "fd limit: soft $(ulimit -Sn) hard $(ulimit -Hn)" | tee -a "$LOG"

# Baseline: the harness defaults on this machine, full ladder, repeats at 10000 and at N.
run cloud-ws --ladder 1000 5000 10000 "$N"

# The original machine's 4096-port ephemeral range, at rising per-destination occupancy.
set_sysctl net.ipv4.ip_local_port_range "40525 44620"
run cloud-ws-ports4096
run cloud-ws-ports4096-spread16 --spread-loopback 16
run cloud-ws-ports4096-spread8 --spread-loopback 8
run cloud-ws-ports4096-spread5 --spread-loopback 5
restore_sysctls

# Load generator connect concurrency against the server accept loop.
run cloud-ws-concurrency16 --connect-concurrency 16
run cloud-ws-concurrency1024 --connect-concurrency 1024

# Accept queue overflow signature: a tiny listen backlog.
run cloud-ws-backlog16 --listen-backlog 16

# SYN queue: cookies off, and cookies off with a small SYN backlog.
set_sysctl net.ipv4.tcp_syncookies 0
run cloud-ws-syncookies0
set_sysctl net.ipv4.tcp_max_syn_backlog 64
run cloud-ws-syncookies0-synbacklog64 --listen-backlog 64
restore_sysctls

# Conntrack table full signature: a loopback-only match with no target registers the conntrack
# hook without accepting anything the chain would otherwise reject; then cap the table below N.
if iptables -I "${CONNTRACK_RULE[@]}" 2>>"$LOG"; then
  set_sysctl net.netfilter.nf_conntrack_max 12000
  run cloud-ws-conntrack12000
  restore_sysctls
  iptables -D "${CONNTRACK_RULE[@]}"
  dmesg 2>/dev/null | grep -i conntrack | tail -3 | tee -a "$LOG" || true
fi

# Loopback receive backlog: a tiny per-cpu softnet queue.
set_sysctl net.core.netdev_max_backlog 8
run cloud-ws-netdevbacklog8
restore_sysctls

# SSE baseline at the same N for item 4.
echo "== cloud-sse $(date -u +%FT%TZ)" | tee -a "$LOG"
python3 bench/run.py --variant cloud-sse --out "$OUT" --transports sse --ladder 1000 5000 10000 "$N" \
  --repeats "$REPEATS" 2>&1 | grep -E '^\[run|^\[limit|^\[skip' | tee -a "$LOG"
echo "done $(date -u +%FT%TZ)" | tee -a "$LOG"
trap - EXIT INT TERM
restore_sysctls
