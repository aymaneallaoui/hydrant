#!/usr/bin/env bash
# Heap profile of hydrant-server holding N idle connections of one transport, with heaptrack.
#
#   bench/heap-profile.sh <ws|sse> <N> <out-dir> [server-bin]
#
# Needs a server binary with symbols: build with
#   CARGO_PROFILE_RELEASE_DEBUG=line-tables-only CARGO_PROFILE_RELEASE_STRIP=false \
#   cargo build --release --locked -p hydrant-server --target-dir target/prof
# Writes heaptrack's raw file, the heaptrack_print summary and the server RSS at idle.
set -euo pipefail
cd "$(dirname "$0")/.."
TRANSPORT=$1; N=$2; OUT=$(mkdir -p "$3" && cd "$3" && pwd); BIN=${4:-target/prof/release/hydrant-server}
PORT=${PORT:-48600}
SPREAD=64
[ "$N" -ge "$SPREAD" ] || { echo "N must be at least $SPREAD (the loopback spread)" >&2; exit 2; }
curl -fs "http://127.0.0.1:$PORT/healthz" >/dev/null 2>&1 && { echo "port $PORT is already serving; pick another PORT" >&2; exit 1; }
LOADGEN=target/release/hydrant-loadgen
TAG=${TRANSPORT}_${N}

HYDRANT_LISTEN_ADDR=0.0.0.0:$PORT HYDRANT_LOG_LEVEL=warn \
  heaptrack --record-only -o "$OUT/heaptrack_$TAG" "$BIN" > "$OUT/server_$TAG.log" 2>&1 &
HT=$!
# The server is the hydrant-server process descended from this heaptrack invocation, never a stray one.
server_pid_under() {
  local pid ancestor
  for pid in $(pgrep -x hydrant-server); do
    ancestor=$pid
    while [ "$ancestor" -gt 1 ]; do
      [ "$ancestor" = "$1" ] && { echo "$pid"; return 0; }
      ancestor=$(awk '/^PPid:/{print $2}' "/proc/$ancestor/status" 2>/dev/null || echo 1)
    done
  done
  return 1
}
for _ in $(seq 1 100); do curl -fs "http://127.0.0.1:$PORT/healthz" >/dev/null 2>&1 && break; sleep 0.2; done
SERVER_PID=$(server_pid_under "$HT") || { echo "the profiled server did not start; see $OUT/server_$TAG.log" >&2; exit 1; }
grep -E 'VmRSS|RssAnon' "/proc/$SERVER_PID/status" | tr -s ' ' | tr '\n' ' ' | sed 's/^/baseline: /' | tee "$OUT/rss_$TAG.txt"; echo | tee -a "$OUT/rss_$TAG.txt"

# Hold idle long enough to sample at a stable gauge; the loadgen publishes 2 messages then disconnects.
"$LOADGEN" --url "http://127.40.1.1:$PORT" --transport "$TRANSPORT" --connections "$N" --spread-loopback "$SPREAD" \
  --rate 1 --duration 2 --payload-bytes 64 --connect-concurrency 128 --hold-idle 25 \
  --json "$OUT/loadgen_$TAG.json" > "$OUT/loadgen_$TAG.txt" 2>&1 &
LG=$!
for _ in $(seq 1 300); do
  gauge=$(curl -fs "http://127.0.0.1:$PORT/metrics" | awk -v t="$TRANSPORT" '$1 ~ "hydrant_connections\\{transport=\"" t "\"\\}" {print int($2)}')
  [ "${gauge:-0}" -ge "$N" ] && break
  sleep 0.5
done
if [ "${gauge:-0}" -lt "$N" ]; then
  echo "only ${gauge:-0} of $N connections established; profile not taken (see $OUT/loadgen_$TAG.txt)" >&2
  kill "$LG" "$SERVER_PID" 2>/dev/null || true
  exit 1
fi
sleep 5
grep -E 'VmRSS|RssAnon' "/proc/$SERVER_PID/status" | tr -s ' ' | tr '\n' ' ' | sed "s/^/idle at gauge $gauge: /" | tee -a "$OUT/rss_$TAG.txt"; echo | tee -a "$OUT/rss_$TAG.txt"
wait "$LG" || { echo "load generator failed; profile not taken (see $OUT/loadgen_$TAG.txt)" >&2; kill "$SERVER_PID" 2>/dev/null || true; exit 1; }
kill -INT "$SERVER_PID" 2>/dev/null || true
wait $HT || true
RAW=$(find "$OUT" -maxdepth 1 -name "heaptrack_$TAG*" \( -name '*.zst' -o -name '*.gz' \) -printf '%T@ %p\n' | sort -rn | head -1 | cut -d' ' -f2-)
heaptrack_print -f "$RAW" -n 40 -s 3 > "$OUT/summary_$TAG.txt" 2>&1
heaptrack_print -f "$RAW" --flamegraph-cost-type peak -F "$OUT/peak_stacks_$TAG.txt" > /dev/null 2>&1 || true
echo "wrote $OUT/summary_$TAG.txt"
