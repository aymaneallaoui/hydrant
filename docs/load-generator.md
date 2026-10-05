# Load generator

`hydrant-loadgen` opens N subscriber connections (WebSocket or SSE), waits until the connect phase is complete, then publishes open-loop at a fixed rate and measures end-to-end delivery latency. Each payload is `<send_nanos> <seq> <padding>`. The timestamp is the message's intended send time, `start + seq / rate`, in nanoseconds on a process-wide monotonic clock shared by the publisher and the subscribers (the loadgen is both, so no wall-clock synchronisation is involved). Because the stamp is the intended time, a stalled server shows up as latency instead of being hidden by a publisher that slows down with it. A timestamp later than the receive time is counted as malformed, never as a best-case sample.

## Flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `--url` | `http://127.0.0.1:8080` | server base URL (plain `http://` only) |
| `--topic` | `loadgen` | topic to subscribe to and publish on |
| `--connections` | `100` | subscriber connections to open |
| `--transport` | `ws` | `ws` or `sse` |
| `--rate` | `10` | messages published per second |
| `--duration` | `10` | seconds of publishing |
| `--payload-bytes` | `64` | payload size |
| `--connect-concurrency` | `256` | connections opened in parallel |
| `--drain-timeout` | `5` | seconds to wait for in-flight deliveries after publishing |
| `--spread-loopback <N>` | `1` | spread subscribers over N loopback addresses, see below |
| `--hold-idle <SECS>` | `0` | hold established connections idle before publishing |
| `--json <PATH>` | unset | also write the report as JSON |
| `--max-loss-percent <PERCENT>` | unset | exit 3 if loss is above this |

Publishing is by index: exactly `rate x duration` messages are scheduled, none are skipped, and a publisher that falls behind sends without sleeping until it catches up. It stops at the scheduled end plus a 500 ms grace, bounds each request by the remaining time, and reports messages it never sent as `not sent`. The report gives requested and achieved publish rate and prints a warning when achieved is below 95% of requested, since the latency figures then describe a lower load than asked for. Publishing uses one keep-alive connection, which is enough for the modest rates here and not for very high ones.

Sequence numbers are tracked per subscriber: a jump counts the skipped messages as `gaps seen`, and a repeated or older sequence counts as a duplicate and is not a delivery. `missing` is expected minus in-order deliveries, so duplicates cannot hide loss. A publish that times out may still be delivered, so `received` can exceed `expected` by those messages. Throughput is deliveries divided by the publish-phase duration (the scheduled duration when the run completes). `--url` hostnames are resolved to all addresses and the first one that accepts a TCP connection is used. Reports include connect results, loss, latency percentiles and throughput, optionally as JSON (`--json <path>`). Only plain `http://` and `ws://` URLs are supported, and Ctrl-C prints a partial report.

Exit codes: 0 run completed, 1 error (bad arguments, no address reachable, no connection established, JSON write failure), 2 no publish succeeded (the report is printed but meaningless), 3 loss above `--max-loss-percent` (optional; the report is printed first).

A single destination address and port allows only about as many connections as the ephemeral port range (roughly 28,000 by default on Linux, 4,096 on some setups), because every connection needs a distinct source port per destination. `--spread-loopback <N>` (at most the connection count and at most 65,536) spreads the subscriber connections round-robin over N consecutive IPv4 addresses starting at the `--url` host, which must be in 127.0.0.0/8 (for example `--url http://127.0.0.1:8080 --spread-loopback 64` uses 127.0.0.1 to 127.0.0.64). Every address in that block is local, but the server must listen on `0.0.0.0` (`HYDRANT_LISTEN_ADDR=0.0.0.0:8080`) to accept them. The publisher keeps using the `--url` host. `--hold-idle <secs>` keeps the established connections idle for that long before publishing, so an external tool can sample the server at rest. The loadgen prints a progress line once a second while connecting and the marker lines `loadgen: all connected`, `loadgen: publish starting` and `loadgen: disconnecting` on stderr; the JSON report gains `params.spread_loopback` and `params.hold_idle_secs`. See [`bench/README.md`](../bench/README.md) for the connection-scaling sweep.

Measured memory, latency and connect time at up to 100,000 requested connections (99,691 to 99,982 established in the ten runs at that size, with zero lost deliveries among the established ones) are written up in [scaling-to-100k.md](scaling-to-100k.md).

```sh
make build
./target/release/hydrant-loadgen --transport ws --connections 1000 --rate 10 --duration 10
./target/release/hydrant-loadgen --transport sse --connections 1000 --rate 10 --duration 10 --json result.json
./target/release/hydrant-loadgen --connections 1000 --max-loss-percent 0.1
make loadgen ARGS="--connections 1000"
```

Sample report (1000 WebSocket connections against a local release server):

```text
parameters
  url                               http://127.0.0.1:18080
  topic                             loadgen
  transport                         ws
  connections                       1000
  rate (msg/s)                      10
  duration (s)                      10
  payload (bytes)                   64
  connect concurrency               256
  drain timeout (s)                 5

connections
  requested                         1000
  established                       1000
  failed                            0
  closed early                      0
  connect phase (s)                 1.127
  connects per second               887

publish
  scheduled                         100
  ok                                100
  failed                            0
  not sent                          0
  requested rate (msg/s)            10.0
  achieved rate (msg/s)             10.0
  publish phase (s)                 10.001

deliveries
  expected                          100000
  received                          100000
  missing                           0
  loss (%)                          0.000
  malformed                         0
  duplicates                        0
  gaps seen                         0
  deliveries/s over publish phase   9999

latency (ms)
  min                               0.618
  mean                              5.235
  p50                               5.511
  p90                               6.999
  p99                               8.199
  p99.9                             8.799
  max                               9.783
```

Each connection uses one file descriptor on the client and one on the server, and the default soft limit is often 1024, so larger runs need `ulimit -n` raised in the shells that start the server and loadgen. Loadgen reports `Too many open files` failures with that hint. Connecting to a single `host:port` from one address is also bounded by the ephemeral port range (`net.ipv4.ip_local_port_range`). The 100k run and system tuning are not covered here.
