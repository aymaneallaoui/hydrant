# hydrant

hydrant is an SSE/WebSocket fan-out server. Clients subscribe to a topic over SSE or WebSocket, and an HTTP POST publishes a message to every current subscriber of that topic.

## Prerequisites

- Rust 1.96 (pinned by `rust-toolchain.toml`)
- GNU Make
- Docker (optional)

## Quickstart

```sh
cp .env.example .env
make run
curl -i http://127.0.0.1:8080/healthz
```

## Endpoints

| Method and path | Description |
| --- | --- |
| `POST /topics/{topic}` | publish the request body (valid UTF-8, at most `HYDRANT_MAX_MESSAGE_BYTES`) |
| `GET /topics/{topic}/sse` | subscribe over Server-Sent Events |
| `GET /topics/{topic}/ws` | subscribe over WebSocket |
| `GET /healthz` | liveness |
| `GET /metrics` | Prometheus text metrics |

Topic names are 1 to 128 characters from `[A-Za-z0-9._-]`; anything else gets a 400. A topic exists only while it has subscribers.

The publish endpoint is unauthenticated: anyone who can reach the server can publish to any topic.

```sh
# subscribe (prints `id: <seq>` and `data: <payload>` per message)
curl -N http://127.0.0.1:8080/topics/news/sse

# subscribe over WebSocket (one text frame per message)
websocat ws://127.0.0.1:8080/topics/news/ws

# publish; the response is 202 with the number of subscribers it was handed to
curl -X POST --data 'hello' http://127.0.0.1:8080/topics/news
# {"receivers":2}
```

Publishing to a topic with no subscribers is accepted and returns `{"receivers":0}`. A payload over the limit gets 413 and a non-UTF-8 payload gets 400. Once shutdown has begun, publish and subscribe get 503 and the publish is not counted.

WebSocket delivery is byte-exact. SSE is line-oriented: a `\n`, `\r` or `\r\n` in a payload is sent as a line break between `data:` fields, so a client rejoining them with `\n` sees `\r` as `\n` and `\r\n` as two `\n`. SSE delivery is therefore not byte-exact for payloads containing carriage returns.

## Backpressure

Each topic keeps one bounded ring of the last `HYDRANT_TOPIC_CAPACITY` messages, shared by all of its subscribers; every subscriber has its own read position in it, so the ring is per topic and not per connection. tokio rounds the capacity up to the next power of two, so `HYDRANT_TOPIC_CAPACITY=300` retains 512 messages. A ring slot is held until every subscriber has moved past it, so the worst case per topic is the retained message count times `HYDRANT_MAX_MESSAGE_BYTES` (512 x 64 KiB = 32 MiB for the example above), and a stalled subscriber can keep a full ring alive.

A subscriber that falls more than the retained count behind is handled by `HYDRANT_SLOW_CONSUMER_POLICY`. With `disconnect` (the default) the connection is ended: WebSocket gets close code 1008 and SSE just ends. With `drop` the skipped messages are counted in `hydrant_messages_dropped_total` and the subscriber continues from the oldest retained message.

Both transports also have a send timeout. A WebSocket send, or the hand-off of an SSE event to the response body, that blocks longer than `HYDRANT_SEND_TIMEOUT_SECS` counts as a slow consumer disconnect: the subscription and connection gauge are released even if the peer never reads and the socket stays open. Each SSE subscription is driven by a small task feeding a one-slot channel, so a client that stops reading is detected after at most one buffered event plus the timeout, and a client that disconnects while idle releases its resources promptly. An idle subscriber that is not sent anything is not timed out; that is what the SSE keep-alive comments are for.

`HYDRANT_MAX_CONNECTIONS` optionally caps the total of SSE and WebSocket connections. At the cap a new subscribe request gets 503 with a short plain-text message and `hydrant_connections_rejected_total` increments. Unset means unlimited.

## Load generator

`hydrant-loadgen` opens N subscriber connections (WebSocket or SSE), waits until the connect phase is complete, then publishes open-loop at a fixed rate and measures end-to-end delivery latency. Each payload is `<send_nanos> <seq> <padding>`. The timestamp is the message's intended send time, `start + seq / rate`, in nanoseconds on a process-wide monotonic clock shared by the publisher and the subscribers (the loadgen is both, so no wall-clock synchronisation is involved). Because the stamp is the intended time, a stalled server shows up as latency instead of being hidden by a publisher that slows down with it. A timestamp later than the receive time is counted as malformed, never as a best-case sample.

Publishing is by index: exactly `rate x duration` messages are scheduled, none are skipped, and a publisher that falls behind sends without sleeping until it catches up. It stops at the scheduled end plus a 500 ms grace, bounds each request by the remaining time, and reports messages it never sent as `not sent`. The report gives requested and achieved publish rate and prints a warning when achieved is below 95% of requested, since the latency figures then describe a lower load than asked for. Publishing uses one keep-alive connection, which is enough for the modest rates here and not for very high ones.

Sequence numbers are tracked per subscriber: a jump counts the skipped messages as `gaps seen`, and a repeated or older sequence counts as a duplicate and is not a delivery. `missing` is expected minus in-order deliveries, so duplicates cannot hide loss. A publish that times out may still be delivered, so `received` can exceed `expected` by those messages. Throughput is deliveries divided by the publish-phase duration (the scheduled duration when the run completes). `--url` hostnames are resolved to all addresses and the first one that accepts a TCP connection is used. Reports include connect results, loss, latency percentiles and throughput, optionally as JSON (`--json <path>`). Only plain `http://` and `ws://` URLs are supported, and Ctrl-C prints a partial report.

Exit codes: 0 run completed, 1 error (bad arguments, no address reachable, no connection established, JSON write failure), 2 no publish succeeded (the report is printed but meaningless), 3 loss above `--max-loss-percent` (optional; the report is printed first).

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

## Make targets

| Target | Action |
| --- | --- |
| `make run` | run `hydrant-server` |
| `make build` | release build of the workspace |
| `make test` | run all tests |
| `make lint` | clippy with warnings as errors |
| `make fmt` | format the code |
| `make fmt-check` | check formatting |
| `make bench` | run benches (none yet) |
| `make loadgen ARGS="..."` | run the release load generator with extra arguments |

## Configuration

Every option is available as an environment variable and as a CLI flag.

| Variable | Flag | Default | Description |
| --- | --- | --- | --- |
| `HYDRANT_LISTEN_ADDR` | `--listen-addr` | `127.0.0.1:8080` | address to bind |
| `HYDRANT_LOG_LEVEL` | `--log-level` | `info` | tracing filter directive, e.g. `info,tower_http=debug` |
| `HYDRANT_LOG_FORMAT` | `--log-format` | `pretty` | `pretty` or `json` |
| `HYDRANT_SHUTDOWN_TIMEOUT_SECS` | `--shutdown-timeout-secs` | `10` | max seconds to drain connections after SIGTERM or SIGINT |
| `HYDRANT_TOPIC_CAPACITY` | `--topic-capacity` | `256` | messages retained per topic, at least 1; rounded up to a power of two |
| `HYDRANT_SLOW_CONSUMER_POLICY` | `--slow-consumer-policy` | `disconnect` | `disconnect` or `drop` for subscribers that lag past the capacity |
| `HYDRANT_SEND_TIMEOUT_SECS` | `--send-timeout-secs` | `10` | max seconds a WebSocket or SSE send may block before the subscriber is disconnected, at least 1 |
| `HYDRANT_SSE_KEEPALIVE_SECS` | `--sse-keepalive-secs` | `15` | seconds between SSE keep-alive comments, at least 1 |
| `HYDRANT_MAX_MESSAGE_BYTES` | `--max-message-bytes` | `65536` | max publish body size; also caps incoming WebSocket frames |
| `HYDRANT_MAX_CONNECTIONS` | `--max-connections` | unset | cap on SSE plus WebSocket connections, at least 1; unset means unlimited |

## Docker

```sh
docker build -t hydrant-server:dev .
docker run --rm -p 8080:8080 hydrant-server:dev
```
