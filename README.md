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

Publishing to a topic with no subscribers is accepted and returns `{"receivers":0}`. A payload over the limit gets 413 and a non-UTF-8 payload gets 400.

## Backpressure

Each topic keeps one bounded ring of the last `HYDRANT_TOPIC_CAPACITY` messages, shared by all of its subscribers; every subscriber has its own read position in it, so memory is bounded per topic and not per connection. A subscriber that falls more than that many messages behind is handled by `HYDRANT_SLOW_CONSUMER_POLICY`. With `disconnect` (the default) the connection is ended: WebSocket gets close code 1008 and SSE just ends. With `drop` the skipped messages are counted in `hydrant_messages_dropped_total` and the subscriber continues from the oldest retained message. A WebSocket send that blocks longer than `HYDRANT_WS_SEND_TIMEOUT_SECS` also counts as a slow consumer disconnect.

## Load generator

`hydrant-loadgen` opens N subscriber connections (WebSocket or SSE), waits until the connect phase is complete, then publishes at a fixed rate and measures end-to-end delivery latency. Each payload is `<send_unix_nanos> <seq> <padding>`; every subscriber parses the timestamp and records `now - send` into a sharded HDR histogram, so the latency is wall-clock time on one machine. This only holds because loadgen is both publisher and subscriber: pointing it at a remote server still measures correctly, but running subscribers elsewhere would not. Reports include connect results, loss, latency percentiles and receive throughput, optionally as JSON (`--json <path>`). Only plain `http://` and `ws://` URLs are supported, and Ctrl-C prints a partial report.

```sh
make build
./target/release/hydrant-loadgen --transport ws --connections 1000 --rate 10 --duration 10
./target/release/hydrant-loadgen --transport sse --connections 1000 --rate 10 --duration 10 --json result.json
make loadgen ARGS="--connections 1000"
```

Sample report (1000 WebSocket connections against a local release server):

```text
parameters
  url                         http://127.0.0.1:18080
  topic                       loadgen
  transport                   ws
  connections                 1000
  rate (msg/s)                10
  duration (s)                10
  payload (bytes)             64
  connect concurrency         256
  drain timeout (s)           5

connections
  requested                   1000
  established                 1000
  failed                      0
  closed early                0
  connect phase (s)           0.123
  connects per second         8142

publish
  ok                          100
  failed                      0

deliveries
  expected                    100000
  received                    100000
  missing                     0
  loss (%)                    0.000
  malformed                   0
  throughput (deliveries/s)   10093

latency (ms)
  min                         0.488
  mean                        4.222
  p50                         4.511
  p90                         5.595
  p99                         7.143
  p99.9                       8.167
  max                         9.743
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
| `HYDRANT_TOPIC_CAPACITY` | `--topic-capacity` | `256` | messages retained per topic, at least 1 |
| `HYDRANT_SLOW_CONSUMER_POLICY` | `--slow-consumer-policy` | `disconnect` | `disconnect` or `drop` for subscribers that lag past the capacity |
| `HYDRANT_WS_SEND_TIMEOUT_SECS` | `--ws-send-timeout-secs` | `10` | max seconds a WebSocket send may block, at least 1 |
| `HYDRANT_SSE_KEEPALIVE_SECS` | `--sse-keepalive-secs` | `15` | seconds between SSE keep-alive comments, at least 1 |
| `HYDRANT_MAX_MESSAGE_BYTES` | `--max-message-bytes` | `65536` | max publish body size; also caps incoming WebSocket frames |

## Docker

```sh
docker build -t hydrant-server:dev .
docker run --rm -p 8080:8080 hydrant-server:dev
```
