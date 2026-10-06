# hydrant

hydrant is a small server that fans messages out to many subscribers. Clients subscribe to a topic over SSE or WebSocket, and an HTTP POST delivers a message to everyone currently subscribed to that topic. It is written in Rust on tokio and axum.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/img/architecture-dark.png">
  <img src="docs/img/architecture.png" width="880" alt="Architecture diagram. A publisher POSTs to /topics/{topic}. The hub looks up the topic and appends to its single shared ring buffer, assigning a sequence number. Each subscriber connection has its own cursor into that ring: a ws pump writes text frames to a WebSocket client, an sse pump hands events through a one-slot channel to an SSE client. A cursor that lags past the ring capacity triggers the slow consumer policy, drop or disconnect, and a send that exceeds the send timeout disconnects. Healthz, metrics, environment config and graceful shutdown are noted alongside.">
</picture>

The point is to hold many mostly idle connections in one process, and to make the slow-consumer problem explicit instead of hiding it in an unbounded queue. It is not a product: see the end for what is missing.

## Quickstart

You need Rust (the version is pinned in `rust-toolchain.toml`) and GNU Make.

```sh
cp .env.example .env
make run
```

In a second terminal, subscribe, then publish from a third:

```sh
curl -N http://127.0.0.1:8080/topics/news/sse
curl -X POST --data 'hello' http://127.0.0.1:8080/topics/news
```

The subscriber prints `id: 1` and `data: hello`, and the publish answers `{"receivers":1}`. WebSocket works the same way at `/topics/news/ws`. Other make targets: `build`, `test`, `lint`, `fmt`, `loadgen`, `bench-sweep`, `bench-plot`.

## Endpoints

| Request | What it does | Status codes |
| --- | --- | --- |
| `POST /topics/{topic}` | publish the body to every subscriber, answers `{"receivers":N}` | 202; 400 bad topic, empty body or invalid UTF-8; 413 over `HYDRANT_MAX_MESSAGE_BYTES`; 503 shutting down |
| `GET /topics/{topic}/sse` | subscribe over Server-Sent Events | 200 stream; 400 bad topic; 503 shutting down or at the connection cap |
| `GET /topics/{topic}/ws` | subscribe over WebSocket, one text frame per message | 101 upgrade; same errors as SSE |
| `GET /healthz` | liveness | 200 |
| `GET /metrics` | Prometheus text metrics | 200 |

Topic names are 1 to 128 characters from `[A-Za-z0-9._-]`, and a topic exists only while it has subscribers, so publishing to an empty one returns `{"receivers":0}`. An empty body is rejected because an SSE event with no data is not a message. WebSocket delivery is byte-exact. SSE is line-oriented, so `\r` and `\r\n` in a payload come out as line breaks and a client rejoining the `data:` lines sees different bytes.

## How backpressure works

Each topic has one bounded ring of the last `HYDRANT_TOPIC_CAPACITY` messages, shared by all of its subscribers. Every connection keeps its own position in it, so memory is per topic and not per connection. The capacity must be a power of two, because tokio's broadcast channel would otherwise round it up and retain more than configured. Worst case per topic is the capacity times `HYDRANT_MAX_MESSAGE_BYTES` (512 x 64 KiB = 32 MiB), since a stalled subscriber can keep a full ring alive.

A subscriber that falls further behind than the ring holds is handled by `HYDRANT_SLOW_CONSUMER_POLICY`. `disconnect` (the default) ends the connection, with close code 1008 on WebSocket. `drop` skips the missed messages, counts them in `hydrant_messages_dropped_total` and continues from the oldest retained one.

A write that blocks longer than `HYDRANT_SEND_TIMEOUT_SECS` also counts as a slow consumer, so a client that stops reading cannot pin its resources. An idle subscriber is not timed out; the SSE keep-alive comments cover that. `HYDRANT_MAX_CONNECTIONS` caps SSE plus WebSocket connections; at the cap a new subscribe gets 503 and `hydrant_connections_rejected_total` increments.

## Configuration

Every option is an environment variable and a CLI flag (the flag is the lowercased name with dashes, for example `--listen-backlog`).

| Variable | Default | Meaning |
| --- | --- | --- |
| `HYDRANT_LISTEN_ADDR` | `127.0.0.1:8080` | address to bind |
| `HYDRANT_LISTEN_BACKLOG` | `4096` | listen(2) backlog, clamped by the kernel to `net.core.somaxconn`; no measured benefit in my connection benchmark |
| `HYDRANT_LOG_LEVEL` | `info` | tracing filter, e.g. `info,tower_http=debug` |
| `HYDRANT_LOG_FORMAT` | `pretty` | `pretty` or `json` |
| `HYDRANT_SHUTDOWN_TIMEOUT_SECS` | `10` | seconds to drain connections after SIGTERM or SIGINT |
| `HYDRANT_TOPIC_CAPACITY` | `256` | messages retained per topic, a power of two (300 is rejected at startup) |
| `HYDRANT_SLOW_CONSUMER_POLICY` | `disconnect` | `disconnect` or `drop` |
| `HYDRANT_SEND_TIMEOUT_SECS` | `10` | seconds a send may block before the subscriber is disconnected, at least 1 |
| `HYDRANT_SSE_KEEPALIVE_SECS` | `15` | seconds between SSE keep-alive comments, at least 1 |
| `HYDRANT_MAX_MESSAGE_BYTES` | `65536` | max publish body; also caps incoming WebSocket frames |
| `HYDRANT_MAX_CONNECTIONS` | unset | cap on SSE plus WebSocket connections, at least 1; unset means unlimited |

```sh
docker build -t hydrant-server:dev .
docker run --rm -p 8080:8080 hydrant-server:dev
```

## Load generator

`hydrant-loadgen` opens N subscribers (WebSocket or SSE), waits until they are all connected, then publishes open-loop at a fixed rate and measures end-to-end delivery latency. The timestamp in each payload is the message's intended send time, so a stalled server shows up as latency instead of being hidden by a publisher that slows down with it.

```sh
make build
./target/release/hydrant-loadgen --transport ws --connections 1000 --rate 10 --duration 10 --json result.json
make loadgen ARGS="--connections 1000 --max-loss-percent 0.1"
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--url` | `http://127.0.0.1:8080` | server base URL, plain `http://` only |
| `--topic` | `loadgen` | topic to subscribe and publish on |
| `--connections` | `100` | subscriber connections |
| `--transport` | `ws` | `ws` or `sse` |
| `--rate` | `10` | messages per second |
| `--duration` | `10` | seconds of publishing |
| `--payload-bytes` | `64` | payload size |
| `--connect-concurrency` | `256` | connections opened in parallel |
| `--drain-timeout` | `5` | seconds to wait for in-flight deliveries afterwards |
| `--spread-loopback <N>` | `1` | spread subscribers over N consecutive 127.0.0.0/8 addresses starting at the `--url` host; the server must listen on `0.0.0.0` |
| `--hold-idle <SECS>` | `0` | hold connections idle before publishing, so another tool can sample the server at rest |
| `--json <PATH>` | unset | also write the report as JSON |
| `--max-loss-percent <PERCENT>` | unset | exit 3 if loss is above this |

Exit codes: 0 run completed, 1 error (bad arguments, no address reachable, no connection established, JSON write failure), 2 no publish succeeded, 3 loss above `--max-loss-percent`. Ctrl-C prints a partial report. The run warns when the achieved publish rate is under 95 percent of requested. Loss counts sequence gaps per subscriber, and duplicates cannot hide it.

Each connection takes a file descriptor on both sides, so raise `ulimit -n` in the shells that start the server and the loadgen. One destination address is also limited by the ephemeral port range, which is what `--spread-loopback` works around; the details are in the write-up. A report from 1000 WebSocket connections against a local release server, trimmed:

```text
connections   requested 1000   established 1000   failed 0   connect phase 1.127 s
publish       scheduled 100    ok 100             achieved rate 10.0 msg/s
deliveries    expected 100000  received 100000    missing 0  loss 0.000 %  duplicates 0
latency (ms)  min 0.618  mean 5.235  p50 5.511  p90 6.999  p99 8.199  p99.9 8.799  max 9.783
```

## Numbers

On one 32-core WSL2 machine, over loopback, hydrant held up to 99,982 of 100,000 requested WebSocket connections (99,691 in the worst of ten runs). Server memory was 6.5 KB per connection after I fixed a buffer problem, against 137 KB before. SSE costs 27.7 KB per connection and I only got it to 75,000; a one-line router change later brought that down to 22.5 KB. The connect phase at 100,000 connections took anywhere from 4.4 to 175 seconds and I never explained why. A 4-core cloud sandbox, limited to 19,000 connections by its file limit, reproduced the per-connection figures within 1 percent but could not reach the stall.

All of that is one topic at one message per second, so it says little about throughput. The full write-up is in [docs/scaling-to-100k.md](docs/scaling-to-100k.md), and [bench/README.md](bench/README.md) describes how the sweep runs.

## What is missing

- The publish endpoint is unauthenticated. Anyone who can reach the server can publish to any topic.
- No TLS. Put a proxy in front if you need it.
- Single node, no persistence, no replay. A subscriber that reconnects starts from now.
- The benchmark is two machines over loopback. Treat the numbers as a floor for my setup, not a promise.
