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

The subscriber prints `id: 1` and `data: hello`, and the publish answers `{"receivers":1}`. WebSocket works the same way at `/topics/news/ws`. `/healthz` and `/metrics` are there too. Other make targets: `build`, `test`, `lint`, `fmt`, `loadgen`, `bench-sweep`, `bench-plot`.

## How backpressure works

Each topic has one bounded ring that all of its subscribers share. Every connection keeps its own position in that ring, so memory is per topic and not per connection. If a subscriber falls further behind than the ring holds, `HYDRANT_SLOW_CONSUMER_POLICY` decides: `disconnect` (the default) ends the connection, `drop` skips the missed messages and counts them in `hydrant_messages_dropped_total`.

There is also a send timeout. If a write to a socket blocks longer than `HYDRANT_SEND_TIMEOUT_SECS`, that subscriber is disconnected, so a client that stops reading cannot pin its resources.

## Numbers

On one 32-core WSL2 machine, over loopback, hydrant held up to 99,982 of 100,000 requested WebSocket connections (99,691 in the worst of ten runs). Server memory was about 6.5 KB per connection after I fixed a buffer problem, against about 137 KB before. SSE costs about 27.7 KB per connection and I only got it to 75,000. The connect phase at 100,000 connections took anywhere from 4.4 to 175 seconds and I never explained why.

A later pass on a second machine (a 4-core cloud sandbox limited to 19,000 connections by its file limit) reproduced the per-connection figures to within 1 percent, traced most of the SSE cost to hyper's HTTP/1 buffers and a route table that axum rebuilt per connection, and cut SSE to about 22.5 KB per connection by fixing the latter. The memory kept after teardown is the allocator holding freed memory, not a leak. The connect-phase stall could not be reached on that machine and is still open.

All of that is one topic at one message per second, so it says little about throughput. The full write-up, with the caveats, is in [docs/scaling-to-100k.md](docs/scaling-to-100k.md).

## What is missing

- The publish endpoint is unauthenticated. Anyone who can reach the server can publish to any topic.
- No TLS. Put a proxy in front if you need it.
- Single node, no persistence, no replay. A subscriber that reconnects starts from now.
- The benchmark is one machine. Treat the numbers as a floor for my setup, not a promise.

## More

- [docs/configuration.md](docs/configuration.md): endpoints, status codes, every environment variable, Docker.
- [docs/load-generator.md](docs/load-generator.md): `hydrant-loadgen` flags, exit codes and a sample report.
- [bench/README.md](bench/README.md): how the connection sweep runs.
