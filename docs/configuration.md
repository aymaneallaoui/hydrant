# Configuration and endpoints

Reference for hydrant-server. The overview is in the [README](../README.md).

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

Publishing to a topic with no subscribers is accepted and returns `{"receivers":0}`. A payload over the limit gets 413, a non-UTF-8 payload gets 400, and an empty body gets 400 (an SSE event with no data is not a message, so it could not be delivered to every transport) and is not counted as published. Once shutdown has begun, publish and subscribe get 503 and the publish is not counted.

WebSocket delivery is byte-exact. SSE is line-oriented: a `\n`, `\r` or `\r\n` in a payload is sent as a line break between `data:` fields, so a client rejoining them with `\n` sees `\r` as `\n` and `\r\n` as two `\n`. SSE delivery is therefore not byte-exact for payloads containing carriage returns.

## Backpressure in detail

Each topic keeps one bounded ring of the last `HYDRANT_TOPIC_CAPACITY` messages, shared by all of its subscribers; every subscriber has its own read position in it, so the ring is per topic and not per connection. The capacity must be a power of two (tokio's broadcast channel would otherwise round it up and retain more than configured), so `HYDRANT_TOPIC_CAPACITY=300` is rejected at startup, naming 256 and 512 as the nearest valid values. A ring slot is held until every subscriber has moved past it, so the worst case per topic is the retained message count times `HYDRANT_MAX_MESSAGE_BYTES` (512 x 64 KiB = 32 MiB at a capacity of 512), and a stalled subscriber can keep a full ring alive.

A subscriber that falls more than the retained count behind is handled by `HYDRANT_SLOW_CONSUMER_POLICY`. With `disconnect` (the default) the connection is ended: WebSocket gets close code 1008 and SSE just ends. With `drop` the skipped messages are counted in `hydrant_messages_dropped_total` and the subscriber continues from the oldest retained message.

Both transports also have a send timeout. A WebSocket send, or the hand-off of an SSE event to the response body, that blocks longer than `HYDRANT_SEND_TIMEOUT_SECS` counts as a slow consumer disconnect: the subscription and connection gauge are released even if the peer never reads and the socket stays open. Each SSE subscription is driven by a small task feeding a one-slot channel, so a client that stops reading is detected after at most one buffered event plus the timeout, and a client that disconnects while idle releases its resources promptly. An idle subscriber that is not sent anything is not timed out; that is what the SSE keep-alive comments are for.

`HYDRANT_MAX_CONNECTIONS` optionally caps the total of SSE and WebSocket connections. At the cap a new subscribe request gets 503 with a short plain-text message and `hydrant_connections_rejected_total` increments. Unset means unlimited.

## Environment variables

Every option is available as an environment variable and as a CLI flag.

| Variable | Flag | Default | Description |
| --- | --- | --- | --- |
| `HYDRANT_LISTEN_ADDR` | `--listen-addr` | `127.0.0.1:8080` | address to bind |
| `HYDRANT_LISTEN_BACKLOG` | `--listen-backlog` | `4096` | listen(2) backlog, clamped by the kernel to `net.core.somaxconn`; a larger accept queue than tokio's default of 128, chosen as a sane default, with no measured benefit in the connection benchmark |
| `HYDRANT_LOG_LEVEL` | `--log-level` | `info` | tracing filter directive, e.g. `info,tower_http=debug` |
| `HYDRANT_LOG_FORMAT` | `--log-format` | `pretty` | `pretty` or `json` |
| `HYDRANT_SHUTDOWN_TIMEOUT_SECS` | `--shutdown-timeout-secs` | `10` | max seconds to drain connections after SIGTERM or SIGINT |
| `HYDRANT_TOPIC_CAPACITY` | `--topic-capacity` | `256` | messages retained per topic; must be a power of two (1, 2, 4, ... 256, 512) |
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
