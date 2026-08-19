# hydrant

hydrant is a planned SSE/WebSocket fan-out server. This repository is scaffolding only: there is no fan-out yet, just an HTTP server with `/healthz`, a stub `/metrics`, structured logging and graceful shutdown.

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

## Configuration

Every option is available as an environment variable and as a CLI flag.

| Variable | Flag | Default | Description |
| --- | --- | --- | --- |
| `HYDRANT_LISTEN_ADDR` | `--listen-addr` | `127.0.0.1:8080` | address to bind |
| `HYDRANT_LOG_LEVEL` | `--log-level` | `info` | tracing filter directive, e.g. `info,tower_http=debug` |
| `HYDRANT_LOG_FORMAT` | `--log-format` | `pretty` | `pretty` or `json` |
| `HYDRANT_SHUTDOWN_TIMEOUT_SECS` | `--shutdown-timeout-secs` | `10` | max seconds to drain connections after SIGTERM or SIGINT |

## Docker

```sh
docker build -t hydrant-server:dev .
docker run --rm -p 8080:8080 hydrant-server:dev
```
