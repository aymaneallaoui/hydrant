# Connection-scaling sweep

`bench/run.py` measures hydrant-server memory per connection and delivery latency against connection count, for WebSocket (`ws`) and SSE (`sse`). It uses only the Python 3 standard library.

## Reproduce

```sh
make bench-sweep                         # builds release binaries, runs the full sweep
make bench-sweep ARGS="--transports ws --ladder 1000 5000 --no-repeat"
python3 bench/run.py --variant unpatched --server-bin /path/to/older/hydrant-server
```

Results go to `bench/results/`:

| File | Content |
| --- | --- |
| `sweep.csv` | one row per variant x transport x N x repetition x phase (`baseline`, `idle`, `load`, `after`), all sampled values, derived per-connection figures and the run-level loadgen results |
| `runs.csv` | one row per run: status, connection results, loss, latency percentiles, achieved rate, teardown times, flags |
| `ramp_<transport>.csv` | time series (2 samples per second) of server RSS, connection gauge, loadgen RSS, kernel TCP memory and `MemAvailable` across the largest ramp-up; `ramp_<transport>_<variant>.csv` for other variants |
| `raw/` | per run: loadgen `--json` report and text report, server log, final `/metrics` scrape, full time series |
| `../../docs/img/` | charts rendered by `bench/plot.py` from the CSVs above (`make bench-plot`) |
| `environment.json` | CPU, cores, RAM, kernel, rustc, git commit, limits, sysctls, server and loadgen settings, notes, one entry per invocation |

Each step starts a fresh server process, so allocator retention from earlier steps never pollutes a number. Sampling points: `baseline` (server up, no connections), `idle` (all connections established, nothing published, 3 s after the server's connection gauge equals the loadgen's established count), `load` (middle of the publish phase), `after` (3 s after the server's gauge returned to zero). Publishing is open-loop at `--rate` messages per second (default 1) for `--duration` seconds (default 20) with 64-byte payloads on one topic, so N connections receive N deliveries per second. Repeats (default 3 runs) are made at N = 10000 and at the largest N that completed. Every run is kept; runs with loss, publish failures, early closes, failed connects or an achieved rate under 95% of requested are flagged in `runs.csv`, never rerun to look better.

Memory columns: `VmRSS`, `RssAnon`, `RssFile`, `VmHWM` from `/proc/<pid>/status`; `Pss` and `Pss_Anon` from `smaps_rollup`; thread and file-descriptor counts; CPU seconds; the same for the loadgen; `tcp_*` from `/proc/net/sockstat` (`tcp_mem_pages` is the kernel's `mem` field in pages, `tcp_mem_kb` the same in KiB). RSS does not include kernel socket buffers, so the kernel cost is only visible in `tcp_mem_*` and in `system_used_per_conn_bytes` (drop in `MemAvailable`, which also covers both loopback ends, the loadgen and any other activity on the machine). `*_per_conn_bytes` are `(value - baseline) / N` in bytes.

## Charts

`make bench-plot` creates `bench/.venv` on first use (matplotlib 3.11.2, nothing installed system-wide) and runs `bench/plot.py`, which reads only `bench/results/*.csv` and writes light and dark SVG and PNG files to `docs/img/`.

## Variants

`--variant` labels the server build under test (column `variant`); rows of other variants already in the CSVs are kept. The committed results contain `unpatched` (the server before the WebSocket buffer change, built from the parent commit and passed with `--server-bin`), `current` (this tree) and `diag-netstat` (a single 100,000-connection WebSocket attempt kept only for its kernel TCP counter deltas; it was aborted by the memory floor). The SSE server code is identical in `unpatched` and `current`, so both SSE sweeps are independent repetitions of the same code.

Follow-up variants, each one 100,000-connection WebSocket connect phase unless noted: `backlog1024` and `backlog1024-b` (`--listen-backlog 1024`), `backlog4096` (three repeats) and `backlog4096-b` (`--listen-backlog 4096`), `tokio-default-bind` (a build that uses `TcpListener::bind`, listen backlog 128, passed with `--server-bin`), and `sse-extend` (SSE at 75,000, then 100,000 which the memory floor aborted). A first `backlog4096` run (4.4 s connect phase) was overwritten when the label was reused for the three-repeat run; it is not in the CSVs. `--listen-backlog` sets `HYDRANT_LISTEN_BACKLOG` for the server under test.

## Getting past the ephemeral port limit without root

A TCP connection needs a unique (source IP, source port, destination IP, destination port) tuple. With one destination address and port, the number of connections is capped by the ephemeral port range (`net.ipv4.ip_local_port_range`, 40525 to 44620 on the benchmark machine, about 4,096 ports). Every address in 127.0.0.0/8 is local on the loopback interface, so the server listens on `0.0.0.0:<port>` and the loadgen spreads subscribers round-robin over many destination addresses with `--spread-loopback 64` (127.x.y.1 to 127.x.y.64, 64 x about 4,096 ports available). The kernel picks the source port per destination, so every destination address gets its own pool.

Closed connections linger in TIME_WAIT for 60 s and keep their tuple, so every run uses a different /24 (`127.<block>.<run>.1`), where `<block>` is random per invocation.

## What a machine with root would do instead (not applied here)

These settings were NOT changed for this benchmark, and no system configuration was modified:

```sh
ulimit -n 1048576                                   # per-process fd limit (already 1048576 here)
sysctl -w net.ipv4.ip_local_port_range="1024 65535" # more ephemeral ports per destination
sysctl -w net.core.somaxconn=65535                  # accept queue cap (tokio's default bind listens with backlog 128, measured with ss)
sysctl -w net.ipv4.tcp_max_syn_backlog=65535
sysctl -w fs.file-max=2097152
sysctl -w net.ipv4.tcp_tw_reuse=1
```

Even then one destination tops out around 64,000 connections, so spreading over several addresses or ports is still needed past that.

## Safety

The script samples `MemAvailable` four times per second. If it would drop below 1.5 GiB it kills the benchmark processes and records the run as `aborted_memory`. Before each step it predicts the memory need from the previous step's measured cost and skips the step (`skipped_memory_prediction`) if it would not fit. The ladder for a transport stops at the first step that does not finish. Servers and loadgens are killed on exit, failure or Ctrl-C.
