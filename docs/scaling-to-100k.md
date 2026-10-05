# Scaling hydrant to 100,000 connections

On one 32-core WSL2 machine, hydrant accepted up to 99,982 of 100,000 requested idle WebSocket subscribers (99,691 in the worst of ten runs) and lost no deliveries among the connections that were established: 0 of the 33,360,760 deliveries expected over all 41 completed runs were missing. The 18 to 309 connections per 100,000-connection run that never connected are not part of that loss figure. The server process used about 6,470 bytes of RSS per requested WebSocket connection, 618 MiB in total at 100,000. SSE costs about 27,700 to 28,000 bytes per connection and reached 75,000 requested (74,983 established, one run); a 100,000 SSE run was stopped by the benchmark's memory floor before it connected. One thing needs attention before you trust the result: the connect phase at 100,000 connections took between 4.4 and 175 seconds depending on the run, and I do not know why.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="img/rss-vs-connections-dark.svg">
  <img src="img/rss-vs-connections.svg" alt="Line chart of server RSS in MiB against live connections: WebSocket before the fix rises at about 137,400 bytes per connection, SSE at about 27,800, WebSocket after the fix at about 6,470.">
</picture>

| Server build and transport | Largest N completed | Established | Server RSS per requested connection | Latency p50 / p99 | Connect phase |
| --- | --- | --- | --- | --- | --- |
| WebSocket, before buffer fix | 10,000 (3 runs) | 9,999 each | 137,275 to 137,616 bytes | 27 to 29 ms / 48 to 94 ms | 0.83 to 0.96 s |
| WebSocket, after buffer fix | 100,000 (10 runs) | 99,691 to 99,982 | 6,460 to 6,483 bytes (10 runs) | 135 to 168 ms / 271 to 1,063 ms | 4.4 to 175 s |
| SSE | 75,000 (1 run) | 74,983 | 28,005 bytes | 112 ms / 214 ms | 30.7 s |
| SSE | 50,000 (5 runs) | 49,979 to 49,988 | 27,689 to 27,707 bytes | 78 to 95 ms / 151 to 186 ms | 2.0 to 2.8 s |

The before-fix WebSocket server also produced an idle sample at 25,000 connections (137,472 bytes each) before the memory floor stopped that run, so 25,000 is where this machine ran out of RAM for that build, not a measured server limit. All figures come from `bench/results/runs.csv` and `bench/results/sweep.csv`; the 10 runs at 100,000 WebSocket connections include the follow-up experiments described below, which differ in the listen backlog and, as the limits section explains, in the exact binary. "Established" is what the load generator got connected out of the requested N; loss is counted over established connections only.

## What was measured and how

The machine is an Intel i9-13980HX (32 logical cores) with 15.5 GiB of RAM under WSL2, kernel 6.18.33.1-microsoft-standard-WSL2, rustc 1.96.0, release builds. It was also the author's working machine and was in use during the runs, which is why memory deltas from the whole system are noisy and why the benchmark refuses to run below a floor of 1.5 GiB available. The server and the load generator share the machine and its cores, and all traffic is loopback. The run date is in `bench/results/environment.json`.

Each step starts a fresh server process, so memory retained from an earlier step cannot affect a number. The load generator opens N subscribers on one topic, waits until the server's own connection gauge equals the number the client established, holds them idle, then publishes open-loop at 1 message per second for 20 seconds with a 64-byte payload. Every subscriber receives every message, so N connections means N deliveries per second. Latency is measured from the message's intended send time, not from when the publisher actually sent it, so a stalled server shows up as latency instead of being hidden. The sweep covers 1,000, 5,000, 10,000, 25,000, 50,000, 75,000 and 100,000 connections, with repeats at 10,000 and at the largest N reached. All 47 recorded runs are kept: 41 completed, 4 were aborted by the memory floor and 2 were skipped because the predicted memory did not fit.

"RSS per connection" is the server process's resident set at the idle sample, minus its resident set before any connection, divided by the requested N (dividing by the established count instead changes the figure by at most 0.3 percent), read from `/proc/<pid>/status`. It does not include kernel socket memory. The kernel side shows up as growth in unreclaimable slab (the `SUnreclaim` line of `/proc/meminfo`), which covers both ends of each loopback connection: about 5,884 to 6,309 bytes per connection for WebSocket at 100,000 (10 runs) and 6,159 to 6,236 for SSE at 50,000 (3 runs; the slab column was not recorded in the other two). The load generator's own RSS adds about 9,300 bytes per WebSocket connection and 5,200 per SSE connection (its whole RSS divided by N, so it includes fixed overhead). The system-wide drop in `MemAvailable` per connection at 100,000 WebSocket connections was 14,534 to 27,912 bytes (10 runs), which is consistent with those three parts but noisy. TCP buffer memory reported in `/proc/net/sockstat` stayed near zero because idle loopback sockets hold no buffered data.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="img/memory-per-connection-dark.svg">
  <img src="img/memory-per-connection.svg" alt="Horizontal bars of memory per connection for three server configurations, with server RSS, load generator RSS and kernel slab growth shown separately.">
</picture>

## Getting past the port limit without root

A TCP connection needs a unique combination of source address, source port, destination address and destination port. With one destination address and port the ephemeral port range caps connections; on this machine `net.ipv4.ip_local_port_range` is 40525 to 44620, which is 4,096 ports. Every address in 127.0.0.0/8 is local, so the server listens on `0.0.0.0` and the load generator spreads its connections round-robin over 64 loopback destination addresses (`--spread-loopback 64`), each with its own pool of source ports. Closed connections sit in TIME_WAIT for 60 seconds and keep their tuple, so every run uses a fresh /24 block of addresses. No system setting was changed. With root you would instead widen the port range, raise `net.core.somaxconn` and `tcp_max_syn_backlog`, and raise the file limits; `bench/README.md` lists the commands, none of which were applied here.

## Finding 1: a 128 KiB default buffer per WebSocket

As merged, the server cost 137,275 to 137,616 bytes of RSS per WebSocket connection. The tungstenite version in `Cargo.lock` (0.29.0) defaults both `read_buffer_size` and `write_buffer_size` to 128 KiB, and axum's `WebSocketUpgrade` starts from `Default::default()` for that config. Setting `read_buffer_size(4096)` and `write_buffer_size(0)` in `crates/server/src/routes/ws.rs` brought the cost to 6,460 to 6,483 bytes at 100,000 connections (10 runs), about 21 times less. The fitted difference between the two builds, 130,933 bytes, is almost exactly 128 KiB (131,072), so the read buffer accounts for essentially all of it. RSS barely changed between the idle and load samples of the unpatched build (61 to 69 bytes per connection at N = 10,000 in 3 runs, 94 and 82 at 1,000 and 5,000 in one run each), so these runs give no direct evidence about the write buffer; at 1 message per second it was apparently never filled. A test (`websocket_delivers_a_max_size_payload_byte_exact` in `crates/server/tests/fanout.rs`) checks that a 65,536-byte payload still arrives intact with the smaller buffers.

## Finding 2: SSE costs about four times as much as WebSocket after the fix

Per connection the server's RSS is 27,689 to 28,005 bytes for SSE against about 6,480 for the fixed WebSocket handler, a ratio of about 4.3. SSE reached 50,000 in five runs and 75,000 in one. A 100,000 run was started and aborted by the memory floor at 1,471 MiB available while connecting, so SSE at 100,000 is untested, not failed. Why SSE costs more is not measured here. A plausible but unchecked explanation is that an SSE response stays inside hyper's HTTP/1 connection state, with its buffers and a keep-alive timer, while a WebSocket drops out of that machinery after the upgrade; the SSE handler also keeps a channel and a spawned pump task per stream. Treat that as a hypothesis to test with a heap profile.

## Finding 3: the connect-phase cliff, and what was not explained

Up to 75,000 WebSocket connections the connect phase scales smoothly: 3.3 s at 75,000. At 100,000 it took 4.4, 43.8, 47.8, 57.1, 69.4, 97.0, 105.0, 106.2, 150.9 and 174.6 seconds in ten runs. SSE at 75,000 took 30.7 s (8,239 SYN retransmissions), against 2.0 to 2.8 s at 50,000, so the effect is not specific to WebSocket. In the slow ramps (`ramp_ws.csv`, `ramp_ws_backlog4096.csv`, `ramp_ws_tokio-default-bind.csv`) about 65,000 connections are up within 4 to 11 seconds and the remainder crawls in over 34 to 168 more seconds. The fast run (`ramp_ws_backlog1024.csv`) went through the same range in a few seconds.

The first suspect was the listen backlog. tokio's default `TcpListener::bind` listens with a backlog of 128, not the 1,024 an earlier note assumed: mio's listener uses 128 on Linux and tokio's bind goes through it (the tokio documentation only says the value is platform dependent). I added `HYDRANT_LISTEN_BACKLOG` / `--listen-backlog` (default 4096, a larger accept queue chosen as a sane default; the kernel clamps it to `net.core.somaxconn`, which is 4096 here) and reran the 100,000 WebSocket connect phase. I also looked at `ss -ltn` during the session (Send-Q 128 for the default bind, and a request for 100,000 clamped to 4096); those observations are not recorded in the results. No effect of the backlog was detected:

| Listen backlog | Runs | Connect phase | `SynRetrans` delta |
| --- | --- | --- | --- |
| 128 (tokio default bind), current build | 3 runs, no counters recorded | 97.0, 105.0, 106.2 s | not recorded |
| 128 (tokio default bind), rerun | 1 | 43.8 s | 11,833 |
| 1024 | 2 | 4.4 s and 57.1 s | 7 and 10,687 |
| 4096 | 4 | 47.8, 69.4, 150.9, 174.6 s | 13,099, 19,431, 32,722, 32,485 |

This experiment could not separate the settings, for three reasons. There are only 1 to 4 runs per setting, and only one of the ten recorded runs was fast (a second fast run, backlog 4096, was seen on the console during the session, but its row was overwritten when I reused the variant label, so it is not in the CSVs and not counted here). `environment.json` records `net.ipv4.tcp_max_syn_backlog` = 1024, so backlogs 1024 and 4096 cannot differ in SYN-queue size; only the accept queue differs. And across every run that recorded the kernel counters (the backlog and tokio-default runs, the SSE extension and the diagnostic run), `ListenOverflows` and `ListenDrops` stayed at 0, and `ReqQFullDoCookies` was 0 except in the original diagnostic run (1,881, with 29,466 SYN retransmissions, aborted by the memory floor), so the accept queue was not observed to fill at any setting. Slow runs have thousands of SYN retransmissions and fast ones have a handful, so the delay is consistent with SYNs being dropped somewhere and retried with TCP's backoff, but the cause is unexplained. The new setting is harmless and kept, with no measured benefit in this benchmark. I did not test the load generator's `--connect-concurrency`, SYN cookie settings or conntrack.

TIME_WAIT does not fit either. Five of the six 100,000-connection ramps in the CSVs start with the table full (65,509 to 65,536 entries, against a bucket limit of 65,536), including the fast `backlog1024` run (4.4 s), which stays full through its connect phase. The sixth, the slow `backlog1024-b` run (57 s), starts with 31 entries. In the five slow ramps the count falls to between 7 and 46 during the connect phase, while the gauge is still at 75,000 to 86,000 in four of them (the fifth never held a full table), and the remaining connections still take tens of seconds to arrive. A full TIME_WAIT table therefore neither separates fast from slow runs nor persists through the crawl, so these ramps argue against TIME_WAIT as the cause. Why the slow ramps stall near 65,000 connections is unexplained.

One more observation from the ramp files, offered as evidence and not as a proven cause. During the stall the harness's own `/metrics` scrape, which opens a new connection from 127.0.0.1 to the same listener with a 5 s timeout, often failed: in the five slow ramps, 53 of the 171 samples taken after the gauge passed 60,000 have a blank connection gauge, and each blank sits after a gap of 5.6 to 5.8 s between samples. Those gaps cover 298 of the 432 s (69 percent) of that part of the ramps; the fast ramp has no blank gauge. So new connections from an unrelated source address also stalled, which points at the listener side (SYN or accept handling) and away from the load generator's source-port space. Because a stalled sampler records one row per 5.6 s, the stall-duration figures in this section come from sparse samples, not from a continuous measurement.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="img/connect-phase-dark.svg">
  <img src="img/connect-phase.svg" alt="Left: connect-phase seconds against connections requested, log-log, with a cluster of 100,000-connection runs from 4 to 175 seconds. Right: established connections against time for three 100,000-connection runs.">
</picture>

## Latency under fan-out

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="img/latency-vs-connections-dark.svg">
  <img src="img/latency-vs-connections.svg" alt="Two log-log panels of delivery latency p50, p99 and max against connections, one for WebSocket and one for SSE, with every run drawn as a point.">
</picture>

Delivery latency grows with N because each message is written to every subscriber. For the fixed WebSocket handler p50 was 3.6 ms at 1,000 connections, 17 to 18 ms at 10,000, 70 ms at 50,000 and 135 to 168 ms at 100,000; p99 was 9 ms, 31 to 41 ms, 133 ms and 271 to 1,063 ms. SSE tracks the same curve (p50 112 ms and p99 214 ms at 75,000). The spread at 100,000 is not noise around a stable value: six of the ten runs have p99 between 271 and 297 ms, two are 407 and 486 ms, and two are 949 and 1,063 ms, with a worst delivery of 1,904 ms. The chart draws every run, so the outliers are visible rather than averaged away. Because the load generator and server compete for the same 32 cores and the machine was in use, these figures say how this pair behaved together, not how the server alone would behave on dedicated hardware.

## Memory is not returned after teardown

Three seconds after the server's connection gauge returned to zero, its RSS was within about 3 percent of the idle figure, or slightly above it for SSE: 602 to 613 MiB above baseline after 100,000 WebSockets (against 616 to 618 MiB at idle), 1,337 MiB after 50,000 SSE streams and 2,006 MiB after 75,000. This matches the allocator keeping freed memory in the process, but that cause was not checked, and the server was not observed for longer than 3 seconds after teardown. Teardown itself was quick: 1.47 to 1.73 s for 100,000 WebSocket connections.

## What these numbers do not show

They come from one machine, under WSL2, over loopback, with no network latency, packet loss or real NIC. Everything uses one topic, one broadcast ring shared by all subscribers, at 1 message per second with a 64-byte payload, so this is an idle-heavy workload and says nothing about throughput under many topics or high publish rates. There is no TLS and no proxy. Connections were not all established: 18 to 309 of 100,000 failed to connect in each 100,000 run, and smaller counts at lower N, all flagged in `runs.csv`. Latency at 100,000 was measured over 99,691 to 99,982 connections. Most points are single runs; only 10,000 and the largest N have repeats, and the largest N for SSE has none beyond 50,000. The before-fix WebSocket build was only measured up to 25,000 connections, with latency to 10,000. The cause of the connect-phase cliff, why SSE costs more than WebSocket and why memory stays resident after teardown are all open. The load generator's per-connection figure includes its fixed overhead. 

### Build identity and data integrity

The results were produced from an uncommitted working tree on top of commit 4c9191a (`main`): `environment.json` records `git_dirty: true` for every invocation, so no commit reproduces them exactly, and five different server binaries were used, identified only by SHA-256 (first 8 hex digits):

| Variant labels | Binary | Code state |
| --- | --- | --- |
| `unpatched` | 2cbe76a6 | built from the parent commit, before the WebSocket buffer fix |
| `current`, `diag-netstat` | f6581b5d | buffer fix, bound with tokio's `TcpListener::bind` (backlog 128), measured before `HYDRANT_LISTEN_BACKLOG` existed |
| `backlog1024`, `backlog4096` | 4ae88160 | first build with the listen-backlog setting |
| `backlog1024-b`, `backlog4096-b`, `sse-extend` | 31de8383 | a later rebuild with the setting; what changed from 4ae88160 is not recorded |
| `tokio-default-bind` | 3f4af462 | a build using `TcpListener::bind` (backlog 128), passed with `--server-bin` |

The committed default backlog is now 4096, so `make bench-sweep` on the committed tree does not reproduce the recorded `current` configuration exactly: it listens with 4096 where `current` listened with 128. Passing `--listen-backlog 128` is the closest equivalent. Stale-report audit: the four aborted and two skipped rows in `runs.csv` carry no connection, latency or loss values and none has a loadgen JSON report in `raw/`, so no row inherited another run's report; `run.py` now deletes any report for a run's tag before launching the load generator. The `unpatched` binary's build recipe is in the reproduce section. Teardown and idle audit: the runner at the time treated a failed `/metrics` scrape as a gauge of zero, so for each of the 41 `ok` runs the per-run series in `raw/ts_*.csv` was checked from its own scraper, and every run has real scrapes in its teardown phase showing the gauge at zero (none blank; blank scrapes occur only in the `start` and `connecting_start` phases), the `after` sample reads zero from a real scrape with 10 to 26 server file descriptors against a baseline of 10, and the `idle` sample equals the established count in all 41 runs; for the ten 100,000-connection WebSocket runs, which carry the 1.47 to 1.73 s range, the last nonzero scrape also precedes the recorded drain time, so no figure was cut short by a failed scrape. The loadgen exit code was not stored then, but all 41 `ok` runs have a JSON report, at least 20 successful publishes and 0 percent loss, so none is a run the load generator would have rejected (the runner passes no `--max-loss-percent`).

## Follow-up on a second machine

Everything above is the original record from the WSL2 laptop and is left as written. This section reports a later pass on a different machine that tried to answer the three open questions and the SSE gap. Its data lives in `bench/results/cloud/` (same file layout, `cloud-` variant labels, its own `environment.json`) and never mixes with the files above; the charts draw it as hollow markers labelled by machine.

The second machine is a cloud sandbox: 4 logical cores (Intel Xeon at 2.10 GHz), 16.1 GiB of RAM, no swap, kernel 6.18.44 in a virtual machine (not WSL2), rustc 1.96.0, root in the sandbox, release builds from the committed tree (`git_dirty` is recorded per invocation). Client and server share the machine over loopback, as before. The settings that the first write-up suspected were already the same here: `net.ipv4.tcp_max_tw_buckets` 65,536, `net.ipv4.tcp_max_orphans` 65,536, `net.ipv4.tcp_max_syn_backlog` 1,024, `net.core.somaxconn` 4,096, `tcp_syncookies` 1. The differences: `ip_local_port_range` is 32,768 to 60,999 (28,232 ports against 4,096), 4 cores against 32, a plain VM kernel, and one hard limit that shaped the whole pass: the per-process open-file limit is 20,000, hard, and cannot be raised because the sandbox drops `cap_sys_resource` from the capability bounding set even for root. A server process can therefore hold at most about 19,990 sockets, so no experiment here could reach 65,000 connections, let alone 100,000. The largest count that completes is 19,000, and that is the N used for every experiment below.

### Open question 1: the connect-phase stall

Verdict: not reproduced, and could not be reproduced here, because the stall point (about 65,000 connections) is beyond the descriptor limit. What could be done was to measure the listener directly and to test, one factor at a time and five runs each, whether any of the untested candidates produces the recorded signature (thousands of SYN retransmissions, `ListenOverflows` and `ListenDrops` at 0, and a stalled fresh connection from 127.0.0.1) at 19,000 connections. `bench/run.py` now samples the listener's accept-queue depth and the number of SYN_RECV sockets twice a second from `/proc/net/tcp`, and records `TCPMemoryPressures` and softnet drop deltas per run, so the accept side is observed and not inferred. The driver is `bench/experiments-cloud.sh`; it records every sysctl it changes and restores it (one restore failed because of a shell bug, fixed in the same change, and `netdev_max_backlog` was put back by hand, noted in `experiments.log`).

| Variant (`cloud-`) | What changed | Connect phase, 5 runs | `TCPSynRetrans` | `ListenOverflows` / `ListenDrops` | `ReqQFullDoCookies` | Peak accept queue | Peak SYN_RECV |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `ws` | nothing (baseline) | 0.71 to 0.84 s | 0 | 0 / 0 | 0 | 79 | 0 |
| `ws-ports4096` | port range 40525 to 44620 as on the laptop, 64 addresses (297 ports per destination) | 0.72 to 0.82 s | 0 | 0 / 0 | 0 | 128 | 0 |
| `ws-ports4096-spread16` | same range, 16 addresses (1,188 ports per destination, 29 percent, the laptop's occupancy at 65,000) | 0.71 to 0.76 s | 0 | 0 / 0 | 0 | 128 | 0 |
| `ws-ports4096-spread8` | 8 addresses (2,375 per destination, 58 percent) | 0.77 to 0.86 s | 0 | 0 / 0 | 0 | 128 | 0 |
| `ws-ports4096-spread5` | 5 addresses (3,800 per destination, 93 percent) | 0.90 to 1.02 s | 0 | 0 / 0 | 0 | 74 | 0 |
| `ws-concurrency16` | loadgen `--connect-concurrency 16` | 0.96 to 1.09 s | 0 | 0 / 0 | 0 | 12 | 1 |
| `ws-concurrency1024` | `--connect-concurrency 1024` | 0.57 to 0.67 s | 0 | 0 / 0 | 0 | 667 | 0 |
| `ws-backlog16` | server listen backlog 16 | 1.26 to 2.50 s | 111 to 206 | 199 to 371 / same | 75 to 285 | 17 | 5 |
| `ws-syncookies0` | `tcp_syncookies` 0 | 0.66 to 0.75 s | 0 | 0 / 0 | 0 | 128 | 0 |
| `ws-syncookies0-synbacklog64` | cookies 0, `tcp_max_syn_backlog` 64, listen backlog 64 | 1.25 to 2.13 s | 63 to 75 | 65 to 74 / 65 to 84 | 0 | 65 | 1 |
| `ws-conntrack12000` | conntrack hooked on INPUT, `nf_conntrack_max` 12,000 (1 run, aborted by the connect timeout) | never completed | 7,808 | 0 / 0 | 0 | 72 | 0 |
| `ws-netdevbacklog8` | `netdev_max_backlog` 8 | 0.71 to 0.82 s | 0 | 0 / 0 | 0 | 110 | 0 |

What this says. The source-port search is not the mechanism: filling each destination's 4,096-port pool to 93 percent costs about 0.2 s over 19,000 connects and produces no retransmission at all, so the laptop's 25 percent occupancy at 65,000 cannot explain a stall. The server's accept loop keeps up: with the default concurrency the accept queue never exceeded 128 entries of 4,096 and SYN_RECV stayed at 0 or 1; raising the load generator's concurrency to 1,024 pushed the queue to 667 and made the connect phase faster, not slower. Accept-queue overflow (backlog 16, or a 64-entry SYN queue without cookies) does cause SYN retransmissions and a two to three times longer connect phase, but every such run also shows `ListenOverflows` and `ListenDrops` in the hundreds, which the laptop's slow runs never did, so the laptop's listener was not overflowing. Softnet drops on loopback (thousands per run with an 8-packet backlog) cost nothing visible at this scale and produce no SYN retransmission. TCP memory pressure never occurred (`TCPMemoryPressures` 0 in every run, kernel socket memory near zero as before). The one setting that reproduced the recorded counter signature exactly, thousands of SYN retransmissions with both listen counters at 0 and a new connection from 127.0.0.1 blocked, was a full connection-tracking table: the gauge stopped dead at 11,968 of 12,000 entries and the kernel logged `nf_conntrack: table full, dropping packet`. That is a demonstration that conntrack can produce the signature, not evidence that it did on the laptop: the shape differs (a hard stop here against a crawl of a few hundred per second there), and nothing was recorded about conntrack on the laptop. It is the first thing to check there: `cat /proc/sys/net/netfilter/nf_conntrack_max` and `nf_conntrack_count` during a run, and `dmesg | grep conntrack` after a slow one. Many distributions and Docker Desktop on WSL2 load conntrack with a 65,536-entry table, and entries in TIME_WAIT live 120 s, which would also fit the 65,000 stall point and the next run's slow start; `environment.json` now records both values on every invocation.

Still open: the cause. Reaching 65,000 connections on one host needs a process limit above 65,000 descriptors, which this sandbox cannot grant, so the direct reproduction and the counter deltas at the stall remain to be taken on a machine that can. The listener sampling, the extra counters and the driver are in place for that run.

### Open question 2: why SSE costs four times WebSocket

Verdict: measured, and about a fifth of it removed. Heap profiles with heaptrack (`bench/heap-profile.sh`, a release build with line tables, 10,000 idle connections of one transport, summaries in `bench/results/cloud/heap/`) attribute the live heap at its peak as follows, in bytes per connection:

| Site | WebSocket | SSE |
| --- | --- | --- |
| hyper HTTP/1 read buffer (`BytesMut` reserved in `poll_read_head`: 8 KiB at first, grown once to 16 KiB by an amortised reserve) | 0 (freed at the upgrade) | 16,426 |
| hyper HTTP/1 write buffer (`Vec::with_capacity(8192)` in `Buffered::new`) | 0 (freed at the upgrade) | 8,193 |
| axum route table rebuilt per connection (`Router::with_state` inside the make-service that `axum::serve` wraps a bare `Router` in) | 0 (freed when the connection task ends at the upgrade) | 3,501 |
| tungstenite read buffer (`BytesMut::with_capacity(4096)`) | 4,096 | 0 |
| pump task (the spawned future) | 1,536 | 768 |
| tokio `mpsc::channel(1)` for the SSE pump (a 32-slot block plus the channel) | 0 | 2,080 |
| http `HeaderMap` and `Extensions` of the request and response | 0 | 656 |
| tokio I/O registration | 256 | 256 |
| everything else (hyper connection task, runtime, hub, topic name) | 302 | 3,656 |
| live heap at peak | 6,193 | 35,535 |
| RSS at idle minus baseline, same run | 6,513 | 27,705 |

For WebSocket the heap and the RSS agree to within 5 percent. For SSE the heap exceeds the RSS by about 8 KB because the 16 KiB read buffer is reserved but only its first part is ever written, so half of it never becomes resident; with that correction the attributed total and the RSS agree to within about 1 KB per connection. The hypotheses in Finding 2 were therefore right about hyper's HTTP/1 state (24 KiB allocated, about 16 KiB resident, per idle SSE stream: 60 to 70 percent of the cost) and about the channel and the pump task (about 2.8 KB together); the keep-alive timer and the tracing span are too small to show (the request span is filtered out at the `warn` log level the benchmark uses). What nobody had guessed is the route table: `axum::serve(listener, router)` wraps a bare `Router` in a make-service whose `call` runs `self.clone().with_state(())` for every accepted connection, rebuilding the route hash table and keeping that copy alive as long as the connection, which for an SSE stream is forever. `Router::into_make_service()` does that conversion once at startup and clones an `Arc` per connection instead. That change is in `crates/server/src/lib.rs`; it changes no routing, no middleware order and no shutdown behaviour, and the existing tests pass. Measured with `bench/run.py` at 10,000 connections, five runs each (variants `cloud-sse`, `cloud-sse-makeservice`, `cloud-ws`, `cloud-ws-makeservice`):

| Server | SSE RSS per connection | WebSocket RSS per connection |
| --- | --- | --- |
| before (`cloud-*`) | 27,707 to 27,709 bytes | 6,529 to 6,547 bytes |
| after (`cloud-*-makeservice`) | 22,521 to 22,524 bytes | 6,488 to 6,495 bytes |

That is 5,186 bytes (18.7 percent) less per SSE stream and nothing measurable for WebSocket, and a second heap profile of the new build shows the route-table site at 0 bytes and the SSE live heap at 30,575 bytes per connection. The remaining gap to WebSocket is hyper's two buffers, which cannot be shrunk from this code: hyper's server builder has no read-buffer-size setting, its `max_buf_size` floor is 8 KiB and does not stop the amortised growth, and the 8 KiB write buffer is a fixed `Vec::with_capacity`. Replacing `axum::serve` with a hand-written accept loop would therefore buy nothing here, so it was not done. The `mpsc::channel(1)` block (about 2 KB) could be replaced by a hand-written one-slot handoff, but that is new synchronisation code in the path that the stall-protection test guards, for about 7 percent of the remaining cost, and was left alone. Both profiles were taken on the cloud sandbox; the per-connection RSS it measures (6.5 KB and 27.7 KB before the change) matches the laptop's figures to within 1 percent, so the attribution should carry over.

### Open question 3: memory not returned after teardown

Verdict: allocator retention, not a leak. `bench/teardown_cycles.py` connects 15,000 WebSockets to one server process, holds them, disconnects, watches RSS and `RssAnon` for 60 s, and repeats; five cycles per setting, output in `bench/results/cloud/teardown/`. With glibc 2.39 defaults the server's RSS after each teardown was 98.2, 103.6, 104.2, 103.7 and 103.8 MiB against a baseline of 4.9 MiB and a peak of 100 to 107 MiB during the cycle: a plateau reached in the first cycle, not growth proportional to cycles. Through gdb, glibc's `malloc_stats()` after each cycle reported 279, 290, 315, 329 and 334 KB in use against 93 to 99 MB of "system bytes" in five arenas, so the process holds a few hundred kilobytes of live allocations and about 95 MiB of freed memory the allocator has not returned; the 55 KB drift in the in-use figure over five cycles is under 4 bytes per connection and could not be separated from the hub's and runtime's own bookkeeping. A forced `malloc_trim(0)` (gdb, after a one-off 15,000-connection cycle) took RSS from 99.9 MiB to 6.8 MiB at once, which is the decisive sign of retention: the memory is free inside the process and goes back to the kernel the moment the allocator is asked. RSS was never observed to fall on its own within 60 s. Settings tried, five cycles each: `MALLOC_ARENA_MAX=1` gives the flattest line (97.8 to 98.2 MiB after each teardown, the lowest of the four) because one arena fragments less than five, but returns nothing either; `MALLOC_TRIM_THRESHOLD_=131072` with `MALLOC_TOP_PAD_=0` changes nothing (99.8 to 106.3 MiB) because trimming only releases the top of each arena's heap and the freed chunks are spread through it; jemalloc 5 loaded with `LD_PRELOAD` behaves the same (97.9 to 101.4 MiB after four of the five teardowns, 84.9 MiB once), since jemalloc's page purging runs on allocation activity and an idle server allocates nothing. No alternative allocator was added as a cargo feature because none behaved materially better in this test. If returning memory after a drop in connections matters for a deployment, the honest options are a periodic `malloc_trim(0)` call (glibc only, a few milliseconds here) or jemalloc with `background_thread:true`; neither was tested at 100,000 connections and neither is shipped.

### SSE at 100,000 connections

Not run: the sandbox's 20,000-descriptor hard limit stops any process at about 19,990 sockets, so the largest count that completed here is 19,000 for both transports, five runs each with the same workload as the sweep (one topic, 1 message per second for 20 s, 64-byte payload, fresh server per step), variants `cloud-ws` and `cloud-sse`:

| Transport, N = 19,000 | Established | Connect phase | Server RSS per connection | Latency p50 / p99 | Teardown |
| --- | --- | --- | --- | --- | --- |
| WebSocket | 19,000 in all 5 runs | 0.71 to 0.84 s | 6,494 to 6,502 bytes | 48.5 to 51.0 ms / 101 to 106 ms | 0.33 to 0.38 s |
| SSE | 19,000 in all 5 runs | 0.58 to 0.65 s | 27,705 to 27,707 bytes | 57.2 to 60.1 ms / 109 to 117 ms | 0.27 to 0.37 s |
| WebSocket, server with the make-service change (3 runs) | 19,000 in all 3 | 0.59 to 0.62 s | 6,477 to 6,479 bytes | 48.0 to 49.3 ms / 93 to 99 ms | 0.34 to 0.38 s |
| SSE, server with the make-service change (3 runs) | 19,000 in all 3 | 0.59 to 0.79 s | 22,520 to 22,524 bytes | 58.5 to 61.3 ms / 113 to 137 ms | 0.34 to 0.35 s |

Loss was 0 in all 36 runs at 10,000 and 19,000 (variants `cloud-ws`, `cloud-sse`, `cloud-*-makeservice` and `cloud-*-makeservice-top`). The per-connection figures reproduce the laptop's to within 1 percent on a different kernel and allocator, so the memory claims do not depend on WSL2. The 100,000 SSE run remains to be taken on a machine with at least 100,000 descriptors per process and about 2.3 GiB free for the server with the make-service change (2.8 GiB without it), plus about 0.5 GiB for the load generator.

### Leaked local paths in the history

`bench/results/environment.json` carried absolute paths (a home directory and a session-scoped temporary directory) in four commits from b5d4a13 to 0cbe65e; every blob in every ref was searched and nothing else matched. `tools/scrub-history.sh` rewrites a fresh mirror clone with `git filter-repo` text replacement, limited to the commits after 4c9191a so that the 23 earlier commits keep their hashes and SSH signatures, and prints a verification report. The rewritten `main` is pushed as the branch `scrubbed/main` for inspection; the force push of `main` itself is the owner's step and is described in the pull request that added the script.

## How to reproduce

```sh
make build
make bench-sweep                                        # ws and sse, 1,000 to 100,000 (backlog 4096 on the committed tree)
python3 bench/run.py --variant unpatched --server-bin /path/to/older/hydrant-server
python3 bench/run.py --variant backlog4096 --listen-backlog 4096 --transports ws --ladder 100000 --no-repeat
python3 bench/run.py --variant sse-extend --transports sse --ladder 75000 100000 --no-repeat
make bench-plot                                         # charts into docs/img/
```

The sweep needs about 4 GiB of free memory for the 100,000 WebSocket step and more for SSE; steps that would leave less than 1.5 GiB available are skipped or aborted and recorded as such. The pre-fix binary is the server built with the two `read_buffer_size` and `write_buffer_size` calls removed from `crates/server/src/routes/ws.rs`. Full column descriptions and the safety rules are in `bench/README.md`. `run.py` refuses to reuse a variant label that already has rows; pick a new label or pass `--replace-variant`.
