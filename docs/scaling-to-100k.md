# Scaling hydrant to 100,000 connections

hydrant accepted up to 99,982 of 100,000 requested idle WebSocket subscribers on one 32-core WSL2 machine (99,691 in the worst of ten runs) and lost no deliveries among the connections that were established: 0 of 33,360,760 expected over all 41 completed runs. The 18 to 309 connections per 100,000-connection run that never connected are not in that loss figure. The server used 6,460 to 6,483 bytes of RSS per requested WebSocket connection, 616 to 618 MiB in total at 100,000. SSE cost 27,689 to 28,005 bytes per connection and reached 75,000 requested; a 100,000 SSE run was stopped by the benchmark's memory floor before it connected. A later fix to the router cut SSE to 22,520 to 22,524 bytes (measured at 10,000 and 19,000 connections on a second machine). One thing needs attention before you trust the result: the connect phase at 100,000 connections took between 4.4 and 175 seconds depending on the run, and I do not know why.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="img/rss-vs-connections-dark.svg">
  <img src="img/rss-vs-connections.svg" alt="Line chart of server RSS in MiB against live connections: WebSocket before the fix rises at about 137,400 bytes per connection, SSE at about 27,800, WebSocket after the fix at about 6,470.">
</picture>

| Finding | Verdict |
| --- | --- |
| 1. A 128 KiB default read buffer per WebSocket made it cost 137 KB; fixed | confirmed |
| 2. SSE costs about 4 times WebSocket; hyper buffers are 60 to 70 percent of it, and a per-connection route table (18.7 percent of SSE RSS) was removed | confirmed, partly fixed |
| 3. The connect phase at 100,000 connections stalls for 4.4 to 175 s | open |
| 4. Latency grows with N; the p99 spread at 100,000 is not noise | measured |
| 5. RSS stays near its peak after teardown | confirmed: allocator retention, not a leak |
| 6. SSE at 100,000 connections | not run to completion |

## Setup

| | Laptop | Cloud sandbox |
| --- | --- | --- |
| CPU | Intel i9-13980HX, 32 logical cores | Intel Xeon 2.10 GHz, 4 logical cores |
| RAM | 15.5 GiB (swap 4 GiB) | 15.7 GiB, no swap |
| Kernel | 6.18.33.1-microsoft-standard-WSL2 | 6.18.44-fc-v70, a VM |
| Per-process file limit | 1,048,576 | 20,000, hard (about 19,990 sockets) |
| `ip_local_port_range` | 40525 to 44620 (4,096 ports) | 32768 to 60999 (28,232 ports) |
| Root | no | yes |
| Data | `bench/results/` | `bench/results/cloud/`, variants `cloud-*` |

Both have rustc 1.96.0, release builds, `tcp_max_tw_buckets` 65,536, `tcp_max_syn_backlog` 1,024 and `somaxconn` 4,096; the sandbox also records `tcp_max_orphans` 65,536 and `tcp_syncookies` 1 (not recorded for the laptop). Server and load generator share the machine and its cores over loopback. The laptop was my working machine and in use during the runs, so system-wide memory deltas are noisy, and the benchmark refuses to run below 1.5 GiB available. The sandbox's file limit could not be raised even as root; I believe it drops `cap_sys_resource`, but I did not record that. Charts draw sandbox rows as hollow markers; the two data sets never mix.

**Method.** Each step starts a fresh server. The load generator opens N subscribers on one topic, waits until the server's connection gauge equals the number it established, holds them idle, then publishes open-loop at 1 message per second for 20 s with a 64-byte payload, so N connections get N deliveries per second. Latency runs from each message's intended send time, so a stalled server shows up as latency. The laptop sweep covers 1,000 to 100,000 with repeats at 10,000 and at the largest N; all 47 runs are kept (41 completed, 4 aborted by the memory floor, 2 skipped because predicted memory did not fit). The sandbox sweep stops at 19,000 (91 runs: 90 ok, 1 connect failure in the conntrack experiment). RSS per connection is the server's `VmRSS` at the idle sample minus its RSS before any connection, over the requested N (using the established count changes it by at most 0.3 percent); it excludes kernel socket memory.

| Server build and transport (laptop) | Largest N | Established | RSS per requested connection | Latency p50 / p99 | Connect phase |
| --- | --- | --- | --- | --- | --- |
| WebSocket, before buffer fix | 10,000 (3 runs) | 9,999 each | 137,275 to 137,616 bytes | 27 to 29 ms / 48 to 94 ms | 0.83 to 0.96 s |
| WebSocket, after buffer fix | 100,000 (10 runs) | 99,691 to 99,982 | 6,460 to 6,483 bytes | 135 to 168 ms / 271 to 1,063 ms | 4.4 to 175 s |
| SSE | 75,000 (1 run) | 74,983 | 28,005 bytes | 112 ms / 214 ms | 30.7 s |
| SSE | 50,000 (5 runs) | 49,979 to 49,988 | 27,689 to 27,707 bytes | 78 to 95 ms / 151 to 186 ms | 2.0 to 2.8 s |

The before-fix build also gave 139,715 and 137,869 bytes at 1,000 and 5,000 and 137,472 at 25,000, where the memory floor stopped it: that is where the laptop ran out of RAM, not a server limit. The 100,000 rows include the backlog experiments of Finding 3, whose binaries differ (see Build identity).

**Kernel and client memory (laptop).** Unreclaimable slab growth (both loopback ends) was 5,884 to 6,309 bytes per connection for WebSocket at 100,000 (10 runs) and 6,159 to 6,236 for SSE at 50,000 (3 runs; not recorded for the other two). The load generator's RSS adds 9,270 to 9,304 bytes per WebSocket connection and 5,161 to 5,179 per SSE connection (whole RSS over N, so it includes fixed overhead). The drop in `MemAvailable` per connection at 100,000 WebSocket was 14,534 to 27,912 bytes, consistent but noisy. TCP buffer memory stayed near zero.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="img/memory-per-connection-dark.svg">
  <img src="img/memory-per-connection.svg" alt="Horizontal bars of memory per connection for three server configurations, with server RSS, load generator RSS and kernel slab growth shown separately.">
</picture>

**Past the port limit without root.** A connection needs a unique (source address, source port, destination address, destination port), so one destination caps the laptop at its 4,096 ephemeral ports. Every address in 127.0.0.0/8 is local, so the server listens on `0.0.0.0` and the load generator spreads connections over 64 destination addresses (`--spread-loopback 64`), each with its own source-port pool. Closed connections hold their tuple in TIME_WAIT for 60 s, so every run uses a fresh /24. No system setting was changed; `bench/README.md` lists what root would change.

## Finding 1: a 128 KiB default buffer per WebSocket (confirmed)

As merged, WebSocket cost 137,275 to 137,616 bytes per connection. tungstenite 0.29.0 defaults `read_buffer_size` and `write_buffer_size` to 128 KiB and axum's `WebSocketUpgrade` starts from that default. Setting `read_buffer_size(4096)` and `write_buffer_size(0)` in `crates/server/src/routes/ws.rs` gave 6,460 to 6,483 bytes at 100,000 (10 runs), about 21 times less. The fitted difference, 130,933 bytes, is almost exactly 128 KiB (131,072), so the read buffer is essentially all of it. The unpatched build's RSS barely moved between idle and load (61 to 69 bytes per connection at 10,000 in 3 runs; 94 and 82 at 1,000 and 5,000 in one run each), so I have no direct evidence about the write buffer; at 1 message per second it was apparently never filled. `websocket_delivers_a_max_size_payload_byte_exact` (`crates/server/tests/fanout.rs`) checks that a 65,536-byte payload still arrives intact.

## Finding 2: SSE costs about four times WebSocket (confirmed, partly fixed)

SSE is 27,689 to 28,005 bytes per connection against about 6,480 for WebSocket, a ratio of about 4.3. I first guessed hyper's HTTP/1 state, a channel and a pump task. Heap profiles (heaptrack, `bench/heap-profile.sh`, 10,000 idle connections of one transport, cloud sandbox, one profile each, summaries in `bench/results/cloud/heap/`) show live heap at peak per connection in bytes:

| Site | WebSocket | SSE |
| --- | --- | --- |
| hyper read buffer (`BytesMut` in `poll_read_head`, 8 KiB growing once to 16 KiB) | 0 (freed at upgrade) | 16,426 |
| hyper write buffer (`Vec::with_capacity(8192)`) | 0 (freed at upgrade) | 8,193 |
| axum route table rebuilt per connection (`Router::with_state` in the make-service `axum::serve` wraps a bare `Router` in) | 0 | 3,501 |
| tungstenite read buffer | 4,096 | 0 |
| pump task | 1,536 | 768 |
| tokio `mpsc::channel(1)` for the SSE pump | 0 | 2,080 |
| `HeaderMap` and `Extensions` | 0 | 656 |
| tokio I/O registration | 256 | 256 |
| everything else | 302 | 3,656 |
| live heap at peak | 6,193 | 35,535 |
| RSS at idle minus baseline, same run | 6,513 | 27,705 |

For WebSocket heap and RSS agree within 5 percent. For SSE the heap is about 8 KB above RSS because half of the 16 KiB read buffer is reserved but never written; corrected, they agree within about 1 KB. So hyper's buffers (24 KiB allocated, about 16 KiB resident) are 60 to 70 percent of the SSE cost and the channel plus pump task about 2.8 KB. The keep-alive timer and tracing span are too small to show (the request span is filtered at the benchmark's `warn` level). The route table was the surprise: `axum::serve(listener, router)` runs `self.clone().with_state(())` for every accepted connection and keeps that copy for the connection's life, which for SSE is forever. `Router::into_make_service()` does it once at startup (`crates/server/src/lib.rs`); routing, middleware order, shutdown and the existing tests are unchanged. Five runs each at 10,000 connections on the sandbox:

| 10,000 connections, 5 runs each (cloud) | SSE RSS per connection | WebSocket RSS per connection |
| --- | --- | --- |
| before (`cloud-sse`, `cloud-ws`) | 27,707 to 27,709 bytes | 6,529 to 6,547 bytes |
| after (`*-makeservice`) | 22,521 to 22,524 bytes | 6,488 to 6,495 bytes |

That is 5,186 bytes (18.7 percent) less per SSE stream and nothing measurable for WebSocket; a second profile shows the route-table site at 0 and SSE live heap at 30,575 bytes. The rest of the gap is hyper's two buffers, which I cannot shrink from this code: its server builder has no read-buffer setting, its 8 KiB `max_buf_size` floor does not stop the amortised growth, and the write buffer is a fixed `Vec::with_capacity`. A hand-written accept loop would buy nothing. Replacing the `mpsc::channel(1)` (about 2 KB, 7 percent of what remains) would add synchronisation code in the path the stall-protection test guards, so I left it. I profiled only on the sandbox, whose RSS per connection matches the laptop's within 1 percent, so I expect the attribution to carry over. SSE latency at 10,000 with the new build was noisier (p50 32 to 47 ms, p99 66 to 152 ms in 5 runs, against 30 to 32 and 56 to 61 before); I did not investigate.

## Finding 3: the connect-phase stall (open)

Up to 75,000 WebSocket connections the connect phase is smooth (3.3 s at 75,000). At 100,000 it took 4.4, 43.8, 47.8, 57.1, 69.4, 97.0, 105.0, 106.2, 150.9 and 174.6 s in ten runs. SSE at 75,000 took 30.7 s with 8,239 SYN retransmissions (2.0 to 2.8 s at 50,000), so it is not WebSocket-specific. In the five slow ramp files the gauge first reads 60,000 or more after 3.8 to 15.7 s and the rest takes most of the connect phase; the fast ramp (`ramp_ws_backlog1024.csv`) had a 4.4 s connect phase.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="img/connect-phase-dark.svg">
  <img src="img/connect-phase.svg" alt="Left: connect-phase seconds against connections requested, log-log, with a cluster of 100,000-connection runs from 4 to 175 seconds. Right: established connections against time for three 100,000-connection runs.">
</picture>

**Listen backlog (laptop): no effect detected.** tokio's default bind listens with a backlog of 128 (mio's value; the tokio docs only say platform dependent), not the 1,024 an earlier note assumed. I added `HYDRANT_LISTEN_BACKLOG` (default 4096, clamped to `somaxconn`) and reran 100,000 WebSocket. The `ss -ltn` observations of the 128 and the clamp were not recorded.

| Listen backlog | Runs | Connect phase | `SynRetrans` delta |
| --- | --- | --- | --- |
| 128 (tokio default bind), current build | 3, counters not recorded | 97.0, 105.0, 106.2 s | not recorded |
| 128 (tokio default bind), rerun | 1 | 43.8 s | 11,833 |
| 1024 | 2 | 4.4 s and 57.1 s | 7 and 10,687 |
| 4096 | 4 | 47.8, 69.4, 150.9, 174.6 s | 13,099, 19,431, 32,722, 32,485 |

This cannot separate the settings: 1 to 4 runs each, one fast run of ten (a second fast 4096 run was seen on the console but its row was overwritten when I reused the label), and `tcp_max_syn_backlog` is 1,024, so 1024 and 4096 differ only in accept-queue size. Wherever counters were recorded `ListenOverflows` and `ListenDrops` stayed 0, and `ReqQFullDoCookies` was 0 except in the diagnostic run the memory floor aborted (1,881, with 29,466 SYN retransmissions). Slow runs have thousands of SYN retransmissions and fast ones a handful, which fits SYNs dropped somewhere and retried with backoff. I did not test `--connect-concurrency`, SYN cookies or conntrack on the laptop.

**TIME_WAIT does not fit.** Five of six 100,000 ramps start with the table full (65,509 to 65,536 of 65,536), including the fast one, which stays full. The sixth (`backlog1024-b`, 57 s) starts at 38. In the four slow ramps that started full, the count falls to 7 to 46 while the gauge is at 82,673 to 89,610, and the last connections still take tens of seconds.

**Unrelated connections stall too.** The harness's `/metrics` scrape opens a fresh connection from 127.0.0.1 with a 5 s timeout. In the five slow ramps 53 of the 171 samples after the gauge passed 60,000 are blank, each after a 5.6 to 5.8 s gap, covering 298 of those 432 s (69 percent); the fast ramp has none. That points at the listener side (SYN or accept handling), away from the load generator's source ports. Stall durations come from sparse samples.

**Sandbox experiments (N = 19,000, 5 runs each).** The stall point is beyond the sandbox's descriptor limit, so I could not reproduce it. Instead I changed one factor at a time and checked for the recorded signature: thousands of SYN retransmissions, `ListenOverflows` and `ListenDrops` at 0, a stalled fresh connection from 127.0.0.1. `bench/run.py` now samples accept-queue depth and SYN_RECV twice a second from `/proc/net/tcp` and records `TCPMemoryPressures` and softnet drops; `bench/experiments-cloud.sh` records and restores every sysctl it changes (one restore failed on a shell bug, since fixed; `netdev_max_backlog` was restored by hand, noted in `experiments.log`).

| Variant (`cloud-`) | What changed | Connect phase | `TCPSynRetrans` | `ListenOverflows` / `ListenDrops` | `ReqQFullDoCookies` | Peak accept queue | Peak SYN_RECV |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `ws` | baseline | 0.71 to 0.84 s | 0 | 0 / 0 | 0 | 79 | 0 |
| `ws-ports4096` | laptop port range, 64 addresses (297 ports each) | 0.72 to 0.82 s | 0 | 0 / 0 | 0 | 128 | 0 |
| `ws-ports4096-spread16` | 16 addresses (1,188 each, 29 percent of the pool) | 0.71 to 0.76 s | 0 | 0 / 0 | 0 | 128 | 0 |
| `ws-ports4096-spread8` | 8 addresses (2,375, 58 percent) | 0.77 to 0.86 s | 0 | 0 / 0 | 0 | 128 | 0 |
| `ws-ports4096-spread5` | 5 addresses (3,800, 93 percent) | 0.90 to 1.02 s | 0 | 0 / 0 | 0 | 74 | 0 |
| `ws-concurrency16` | `--connect-concurrency 16` | 0.96 to 1.09 s | 0 | 0 / 0 | 0 | 12 | 1 |
| `ws-concurrency1024` | `--connect-concurrency 1024` | 0.57 to 0.67 s | 0 | 0 / 0 | 0 | 667 | 0 |
| `ws-backlog16` | listen backlog 16 | 1.26 to 2.50 s | 111 to 206 | 199 to 371 / same | 75 to 285 | 17 | 5 |
| `ws-syncookies0` | `tcp_syncookies` 0 | 0.66 to 0.75 s | 0 | 0 / 0 | 0 | 128 | 0 |
| `ws-syncookies0-synbacklog64` | cookies 0, `tcp_max_syn_backlog` 64, backlog 64 | 1.25 to 2.13 s | 63 to 75 | 65 to 74 / 65 to 84 | 0 | 65 | 1 |
| `ws-conntrack12000` | conntrack on INPUT, max 12,000 (1 run, aborted by the connect timeout) | never completed | 7,808 | 0 / 0 | 0 | 72 | 0 |
| `ws-netdevbacklog8` | `netdev_max_backlog` 8 | 0.71 to 0.82 s | 0 | 0 / 0 | 0 | 110 | 0 |

- Source-port search is not the mechanism: a 93 percent full pool costs about 0.2 s over 19,000 connects with no retransmission, and the laptop was at about 25 percent at 65,000 connections.
- The accept loop keeps up: with default concurrency the queue never passed 128 of 4,096, and concurrency 1,024 pushed it to 667 and connected faster.
- Overflowing a queue (backlog 16, or a 64-entry SYN queue without cookies) causes SYN retransmissions and a two to three times longer connect phase, but also hundreds of `ListenOverflows` and `ListenDrops`, which the laptop's slow runs never showed. So its listener was not overflowing.
- Softnet drops (3,779 to 22,724 per run at `netdev_max_backlog` 8) and TCP memory pressure (0 in every sandbox run) did nothing visible.
- Only a full conntrack table reproduced the signature: the gauge stopped dead at 11,968 of 12,000 and the kernel logged `nf_conntrack: table full, dropping packet`. That shows conntrack can produce it, not that it did on the laptop: the shape differs (a hard stop against a crawl), and I recorded nothing about conntrack there.

Still open: the cause. First check on the laptop `nf_conntrack_max` and `nf_conntrack_count` during a run and `dmesg | grep conntrack` after a slow one (`environment.json` now records both on every invocation). Reproducing needs a process limit above 65,000 descriptors, which the sandbox cannot grant; the sampling and driver are ready.

## Finding 4: latency under fan-out (measured)

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="img/latency-vs-connections-dark.svg">
  <img src="img/latency-vs-connections.svg" alt="Two log-log panels of delivery latency p50, p99 and max against connections, one for WebSocket and one for SSE, with every run drawn as a point.">
</picture>

Each message is written to every subscriber, so latency grows with N. Laptop, fixed WebSocket handler: p50 3.6 ms at 1,000, 17.0 to 17.6 at 10,000 (3 runs), 69.6 at 50,000, 116.4 at 75,000, 134.8 to 167.6 at 100,000 (10 runs); p99 9.2, 31.5 to 40.9, 133.2, 350.0 and 271 to 1,063 ms. SSE tracks the same curve (p50 112 ms, p99 214 ms at 75,000). The 100,000 spread is not noise around a stable value: six of ten runs have p99 of 271 to 297 ms, two 407 and 486, two 949 and 1,063, and the worst delivery was 1,904 ms. The chart draws every run. The load generator and server compete for the same cores, so this describes the pair, not the server alone. Sandbox at 19,000 connections, 5 runs each unless noted:

| Transport, N = 19,000 | Established | Connect phase | RSS per connection | Latency p50 / p99 | Teardown |
| --- | --- | --- | --- | --- | --- |
| WebSocket | 19,000 in all | 0.71 to 0.84 s | 6,494 to 6,502 bytes | 48.5 to 51.0 ms / 101 to 106 ms | 0.33 to 0.38 s |
| SSE | 19,000 in all | 0.58 to 0.65 s | 27,705 to 27,707 bytes | 57.2 to 60.1 ms / 109 to 117 ms | 0.27 to 0.37 s |
| WebSocket, router fix (3 runs) | 19,000 in all | 0.59 to 0.62 s | 6,477 to 6,479 bytes | 48.0 to 49.3 ms / 93 to 99 ms | 0.34 to 0.38 s |
| SSE, router fix (3 runs) | 19,000 in all | 0.59 to 0.79 s | 22,520 to 22,524 bytes | 58.5 to 61.3 ms / 113 to 137 ms | 0.34 to 0.35 s |

Loss was 0 in all 36 sandbox runs at 10,000 and 19,000. Per-connection memory matches the laptop's within 1 percent on a different kernel and allocator, so it does not depend on WSL2.

## Finding 5: memory is not returned after teardown (confirmed: retention, not a leak)

Laptop: three seconds after the gauge returned to zero, RSS was within about 3 percent of the idle figure (slightly above it for SSE): 602 to 613 MiB above baseline after 100,000 WebSockets (616 to 618 at idle), 1,337 MiB after 50,000 SSE and 2,006 after 75,000. Teardown took 1.47 to 1.73 s at 100,000 WebSocket.

Sandbox: `bench/teardown_cycles.py` connects 15,000 WebSockets to one server, holds, disconnects, watches RSS and `RssAnon` for 60 s and repeats, 5 cycles per setting (`bench/results/cloud/teardown/`). With glibc 2.39 defaults RSS 60 s after each teardown was 98,244, 103,640, 104,244, 103,748 and 103,760 kB against a baseline of 4,952 kB and peaks of 100,244 to 107,052 kB: a plateau from the first cycle, not growth. Through gdb, `malloc_stats()` showed 279 to 334 kB in use against 93,456 to 99,492 kB of system bytes; the 55 kB drift in use is under 4 bytes per connection and I could not separate it from the hub's and runtime's bookkeeping. A forced `malloc_trim(0)` took RSS from 99,892 kB to 6,760 kB at once, so the memory is free inside the process. RSS never fell by itself within 60 s; I did not watch longer.

| Setting (5 cycles each) | RSS 60 s after teardown | Why it does not help |
| --- | --- | --- |
| `MALLOC_ARENA_MAX=1` | 97,816 to 98,204 kB (flattest) | one arena fragments less, but returns nothing |
| `MALLOC_TRIM_THRESHOLD_=131072`, `MALLOC_TOP_PAD_=0` | 99,812 to 106,348 kB | trimming only frees an arena's top |
| jemalloc 5 via `LD_PRELOAD` | 97,852 to 101,416 kB in four cycles, 84,940 once | purging runs on allocation, which an idle server does not do |

I added no allocator because none was materially better. If it matters, the options are a periodic `malloc_trim(0)` (glibc only, a few milliseconds here) or jemalloc with `background_thread:true`; neither was tested at 100,000 connections or is shipped.

## Finding 6: SSE at 100,000 connections (not run)

The laptop attempt was aborted by the memory floor (1,471 MiB available against 1,536 MiB) while connecting, after 29,047 SYN retransmissions: untested, not failed. The sandbox cannot hold 100,000 descriptors. From the measured per-connection cost I estimate about 2.1 GiB for the server with the router fix and 2.6 GiB without, plus 0.5 GiB for the load generator; these are estimates, not measurements.

## What these numbers do not show

Two machines over loopback: no network latency, packet loss, NIC, TLS or proxy. One topic and one shared ring at 1 message per second with 64-byte payloads, so an idle-heavy workload that says nothing about many topics or high publish rates. Not every connection was established (18 to 309 of 100,000 per 100,000 run, all flagged in `runs.csv`), so latency at 100,000 covers 99,691 to 99,982. Most laptop points are single runs; only 10,000 and the largest N repeat, and SSE does not repeat above 50,000. The before-fix build was measured only to 25,000 connections, with latency to 10,000.

## Build identity and data integrity

The laptop results come from an uncommitted tree on top of commit 4c9191a (`git_dirty: true` on every invocation), so no commit reproduces them. The sandbox `environment.json` records `git_dirty` per invocation (both values occur in its 17). Five server binaries were used (SHA-256 prefixes):

| Variant labels | Binary | Code state |
| --- | --- | --- |
| `unpatched` | 2cbe76a6 | parent commit, before the WebSocket buffer fix |
| `current`, `diag-netstat` | f6581b5d | buffer fix, tokio `TcpListener::bind` (backlog 128), before `HYDRANT_LISTEN_BACKLOG` existed |
| `backlog1024`, `backlog4096` | 4ae88160 | first build with the backlog setting |
| `backlog1024-b`, `backlog4096-b`, `sse-extend` | 31de8383 | a later rebuild; what changed is not recorded |
| `tokio-default-bind` | 3f4af462 | `TcpListener::bind` (backlog 128), passed with `--server-bin` |

The committed default backlog is 4096, so `make bench-sweep` does not reproduce `current`; `--listen-backlog 128` is closest. The 4 aborted and 2 skipped laptop rows carry no results and have no loadgen report in `raw/`, and `run.py` now deletes any report for a run's tag first. An earlier runner counted a failed `/metrics` scrape as a gauge of zero, so for all 41 `ok` runs I checked `raw/ts_*.csv`: real scrapes at zero during teardown (blank scrapes only in the `start` and `connecting_start` phases), 10 to 26 server descriptors after teardown against a baseline of 10, an idle sample equal to the established count, and for the ten 100,000 runs a last nonzero scrape before the recorded drain time. The loadgen exit code was not stored then, but all 41 have a JSON report, at least 20 successful publishes and 0 percent loss.

## Reproduce

```sh
make build
make bench-sweep                                        # ws and sse, 1,000 to 100,000 (backlog 4096 on the committed tree)
python3 bench/run.py --variant unpatched --server-bin /path/to/older/hydrant-server
python3 bench/run.py --variant backlog4096 --listen-backlog 4096 --transports ws --ladder 100000 --no-repeat
python3 bench/run.py --variant sse-extend --transports sse --ladder 75000 100000 --no-repeat
bench/experiments-cloud.sh                              # one-factor connect experiments, needs root
make bench-plot                                         # charts into docs/img/
```

The unpatched binary is the server with the `read_buffer_size` and `write_buffer_size` calls removed from `crates/server/src/routes/ws.rs`. Steps that would leave under 1.5 GiB available are skipped or aborted and recorded. `run.py` refuses to reuse a variant label that already has rows; pick a new one or pass `--replace-variant`. Column descriptions and safety rules are in `bench/README.md`.
