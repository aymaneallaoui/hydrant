#!/usr/bin/env python3
"""Connect N subscribers to one server process, disconnect, wait, repeat; sample server memory throughout.

Decides between allocator retention (RSS plateaus across cycles) and a leak (RSS grows per cycle).
Standard library only. Writes a CSV of samples and prints a per-cycle summary.
"""

import argparse
import csv
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import run as harness  # noqa: E402

COLUMNS = ["t_s", "cycle", "phase", "gauge", "server_rss_kb", "server_rss_anon_kb", "server_hwm_kb",
           "server_threads", "server_fds", "mem_available_kb"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--transport", default="ws", choices=["ws", "sse"])
    parser.add_argument("--connections", type=int, default=15000)
    parser.add_argument("--cycles", type=int, default=5)
    parser.add_argument("--settle", type=float, default=60.0, help="seconds to watch after each teardown")
    parser.add_argument("--hold-idle", type=int, default=5)
    parser.add_argument("--port", type=int, default=48400)
    parser.add_argument("--spread-loopback", type=int, default=64)
    parser.add_argument("--server-bin", type=Path, default=harness.SERVER_BIN)
    parser.add_argument("--server-env", action="append", default=[], metavar="KEY=VALUE",
                        help="extra environment for the server, for example MALLOC_ARENA_MAX=2")
    parser.add_argument("--out", type=Path, required=True, help="CSV file for the samples")
    parser.add_argument("--label", default="")
    args = parser.parse_args()

    env = {k: v for k, v in os.environ.items() if not k.startswith("HYDRANT_")}
    env["HYDRANT_LISTEN_ADDR"] = f"0.0.0.0:{args.port}"
    env["HYDRANT_LOG_LEVEL"] = "warn"
    for item in args.server_env:
        key, _, value = item.partition("=")
        env[key] = value
    server = subprocess.Popen([str(args.server_bin)], env=env, preexec_fn=harness.die_with_parent)
    rows, state = [], {"cycle": 0, "phase": "start"}
    stop = threading.Event()
    scraper = harness.MetricsScraper(args.port)
    t0 = time.monotonic()

    def sampler():
        while not stop.is_set():
            st = harness.proc_status(server.pid)
            _, metrics = scraper.scrape()
            rows.append({
                "t_s": round(time.monotonic() - t0, 2), "cycle": state["cycle"], "phase": state["phase"],
                "gauge": harness.connection_gauge(metrics),
                "server_rss_kb": harness.kb(st, "VmRSS"), "server_rss_anon_kb": harness.kb(st, "RssAnon"),
                "server_hwm_kb": harness.kb(st, "VmHWM"),
                "server_threads": int(st["Threads"][0]) if "Threads" in st else None,
                "server_fds": harness.proc_fds(server.pid), "mem_available_kb": harness.mem_available_kb(),
            })
            stop.wait(0.5)

    try:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if scraper.scrape()[0] is not None:
                break
            time.sleep(0.1)
        threading.Thread(target=sampler, daemon=True).start()
        time.sleep(2)
        print(f"baseline rss {rows[-1]['server_rss_kb']} kB anon {rows[-1]['server_rss_anon_kb']} kB", flush=True)
        summary = []
        for cycle in range(1, args.cycles + 1):
            state.update(cycle=cycle, phase="connect")
            block = 20 + cycle
            cmd = [
                str(harness.LOADGEN_BIN), "--url", f"http://127.{block}.{cycle}.1:{args.port}",
                "--transport", args.transport, "--connections", str(args.connections),
                "--spread-loopback", str(args.spread_loopback), "--rate", "1", "--duration", "2",
                "--payload-bytes", "64", "--connect-concurrency", "128", "--drain-timeout", "5",
                "--hold-idle", str(args.hold_idle),
            ]
            with open(os.devnull, "wb") as sink:
                code = subprocess.call(cmd, stdout=sink, stderr=sink, preexec_fn=harness.die_with_parent)
            peak = max((r["server_rss_kb"] or 0) for r in rows if r["cycle"] == cycle)
            state["phase"] = "after"
            drained = time.monotonic()
            while time.monotonic() - drained < args.settle:
                time.sleep(0.5)
            after = [r for r in rows if r["cycle"] == cycle and r["phase"] == "after"]
            first, last = after[0], after[-1]
            summary.append((cycle, code, peak, first["server_rss_kb"], last["server_rss_kb"], last["server_rss_anon_kb"], last["gauge"]))
            print(f"cycle {cycle}: loadgen exit {code}, peak rss {peak} kB, rss right after {first['server_rss_kb']} kB, "
                  f"after {args.settle:.0f}s {last['server_rss_kb']} kB (anon {last['server_rss_anon_kb']} kB), gauge {last['gauge']}", flush=True)
    finally:
        stop.set()
        time.sleep(0.6)
        server.kill()
        server.wait()
        scraper.close()
        args.out.parent.mkdir(parents=True, exist_ok=True)
        with open(args.out, "w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=COLUMNS)
            writer.writeheader()
            writer.writerows(rows)
        print(f"wrote {len(rows)} samples to {args.out}")


if __name__ == "__main__":
    main()
