#!/usr/bin/env python3
"""Connection-scaling sweep for hydrant-server (stdlib only).

For every transport and connection count it starts a fresh release server,
runs hydrant-loadgen against it with subscribers spread over loopback
addresses, and samples server memory at baseline, idle, under load and after
teardown. See bench/README.md.
"""

import argparse
import csv
import ctypes
import http.client
import json
import math
import os
import platform
import random
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SERVER_BIN = ROOT / "target" / "release" / "hydrant-server"
LOADGEN_BIN = ROOT / "target" / "release" / "hydrant-loadgen"
PAGE_KB = os.sysconf("SC_PAGE_SIZE") // 1024
CLK_TCK = os.sysconf("SC_CLK_TCK")

DEFAULT_LADDER = [1000, 5000, 10000, 25000, 50000, 75000, 100000]
HEADLINE_REPEAT_AT = 10000
SPREAD = 64
PAYLOAD_BYTES = 64
CONNECT_CONCURRENCY = 128
HOLD_IDLE_SECS = 10
IDLE_SETTLE_SECS = 3.0
MEM_FLOOR_KB = int(1.5 * 1024 * 1024)
PORT_BASE = 47300
PR_SET_PDEATHSIG = 1
LIBC = ctypes.CDLL(None) if sys.platform.startswith("linux") else None

SAMPLE_FIELDS = [
    "server_rss_kb", "server_rss_anon_kb", "server_rss_file_kb", "server_hwm_kb",
    "server_pss_kb", "server_pss_anon_kb", "server_threads", "server_fds",
    "server_cpu_s", "loadgen_rss_kb", "loadgen_hwm_kb", "loadgen_fds",
    "loadgen_cpu_s", "tcp_inuse", "tcp_orphan", "tcp_tw", "tcp_alloc",
    "tcp_mem_pages", "tcp_mem_kb", "mem_available_kb", "slab_kb", "sunreclaim_kb", "gauge_ws", "gauge_sse",
    "gauge_connections",
]
RUN_FIELDS = [
    "status", "requested", "established", "failed", "closed_early",
    "connect_secs", "teardown_server_secs", "teardown_loadgen_secs",
    "requested_rate", "achieved_rate", "publish_ok", "publish_failed",
    "publish_not_sent", "expected", "received", "missing", "loss_percent",
    "lat_min_ms", "lat_mean_ms", "lat_p50_ms", "lat_p90_ms", "lat_p99_ms",
    "lat_p999_ms", "lat_max_ms", "deliveries_per_sec", "listen_overflows_delta", "listen_drops_delta", "reqq_full_cookies_delta",
    "abort_on_data_delta", "syn_retrans_delta", "tw_overflow_delta", "flags",
]
DERIVED_FIELDS = [
    "server_rss_per_conn_bytes", "server_anon_per_conn_bytes",
    "server_pss_per_conn_bytes", "loadgen_rss_per_conn_bytes",
    "tcp_mem_per_conn_bytes", "slab_per_conn_bytes", "system_used_per_conn_bytes",
]
SWEEP_COLUMNS = (
    ["variant", "run_id", "transport", "n", "rep", "phase", "t_s"]
    + SAMPLE_FIELDS + DERIVED_FIELDS + RUN_FIELDS
)
RUNS_COLUMNS = ["variant", "run_id", "transport", "n", "rep"] + RUN_FIELDS
TS_COLUMNS = [
    "t_s", "phase", "gauge_connections", "server_rss_kb", "server_rss_anon_kb",
    "loadgen_rss_kb", "tcp_mem_pages", "tcp_tw", "mem_available_kb",
]


def read_text(path):
    try:
        with open(path) as handle:
            return handle.read()
    except OSError:
        return ""


def proc_status(pid):
    out = {}
    for line in read_text(f"/proc/{pid}/status").splitlines():
        key, _, value = line.partition(":")
        out[key] = value.split()
    return out


def kb(status, key):
    values = status.get(key)
    return int(values[0]) if values else None


def proc_cpu_seconds(pid):
    parts = read_text(f"/proc/{pid}/stat").rpartition(")")[2].split()
    if len(parts) < 13:
        return None
    return (int(parts[11]) + int(parts[12])) / CLK_TCK


def proc_fds(pid):
    try:
        return len(os.listdir(f"/proc/{pid}/fd"))
    except OSError:
        return None


def smaps_rollup(pid):
    out = {}
    for line in read_text(f"/proc/{pid}/smaps_rollup").splitlines():
        key, _, value = line.partition(":")
        parts = value.split()
        if parts and parts[0].isdigit():
            out[key] = int(parts[0])
    return out


def meminfo():
    out = {}
    for line in read_text("/proc/meminfo").splitlines():
        key, _, value = line.partition(":")
        parts = value.split()
        if parts and parts[0].isdigit():
            out[key] = int(parts[0])
    return out


def mem_available_kb():
    return meminfo().get("MemAvailable", 0)


def sockstat_tcp():
    for line in read_text("/proc/net/sockstat").splitlines():
        if line.startswith("TCP:"):
            parts = line.split()[1:]
            return {parts[i]: int(parts[i + 1]) for i in range(0, len(parts) - 1, 2)}
    return {}


def die_with_parent():
    if LIBC is not None:
        LIBC.prctl(PR_SET_PDEATHSIG, signal.SIGKILL)


class MetricsScraper:
    """Scrapes /metrics over one kept-alive connection so a stalled listener does not blank samples."""

    def __init__(self, port, timeout=5):
        self.port = port
        self.timeout = timeout
        self.conn = None

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def scrape(self):
        body = None
        for _ in range(2):
            try:
                if self.conn is None:
                    self.conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=self.timeout)
                self.conn.request("GET", "/metrics")
                body = self.conn.getresponse().read().decode()
                break
            except (OSError, http.client.HTTPException):
                self.close()
        if body is None:
            return None, {}
        return body, parse_metrics(body)


def parse_metrics(body):
    values = {}
    for line in body.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        name, _, value = line.rpartition(" ")
        try:
            values[name] = float(value)
        except ValueError:
            pass
    return values


NETSTAT_KEYS = {
    "ListenOverflows": "listen_overflows_delta",
    "ListenDrops": "listen_drops_delta",
    "TCPReqQFullDoCookies": "reqq_full_cookies_delta",
    "TCPAbortOnData": "abort_on_data_delta",
    "TCPSynRetrans": "syn_retrans_delta",
    "TCPTimeWaitOverflow": "tw_overflow_delta",
}


def netstat_counters():
    lines = read_text("/proc/net/netstat").splitlines()
    for head, vals in zip(lines, lines[1:]):
        if head.startswith("TcpExt:"):
            names, nums = head.split()[1:], vals.split()[1:]
            return {k: int(nums[names.index(k)]) for k in NETSTAT_KEYS if k in names}
    return {}


def port_is_free(port):
    with socket.socket() as sock:
        try:
            sock.bind(("0.0.0.0", port))
            return True
        except OSError:
            return False


def sample(server_pid, loadgen_pid, scraper):
    row = {}
    st = proc_status(server_pid)
    row["server_rss_kb"] = kb(st, "VmRSS")
    row["server_rss_anon_kb"] = kb(st, "RssAnon")
    row["server_rss_file_kb"] = kb(st, "RssFile")
    row["server_hwm_kb"] = kb(st, "VmHWM")
    row["server_threads"] = int(st["Threads"][0]) if "Threads" in st else None
    roll = smaps_rollup(server_pid)
    row["server_pss_kb"] = roll.get("Pss")
    row["server_pss_anon_kb"] = roll.get("Pss_Anon")
    row["server_fds"] = proc_fds(server_pid)
    row["server_cpu_s"] = proc_cpu_seconds(server_pid)
    if loadgen_pid:
        lst = proc_status(loadgen_pid)
        row["loadgen_rss_kb"] = kb(lst, "VmRSS")
        row["loadgen_hwm_kb"] = kb(lst, "VmHWM")
        row["loadgen_fds"] = proc_fds(loadgen_pid)
        row["loadgen_cpu_s"] = proc_cpu_seconds(loadgen_pid)
    tcp = sockstat_tcp()
    row["tcp_inuse"] = tcp.get("inuse")
    row["tcp_orphan"] = tcp.get("orphan")
    row["tcp_tw"] = tcp.get("tw")
    row["tcp_alloc"] = tcp.get("alloc")
    row["tcp_mem_pages"] = tcp.get("mem")
    row["tcp_mem_kb"] = tcp["mem"] * PAGE_KB if "mem" in tcp else None
    info = meminfo()
    row["mem_available_kb"] = info.get("MemAvailable")
    row["slab_kb"] = info.get("Slab")
    row["sunreclaim_kb"] = info.get("SUnreclaim")
    _, metrics = scraper.scrape()
    row["gauge_ws"] = metrics.get('hydrant_connections{transport="ws"}')
    row["gauge_sse"] = metrics.get('hydrant_connections{transport="sse"}')
    if row["gauge_ws"] is not None:
        row["gauge_connections"] = row["gauge_ws"] + (row["gauge_sse"] or 0)
    return row


class Run:
    def __init__(self, args, run_id, transport, n, rep, addr_block):
        self.args = args
        self.run_id = run_id
        self.transport = transport
        self.n = n
        self.rep = rep
        self.tag = f"{args.variant}_{transport}_{n}_r{rep}"
        self.port = PORT_BASE + run_id
        self.base_ip = f"127.{addr_block}.{run_id % 250 + 1}.1"
        self.server = None
        self.loadgen = None
        self.abort_reason = None
        self.phase = "start"
        self.t0 = time.monotonic()
        self.markers = {}
        self.stop_sampler = threading.Event()
        self.last_row = {}
        self.ts_rows = []
        self.min_available_kb = None
        self.lock = threading.Lock()
        self.scraper = MetricsScraper(self.port)
        self.sampler_scraper = MetricsScraper(self.port)
        self.points = {}
        self.netstat_before = netstat_counters()
        self.stderr_lines = []
        self.last_metrics_body = None

    def now(self):
        return round(time.monotonic() - self.t0, 3)

    def kill_all(self):
        for proc in (self.loadgen, self.server):
            if proc is not None and proc.poll() is None:
                proc.kill()
        for proc in (self.loadgen, self.server):
            if proc is not None:
                try:
                    proc.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    pass

    def watchdog(self):
        while not self.stop_sampler.is_set():
            avail = mem_available_kb()
            if self.min_available_kb is None or avail < self.min_available_kb:
                self.min_available_kb = avail
            if avail < MEM_FLOOR_KB and self.abort_reason is None:
                self.abort_reason = (
                    f"MemAvailable {avail // 1024} MiB below floor "
                    f"{MEM_FLOOR_KB // 1024} MiB at phase {self.phase}"
                )
                self.kill_all()
                return
            self.stop_sampler.wait(0.25)

    def sampler(self):
        while not self.stop_sampler.is_set():
            if self.server is not None and self.server.poll() is None:
                lg = self.loadgen.pid if self.loadgen and self.loadgen.poll() is None else None
                row = sample(self.server.pid, lg, self.sampler_scraper)
                row["t_s"] = self.now()
                row["phase"] = self.phase
                with self.lock:
                    self.ts_rows.append(row)
            self.stop_sampler.wait(0.5)

    def read_stderr(self):
        for raw in self.loadgen.stderr:
            line = raw.decode(errors="replace").rstrip()
            self.stderr_lines.append(line)
            if "all connected:" in line:
                self.markers.setdefault("connected", time.monotonic())
            elif "publish starting" in line:
                self.markers.setdefault("publish", time.monotonic())
            elif "disconnecting" in line:
                self.markers.setdefault("disconnecting", time.monotonic())

    def wait_marker(self, name, timeout):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if name in self.markers:
                return True
            if self.abort_reason or self.loadgen.poll() is not None:
                return name in self.markers
            time.sleep(0.05)
        return False

    def wait_gauge(self, target, timeout):
        deadline = time.monotonic() + timeout
        value = None
        while time.monotonic() < deadline and not self.abort_reason:
            _, metrics = self.scraper.scrape()
            value = sum(v for k, v in metrics.items() if k.startswith("hydrant_connections{"))
            if value == target:
                return True, value
            time.sleep(0.1)
        return False, value

    def point(self, phase):
        self.phase = phase
        lg = self.loadgen.pid if self.loadgen and self.loadgen.poll() is None else None
        row = sample(self.server.pid, lg, self.scraper)
        row["phase"] = phase
        row["t_s"] = self.now()
        self.points[phase] = row
        return row

    def execute(self):
        args = self.args
        raw = args.out / "raw"
        env = {k: v for k, v in os.environ.items() if not k.startswith("HYDRANT_")}
        env["HYDRANT_LISTEN_ADDR"] = f"0.0.0.0:{self.port}"
        env["HYDRANT_LOG_LEVEL"] = "warn"
        if self.args.listen_backlog is not None:
            env["HYDRANT_LISTEN_BACKLOG"] = str(self.args.listen_backlog)
        result = {"status": "ok", "flags": []}
        server_log = open(raw / f"server_{self.tag}.log", "wb")
        self.server = subprocess.Popen(
            [str(SERVER_BIN)], env=env, stdout=server_log, stderr=server_log,
            preexec_fn=die_with_parent,
        )
        threading.Thread(target=self.watchdog, daemon=True).start()
        threading.Thread(target=self.sampler, daemon=True).start()
        try:
            self.wait_healthy()
            time.sleep(1.0)
            self.point("baseline")
            report_path = raw / f"loadgen_{self.tag}.json"
            report_path.unlink(missing_ok=True)
            (raw / f"metrics_{self.tag}.txt").unlink(missing_ok=True)
            cmd = [
                str(LOADGEN_BIN), "--url", f"http://{self.base_ip}:{self.port}",
                "--transport", self.transport, "--connections", str(self.n),
                "--spread-loopback", str(SPREAD), "--rate", str(args.rate),
                "--duration", str(args.duration), "--payload-bytes", str(PAYLOAD_BYTES),
                "--connect-concurrency", str(CONNECT_CONCURRENCY),
                "--drain-timeout", "10", "--hold-idle", str(HOLD_IDLE_SECS),
                "--json", str(report_path),
            ]
            self.phase = "connecting"
            out_file = open(raw / f"loadgen_{self.tag}.txt", "wb")
            self.loadgen = subprocess.Popen(
                cmd, stdout=out_file, stderr=subprocess.PIPE, env=os.environ.copy(),
                preexec_fn=die_with_parent,
            )
            threading.Thread(target=self.read_stderr, daemon=True).start()
            time.sleep(1.0)
            lg_base = self.point("connecting_start").get("loadgen_rss_kb")

            connect_timeout = 120 + self.n / 200
            if not self.wait_marker("connected", connect_timeout):
                result["status"] = self.abort_reason and "aborted_memory" or "connect_failed"
                return result, report_path, lg_base
            gauge_ok, gauge_value = self.wait_gauge(self.established_from_stderr(), 60)
            result["gauge_matched_established"] = gauge_ok
            result["gauge_value_at_connected"] = gauge_value
            time.sleep(IDLE_SETTLE_SECS)
            self.point("idle")
            if "publish" in self.markers:
                result["flags"].append("idle_sample_overlapped_publish")

            if not self.wait_marker("publish", HOLD_IDLE_SECS + 30):
                result["status"] = self.abort_reason and "aborted_memory" or "publish_never_started"
                return result, report_path, lg_base
            self.phase = "publishing"
            time.sleep(args.duration / 2)
            self.point("load")
            self.last_metrics_body, _ = self.scraper.scrape()

            if not self.wait_marker("disconnecting", args.duration + 60):
                result["status"] = self.abort_reason and "aborted_memory" or "no_disconnect_marker"
                return result, report_path, lg_base
            self.phase = "teardown"
            drained, _ = self.wait_gauge(0, 180)
            drained_at = time.monotonic()
            result["teardown_server_secs"] = round(drained_at - self.markers["disconnecting"], 3)
            result["server_drained"] = drained
            self.loadgen.wait(timeout=120)
            result["teardown_loadgen_secs"] = round(time.monotonic() - self.markers["disconnecting"], 3)
            result["loadgen_exit_code"] = self.loadgen.returncode
            time.sleep(IDLE_SETTLE_SECS)
            self.phase = "after"
            self.point("after")
            return result, report_path, lg_base
        finally:
            if self.abort_reason:
                result["status"] = "aborted_memory"
                result["abort_reason"] = self.abort_reason

    def established_from_stderr(self):
        for line in self.stderr_lines:
            if "all connected:" in line:
                for part in line.split():
                    if part.startswith("established="):
                        return int(part.split("=")[1])
        return self.n

    def wait_healthy(self):
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=2)
                conn.request("GET", "/healthz")
                if conn.getresponse().status == 200:
                    return
            except (OSError, http.client.HTTPException):
                time.sleep(0.1)
        raise RuntimeError("server did not become healthy")

    def cleanup(self):
        self.stop_sampler.set()
        self.kill_all()
        self.scraper.close()
        self.sampler_scraper.close()


def pct(a, b):
    return None if not b else a / b


def derive(row, base, n, lg_base=None):
    def per(delta_kb):
        return None if delta_kb is None or not n else round(delta_kb * 1024 / n, 1)

    def diff(key):
        if row.get(key) is None or base.get(key) is None:
            return None
        return row[key] - base[key]

    gross = row.get("loadgen_rss_kb")
    return {
        "server_rss_per_conn_bytes": per(diff("server_rss_kb")),
        "server_anon_per_conn_bytes": per(diff("server_rss_anon_kb")),
        "server_pss_per_conn_bytes": per(diff("server_pss_kb")),
        "tcp_mem_per_conn_bytes": per(diff("tcp_mem_kb")),
        "slab_per_conn_bytes": per(diff("sunreclaim_kb")),
        "loadgen_rss_per_conn_bytes": per(gross),
        "system_used_per_conn_bytes": per(
            None
            if row.get("mem_available_kb") is None or base.get("mem_available_kb") is None
            else base["mem_available_kb"] - row["mem_available_kb"]
        ),
    }


def run_level(report_path, result, n):
    out = {k: None for k in RUN_FIELDS}
    out["status"] = result["status"]
    out["teardown_server_secs"] = result.get("teardown_server_secs")
    out["teardown_loadgen_secs"] = result.get("teardown_loadgen_secs")
    flags = list(result.get("flags", []))
    try:
        report = json.loads(Path(report_path).read_text())
    except (OSError, ValueError):
        flags.append("no_loadgen_report")
        out["flags"] = ";".join(flags)
        return out
    c, p, d = report["connections"], report["publish"], report["deliveries"]
    lat = report.get("latency_ms") or {}
    out.update(
        requested=c["requested"], established=c["established"], failed=c["failed"],
        closed_early=c["closed_early"], connect_secs=c["connect_secs"],
        requested_rate=p["requested_rate"], achieved_rate=p["achieved_rate"],
        publish_ok=p["ok"], publish_failed=p["failed"], publish_not_sent=p["not_sent"],
        expected=d["expected"], received=d["received"], missing=d["missing"],
        loss_percent=d["loss_percent"], lat_min_ms=lat.get("min"),
        lat_mean_ms=lat.get("mean"), lat_p50_ms=lat.get("p50"), lat_p90_ms=lat.get("p90"),
        lat_p99_ms=lat.get("p99"), lat_p999_ms=lat.get("p999"), lat_max_ms=lat.get("max"),
        deliveries_per_sec=report["throughput"]["deliveries_per_sec"],
    )
    if c["failed"] > 0:
        flags.append(f"connect_failed={c['failed']}")
    if c["closed_early"] > 0:
        flags.append(f"closed_early={c['closed_early']}")
    if d["missing"] > 0 or d["loss_percent"] > 0:
        flags.append(f"loss={d['loss_percent']}%")
    if p["failed"] > 0 or p["not_sent"] > 0:
        flags.append(f"publish_failed={p['failed']}_not_sent={p['not_sent']}")
    if p["requested_rate"] and p["achieved_rate"] < 0.95 * p["requested_rate"]:
        flags.append("achieved_rate_below_95pct")
    if d["malformed"] > 0:
        flags.append(f"malformed={d['malformed']}")
    if result.get("gauge_matched_established") is False:
        flags.append(f"gauge_mismatch={result.get('gauge_value_at_connected')}")
    if result.get("server_drained") is False:
        flags.append("server_did_not_drain_in_180s")
    out["flags"] = ";".join(flags)
    return out


def write_csv(path, columns, rows):
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: ("" if row.get(k) is None else row.get(k)) for k in columns})


def repo_relative(path):
    path = Path(path).resolve()
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return path.name


def read_environment(path):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return {}


def write_environment(args, addr_block):
    path = args.out / "environment.json"
    env = environment()
    previous = read_environment(path).get("invocations", [])
    digest = subprocess.run(
        ["sha256sum", str(SERVER_BIN)], capture_output=True, text=True
    ).stdout.split()[:1]
    git = git_state()
    env["invocations"] = [i for i in previous if i.get("variant") != args.variant] + [
        {
            "variant": args.variant,
            "date_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "mem_available_kb_at_start": mem_available_kb(),
            "server_binary": repo_relative(SERVER_BIN),
            "server_binary_sha256": digest[0] if digest else None,
            **git,
            "ladder": args.ladder,
            "transports": args.transports,
            "sweep": {
                "repeats": args.repeats,
                "no_repeat": args.no_repeat,
                "repeat_points": f"N={HEADLINE_REPEAT_AT} and the maximum N reached",
                "memory_floor_kib": MEM_FLOOR_KB,
            },
            "server": {
                "profile": "release",
                "env": {
                    "HYDRANT_LISTEN_ADDR": "0.0.0.0:<port>",
                    "HYDRANT_LOG_LEVEL": "warn",
                    "HYDRANT_LISTEN_BACKLOG": args.listen_backlog if args.listen_backlog is not None else "unset (server default)",
                    "other HYDRANT_*": "unset, built-in defaults (topic capacity 256, "
                    "max message 65536 bytes, policy disconnect, send timeout 10 s, "
                    "max connections unlimited)",
                },
            },
            "loadgen": {
                "binary": repo_relative(LOADGEN_BIN),
                "single_topic": "loadgen",
                "spread_loopback_addresses": SPREAD,
                "address_block": f"127.{addr_block}.<run>.1 .. +{SPREAD - 1}, one /24 per run",
                "payload_bytes": PAYLOAD_BYTES,
                "connect_concurrency": CONNECT_CONCURRENCY,
                "rate_msgs_per_sec": args.rate,
                "publish_duration_secs": args.duration,
                "hold_idle_secs": HOLD_IDLE_SECS,
                "drain_timeout_secs": 10,
            },
        }
    ]
    path.write_text(json.dumps(env, indent=2) + "\n")


def variant_exists(args):
    for name in ("runs.csv", "sweep.csv"):
        path = args.out / name
        if path.exists():
            with open(path, newline="") as handle:
                if any(r.get("variant") == args.variant for r in csv.DictReader(handle)):
                    return True
    return False


def merge_rows(path, columns, variant, rows):
    kept = []
    if path.exists():
        with open(path, newline="") as handle:
            kept = [r for r in csv.DictReader(handle) if r.get("variant") != variant]
    write_csv(path, columns, kept + rows)


def git_state():
    def sh(cmd):
        try:
            return subprocess.run(cmd, capture_output=True, text=True, cwd=ROOT, timeout=30).stdout.strip()
        except (OSError, subprocess.SubprocessError):
            return None

    return {
        "git_commit": sh(["git", "rev-parse", "HEAD"]),
        "git_branch": sh(["git", "rev-parse", "--abbrev-ref", "HEAD"]),
        "git_dirty": bool(sh(["git", "status", "--porcelain"])),
    }


def environment():
    def sh(cmd):
        try:
            return subprocess.run(cmd, capture_output=True, text=True, cwd=ROOT, timeout=30).stdout.strip()
        except (OSError, subprocess.SubprocessError):
            return None

    cpu = next((l.split(":", 1)[1].strip() for l in read_text("/proc/cpuinfo").splitlines()
                if l.startswith("model name")), None)
    mem = meminfo()
    sysctl = {
        name: read_text(f"/proc/sys/{name.replace('.', '/')}").strip()
        for name in (
            "net.ipv4.ip_local_port_range", "net.core.somaxconn", "net.ipv4.tcp_tw_reuse",
            "net.ipv4.tcp_max_tw_buckets", "net.ipv4.tcp_max_orphans", "net.ipv4.tcp_max_syn_backlog",
            "fs.file-max", "net.ipv4.tcp_mem", "net.ipv4.tcp_rmem", "net.ipv4.tcp_wmem",
        )
    }
    import resource
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    return {
        "cpu_model": cpu,
        "logical_cores": os.cpu_count(),
        "mem_total_kb": mem.get("MemTotal"),
        "swap_total_kb": mem.get("SwapTotal"),
        "kernel": platform.release(),
        "wsl2": "microsoft" in platform.release().lower(),
        "rustc": sh(["rustc", "--version"]),
        "nofile_limit": {"soft": soft, "hard": hard},
        "sysctl": sysctl,
        "page_size_kb": PAGE_KB,
        "notes": [
            "Server and load generator run on the same machine and compete for CPU, "
            "so latency figures are a lower bound on quality, not a server-only measurement.",
            "Server RSS excludes kernel socket buffers; tcp_mem_* and system_used_* "
            "columns cover the kernel side (both ends of every loopback connection).",
            "The machine was in interactive use during the run, so memory deltas "
            "carry background noise (see mem_available_kb).",
            "Connections use a single topic, so one broadcast ring is shared by all subscribers.",
            "WSL2 kernel; loopback traffic never touches a NIC.",
            "Machine facts are read at each invocation and the latest reading is kept; "
            "settings that belong to one invocation live in its entry under invocations.",
        ],
    }


def rederive(out):
    path = out / "sweep.csv"
    with open(path, newline="") as handle:
        rows = list(csv.DictReader(handle))
    bases = {(r["variant"], r["run_id"]): r for r in rows if r["phase"] == "baseline"}

    def num(value):
        return None if value in ("", None) else float(value)

    for row in rows:
        if row["phase"] == "baseline":
            continue
        base = {k: num(v) for k, v in bases[(row["variant"], row["run_id"])].items()
                if k not in ("variant", "transport", "phase", "status", "flags")}
        cur = {k: num(v) for k, v in row.items()
               if k not in ("variant", "transport", "phase", "status", "flags")}
        row.update(derive(cur, base, int(row["n"])))
    write_csv(path, SWEEP_COLUMNS, rows)


def main():
    global SERVER_BIN
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--transports", nargs="+", default=["ws", "sse"], choices=["ws", "sse"])
    parser.add_argument("--ladder", nargs="+", type=int, default=DEFAULT_LADDER)
    parser.add_argument("--repeats", type=int, default=3, help="total runs at the repeat points")
    parser.add_argument("--rate", type=int, default=1)
    parser.add_argument("--duration", type=int, default=20)
    parser.add_argument("--out", "--results-dir", dest="out", type=Path, default=ROOT / "bench" / "results",
                        help="directory for results (default: bench/results)")
    parser.add_argument("--replace-variant", action="store_true",
                        help="overwrite the rows of a variant label that already exists in the results")
    parser.add_argument("--variant", default="current", help="label for the server build under test")
    parser.add_argument("--server-bin", type=Path, default=SERVER_BIN)
    parser.add_argument("--listen-backlog", type=int, default=None, help="set HYDRANT_LISTEN_BACKLOG for the server (default: server default)")
    parser.add_argument("--no-repeat", action="store_true")
    parser.add_argument("--rederive", action="store_true", help="recompute derived columns in an existing sweep.csv and exit")
    args = parser.parse_args()
    if args.rederive:
        rederive(args.out)
        return

    SERVER_BIN = args.server_bin.resolve()
    for binary in (SERVER_BIN, LOADGEN_BIN):
        if not binary.exists():
            sys.exit(f"missing {binary}; run `cargo build --workspace --release --locked`")
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "raw").mkdir(exist_ok=True)
    if variant_exists(args) and not args.replace_variant:
        sys.exit(
            f"variant {args.variant!r} already has rows in {args.out}; pick a new --variant label "
            "or pass --replace-variant to overwrite its rows"
        )

    addr_block = random.randrange(20, 230)
    write_environment(args, addr_block)

    sweep_rows, run_rows = [], []
    run_id = 0
    current = {"run": None}

    def on_signal(signum, _frame):
        if current["run"] is not None:
            current["run"].cleanup()
        sys.exit(130)

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    def one(transport, n, rep, per_conn_cost_kb):
        nonlocal run_id
        run_id += 1
        while not port_is_free(PORT_BASE + run_id):
            run_id += 1
        avail = mem_available_kb()
        predicted = per_conn_cost_kb * n + MEM_FLOOR_KB if per_conn_cost_kb else None
        label = f"{transport} N={n} rep={rep}"
        if predicted and predicted > avail:
            print(f"[skip] {label}: predicted need {predicted // 1024} MiB > "
                  f"available {avail // 1024} MiB", flush=True)
            run_rows.append({"variant": args.variant, "run_id": run_id, "transport": transport, "n": n, "rep": rep,
                             "status": "skipped_memory_prediction",
                             "flags": f"predicted_kib={predicted};available_kib={avail}"})
            merge_rows(args.out / "runs.csv", RUNS_COLUMNS, args.variant, run_rows)
            return None
        run = Run(args, run_id, transport, n, rep, addr_block)
        current["run"] = run
        print(f"[run {run_id}] {label} start, MemAvailable {avail // 1024} MiB", flush=True)
        result, report_path, lg_base = {"status": "error", "flags": []}, None, None
        try:
            result, report_path, lg_base = run.execute()
        except Exception as exc:  # noqa: BLE001 - recorded in the data, not hidden
            result = {"status": f"error: {exc}", "flags": []}
        finally:
            run.cleanup()
            current["run"] = None
        if run.abort_reason:
            result["status"] = "aborted_memory"
            result["flags"].append(run.abort_reason)
        level = run_level(report_path, result, n) if report_path else {
            "status": result["status"], "flags": ";".join(result["flags"])}
        after_netstat = netstat_counters()
        for key, column in NETSTAT_KEYS.items():
            if key in after_netstat and key in run.netstat_before:
                level[column] = after_netstat[key] - run.netstat_before[key]
        level.update(variant=args.variant, run_id=run_id, transport=transport, n=n, rep=rep)
        run_rows.append(level)
        base = run.points.get("baseline", {})
        for phase in ("baseline", "idle", "load", "after"):
            row = run.points.get(phase)
            if row is None:
                continue
            merged = {"variant": args.variant, "run_id": run_id, "transport": transport, "n": n, "rep": rep}
            merged.update(row)
            merged.update(derive(row, base, n if phase != "baseline" else 0, lg_base))
            merged.update({k: level.get(k) for k in RUN_FIELDS})
            sweep_rows.append(merged)
        ts = args.out / "raw" / f"ts_{run.tag}.csv"
        write_csv(ts, TS_COLUMNS, run.ts_rows)
        merge_rows(args.out / "sweep.csv", SWEEP_COLUMNS, args.variant, sweep_rows)
        merge_rows(args.out / "runs.csv", RUNS_COLUMNS, args.variant, run_rows)
        if run.last_metrics_body:
            (args.out / "raw" / f"metrics_{run.tag}.txt").write_text(run.last_metrics_body)
        idle, base_avail = run.points.get("idle"), base.get("mem_available_kb")
        cost = None
        if idle and base_avail and n:
            cost = max(0, (base_avail - idle["mem_available_kb"])) / n
        print(f"[run {run_id}] {label} {level.get('status')} flags={level.get('flags')} "
              f"min MemAvailable {(run.min_available_kb or 0) // 1024} MiB", flush=True)
        return {"ok": level.get("status") == "ok", "cost": cost, "tag": run.tag}

    ramp_source = {}
    for transport in args.transports:
        cost = None
        last_ok = None
        for n in args.ladder:
            res = one(transport, n, 1, cost)
            if res is None:
                break
            if res["cost"]:
                cost = res["cost"] * 1.2
            if not res["ok"]:
                print(f"[limit] {transport}: stopped at N={n}", flush=True)
                break
            last_ok = n
            ramp_source[transport] = res["tag"]
            time.sleep(3)
            if n == HEADLINE_REPEAT_AT and not args.no_repeat:
                for rep in range(2, args.repeats + 1):
                    one(transport, n, rep, cost)
                    time.sleep(3)
        if last_ok and last_ok != HEADLINE_REPEAT_AT and not args.no_repeat:
            for rep in range(2, args.repeats + 1):
                one(transport, last_ok, rep, cost)
                time.sleep(3)
    for transport, tag in ramp_source.items():
        src = args.out / "raw" / f"ts_{tag}.csv"
        if src.exists():
            shutil.copy(src, args.out / (f"ramp_{transport}.csv" if args.variant == "current" else f"ramp_{transport}_{args.variant}.csv"))
    print("sweep done", flush=True)


if __name__ == "__main__":
    main()
