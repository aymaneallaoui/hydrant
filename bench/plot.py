#!/usr/bin/env python3
"""Render the charts in docs/img/ from the CSVs in bench/results/ (no hard-coded numbers).

Needs matplotlib: `make bench-plot` creates bench/.venv on first use.
Each chart is written as <name>.svg and <name>.png (light) and <name>-dark.svg / <name>-dark.png.
"""

import csv
import statistics
import sys
import textwrap
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench" / "results"
CLOUD = RESULTS / "cloud"
OUT = ROOT / "docs" / "img"
CLOUD_LABEL = "cloud sandbox, 4 cores"

THEMES = {
    "light": {
        "surface": "#fcfcfb", "ink": "#0b0b0b", "ink2": "#52514e", "grid": "#e4e3de",
        "blue": "#2a78d6", "orange": "#eb6834", "aqua": "#1baf7a",
    },
    "dark": {
        "surface": "#1a1a19", "ink": "#ffffff", "ink2": "#c3c2b7", "grid": "#34332f",
        "blue": "#3987e5", "orange": "#d95926", "aqua": "#199e70",
    },
}
SUFFIX = {"light": "", "dark": "-dark"}


def read(name, base=RESULTS):
    path = base / name
    if not path.exists():
        return []
    with open(path, newline="") as handle:
        return list(csv.DictReader(handle))


def num(value):
    return None if value in ("", None) else float(value)


def fmt_int(value):
    return f"{value:,.0f}"


def ols(points):
    n = len(points)
    sx = sum(p[0] for p in points)
    sy = sum(p[1] for p in points)
    sxx = sum(p[0] * p[0] for p in points)
    sxy = sum(p[0] * p[1] for p in points)
    slope = (n * sxy - sx * sy) / (n * sxx - sx * sx)
    return slope, (sy - slope * sx) / n


SWEEP = read("sweep.csv")
RUNS = read("runs.csv")
CLOUD_SWEEP = read("sweep.csv", CLOUD)
CLOUD_RUNS = read("runs.csv", CLOUD)
IDLE = [r for r in SWEEP + CLOUD_SWEEP if r["phase"] == "idle" and num(r["server_rss_kb"]) is not None]

SERIES = [
    ("ws_before", "WebSocket, before fix", "orange", lambda r: r["transport"] == "ws" and r["variant"] == "unpatched"),
    ("ws_after", "WebSocket, after fix", "blue", lambda r: r["transport"] == "ws" and r["variant"] == "current"),
    ("sse", "SSE", "aqua", lambda r: r["transport"] == "sse" and r["variant"] in ("unpatched", "current", "sse-extend")),
]
# Runs from the second machine (bench/results/cloud) are drawn hollow with their own markers.
CLOUD_SERIES = [
    ("ws_cloud", f"WebSocket, {CLOUD_LABEL}", "blue", lambda r: r["transport"] == "ws" and r["variant"] == "cloud-ws"),
    ("sse_cloud", f"SSE, {CLOUD_LABEL}", "aqua", lambda r: r["transport"] == "sse" and r["variant"] == "cloud-sse"),
]


def style(theme):
    c = THEMES[theme]
    plt.rcParams.update({
        "figure.facecolor": c["surface"], "axes.facecolor": c["surface"], "savefig.facecolor": c["surface"],
        "text.color": c["ink"], "axes.labelcolor": c["ink2"], "xtick.color": c["ink2"], "ytick.color": c["ink2"],
        "axes.edgecolor": c["grid"], "font.family": "DejaVu Sans", "font.size": 10,
        "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True,
        "grid.color": c["grid"], "grid.linewidth": 0.8, "axes.axisbelow": True,
        "svg.fonttype": "path", "svg.hashsalt": "hydrant", "legend.frameon": False,
    })
    return c


def save(fig, name, theme):
    OUT.mkdir(parents=True, exist_ok=True)
    for ext in ("svg", "png"):
        meta = {"Date": None} if ext == "svg" else None
        fig.savefig(OUT / f"{name}{SUFFIX[theme]}.{ext}", dpi=160, metadata=meta)
    plt.close(fig)


def frame(fig, title, source, c, subtitle=None):
    width = fig.get_figwidth()
    wrapped = textwrap.fill(title, int(width * 8.6))
    fig.text(0.015, 0.975, wrapped, ha="left", va="top", fontsize=12.5, fontweight="bold", color=c["ink"])
    if subtitle:
        fig.text(0.015, 0.975 - 0.055 * (wrapped.count("\n") + 1) - 0.01, subtitle, ha="left", va="top", fontsize=9.5, color=c["ink2"])
    fig.text(0.015, 0.012, "\n".join(textwrap.fill(p, int(width * 15)) for p in source.split("\n")),
             ha="left", va="bottom", fontsize=7.5, color=c["ink2"], linespacing=1.3)


def series_rows(rows, pred):
    return [r for r in rows if pred(r)]


def chart_rss(theme):
    c = style(theme)
    fig, ax = plt.subplots(figsize=(8, 4.8))
    fig.subplots_adjust(left=0.1, right=0.94, top=0.78, bottom=0.22)
    fits = {}
    markers = {"ws_before": "o", "ws_after": "s", "sse": "^", "ws_cloud": "D", "sse_cloud": "v"}
    for key, label, color, pred in SERIES + CLOUD_SERIES:
        if key in ("ws_cloud", "sse_cloud"):
            rows = series_rows(IDLE, pred)
            if rows:
                ax.scatter([num(r["gauge_connections"]) for r in rows], [num(r["server_rss_kb"]) / 1024 for r in rows],
                           s=30, marker=markers[key], zorder=3, linewidths=1.2, facecolors=c["surface"],
                           edgecolors=c[color], label=label)
            continue
        rows = series_rows(IDLE, pred)
        pts = [(num(r["gauge_connections"]), num(r["server_rss_kb"]) / 1024) for r in rows]
        slope, icpt = ols(pts)
        fits[key] = slope * 1024 * 1024
        xmax = max(p[0] for p in pts)
        ax.plot([0, xmax], [icpt, icpt + slope * xmax], color=c[color], lw=2, solid_capstyle="round", zorder=2)
        for r, p in zip(rows, pts):
            hollow = r["status"] != "ok"
            ax.scatter(p[0], p[1], s=42, marker=markers[key], zorder=3, linewidths=1.6,
                       facecolors=c["surface"] if hollow else c[color], edgecolors=c[color])
        bytes_per = slope * 1024 * 1024
        left = key != "ws_before"
        ax.annotate(f"{label}\n{bytes_per / 1024:,.0f} KiB ({bytes_per:,.0f} bytes) per connection",
                    xy=(xmax, icpt + slope * xmax), xytext=(-10 if left else 10, 10 if left else -4),
                    textcoords="offset points", color=c["ink"], fontsize=9.5,
                    va="bottom" if left else "top", ha="right" if left else "left")
    ax.set_xlim(0, 100_000)
    ax.set_ylim(0, None)
    ax.set_xlabel("Live connections")
    ax.set_ylabel("Server RSS (MiB)")
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: fmt_int(v)))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: fmt_int(v)))
    ratio = fits["ws_before"] / fits["ws_after"]
    if CLOUD_SWEEP:
        ax.legend(loc="center right", fontsize=8, labelcolor=c["ink"])
    frame(
        fig,
        f"The WebSocket buffer fix cut server memory per connection about {ratio:.0f}x",
        "Source: bench/results/sweep.csv, phase idle, all runs. Lines are least-squares fits of RSS against live connections.\n"
        "Hollow marker: run stopped by the memory floor after its idle sample, or a run from the cloud sandbox (bench/results/cloud, no fit). "
        "SSE pools the unpatched, current and sse-extend runs (same server code).",
        c,
        "Server process RSS with every connection established and idle; kernel socket memory is not included",
    )
    save(fig, "rss-vs-connections", theme)
    return fits


def median_at_max(rows, key):
    top = max(int(r["n"]) for r in rows if num(r[key]) is not None)
    vals = [num(r[key]) for r in rows if int(r["n"]) == top and num(r[key]) is not None]
    return top, statistics.median(vals)


def chart_per_conn(theme):
    c = style(theme)
    cloud_rows = sum(1 for cs in CLOUD_SERIES if series_rows(IDLE, cs[3]))
    fig, ax = plt.subplots(figsize=(8, 4.6 + 1.1 * cloud_rows))
    fig.subplots_adjust(left=0.27, right=0.95, top=0.78 + 0.04 * cloud_rows, bottom=0.25 - 0.05 * cloud_rows)
    parts = [
        ("server_rss_per_conn_bytes", "Server RSS", None),
        ("loadgen_rss_per_conn_bytes", "Load generator RSS (client)", "////"),
        ("slab_per_conn_bytes", "Kernel slab growth (both ends)", "...."),
    ]
    height, gap = 0.24, 0.27
    yticks, ylabels, server_vals = [], [], {}
    for gi, (key, label, color, pred) in enumerate(SERIES + [cs for cs in CLOUD_SERIES if series_rows(IDLE, cs[3])]):
        rows = [r for r in series_rows(IDLE, pred) if r["status"] == "ok"]
        yc = gi * 1.15
        top_n = max(int(r["n"]) for r in rows)
        for pi, (col, plabel, hatch) in enumerate(parts):
            y = yc + (pi - 1) * gap
            have = [r for r in rows if int(r["n"]) == top_n and num(r[col]) is not None]
            if not have:
                ax.text(2, y, "not measured for this build", va="center", fontsize=8.5, color=c["ink2"])
                continue
            val = statistics.median(num(r[col]) for r in have) / 1024
            if pi == 0:
                server_vals[key] = val
            ax.barh(y, val, height=height, color=c[color] if hatch is None else c["surface"],
                    edgecolor=c[color], hatch=hatch, linewidth=1.2, zorder=3)
            ax.text(val + 1.5, y, f"{val:,.1f} KiB", va="center", fontsize=9, color=c["ink"])
        yticks.append(yc)
        ylabels.append(f"{label}\n(median at N = {fmt_int(top_n)})")
    ax.set_yticks(yticks)
    ax.set_yticklabels(ylabels, color=c["ink"])
    ax.invert_yaxis()
    ax.set_xlim(0, max(server_vals.values()) * 1.18)
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("Memory per connection (KiB)")
    handles = [
        matplotlib.patches.Patch(facecolor=c["ink2"], edgecolor=c["ink2"], label="Server RSS"),
        matplotlib.patches.Patch(facecolor=c["surface"], edgecolor=c["ink2"], hatch="////", label="Load generator RSS (client)"),
        matplotlib.patches.Patch(facecolor=c["surface"], edgecolor=c["ink2"], hatch="....", label="Kernel slab growth, both ends"),
    ]
    ax.legend(handles=handles, loc="lower right", fontsize=8.5, labelcolor=c["ink"])
    frame(
        fig,
        f"Per connection: {server_vals['ws_before']:.0f} KiB for WebSocket before the fix, "
        f"{server_vals['ws_after']:.0f} after, {server_vals['sse']:.0f} for SSE",
        "Source: bench/results/sweep.csv and bench/results/cloud/sweep.csv (cloud sandbox rows), phase idle, status ok runs only, median over the runs at the largest such N of each series. KiB = 1024 bytes.\n"
        "Load generator figure is its whole RSS divided by N, so it includes fixed overhead. Slab is the SUnreclaim delta; it needs the newer sweep columns.",
        c,
        "Server, client and kernel memory shown separately",
    )
    save(fig, "memory-per-connection", theme)


def runs_ok(pred):
    return [r for r in RUNS if r["status"] == "ok" and num(r["lat_p50_ms"]) is not None and pred(r)]


def chart_latency(theme):
    c = style(theme)
    fig, axes = plt.subplots(1, 2, figsize=(9, 4.8), sharey=True)
    fig.subplots_adjust(left=0.08, right=0.97, top=0.74, bottom=0.25, wspace=0.08)
    pcts = [("lat_p50_ms", "p50", "blue"), ("lat_p99_ms", "p99", "orange"), ("lat_max_ms", "max", "aqua")]
    pct_marker = {"lat_p50_ms": "o", "lat_p99_ms": "s", "lat_max_ms": "^"}
    build_style = {
        "unpatched": {"fill": False},
        "current": {"fill": True},
        "backlog4096": {"fill": True},
        "backlog1024": {"fill": True},
        "tokio-default-bind": {"fill": True},
        "sse-extend": {"fill": True},
    }
    p99_ws = []
    for ax, transport, title in ((axes[0], "ws", "WebSocket"), (axes[1], "sse", "SSE")):
        rows = runs_ok(lambda r: r["transport"] == transport)
        line_variants = ("current",) if transport == "ws" else ("current", "sse-extend")
        for col, label, color in pcts:
            groups = {}
            for r in rows:
                groups.setdefault(int(r["n"]), []).append(r)
            xs, meds = [], []
            for n in sorted(groups):
                cur = [num(r[col]) for r in groups[n] if r["variant"] in line_variants]
                if cur:
                    xs.append(n)
                    meds.append(statistics.median(cur))
            ax.plot(xs, meds, color=c[color], lw=1.6, zorder=2, solid_capstyle="round")
            for n, grp in sorted(groups.items()):
                for i, r in enumerate(grp):
                    jitter = 1 + 0.025 * ((i % 5) - 2)
                    filled = build_style.get(r["variant"], {"fill": True})["fill"]
                    ax.scatter(n * jitter, num(r[col]), s=30, marker=pct_marker[col], zorder=3, linewidths=1.3,
                               facecolors=c[color] if filled else c["surface"], edgecolors=c[color])
            if xs:
                ax.annotate(label, xy=(xs[-1], meds[-1]), xytext=(14, 0), textcoords="offset points",
                            color=c["ink"], fontsize=9.5, va="center", ha="left")
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_xticks([1000, 10000, 100000])
        ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: fmt_int(v)))
        ax.xaxis.set_minor_formatter(FuncFormatter(lambda v, _: ""))
        ax.set_xlim(700, 190_000)
        ax.set_xlabel("Connections")
        ax.set_title(title, loc="left", fontsize=10.5, color=c["ink"], pad=6)
        ax.tick_params(axis="x", labelsize=8.5)
        if transport == "ws":
            p99_ws = [num(r["lat_p99_ms"]) for r in rows if int(r["n"]) == 100000]
            n_ws = len(p99_ws)
    axes[0].set_ylabel("Delivery latency (ms, log scale)")
    axes[0].yaxis.set_major_formatter(FuncFormatter(lambda v, _: fmt_int(v)))
    handles = [
        Line2D([], [], marker="o", color=c["ink2"], linestyle="", markerfacecolor=c["ink2"], label="current build run"),
        Line2D([], [], marker="o", color=c["ink2"], linestyle="", markerfacecolor=c["surface"], label="unpatched build run"),
    ]
    axes[0].legend(handles=handles, loc="upper left", fontsize=8.5, labelcolor=c["ink"])
    p50_ws = [num(r["lat_p50_ms"]) for r in runs_ok(lambda r: r["transport"] == "ws" and int(r["n"]) == 100000)]
    frame(
        fig,
        f"At 100,000 WebSockets: p50 about {statistics.median(p50_ws):.0f} ms, "
        f"p99 {fmt_int(min(p99_ws))} to {fmt_int(max(p99_ws))} ms over {n_ws} runs",
        "Source: bench/results/runs.csv, status ok runs, one point per run; lines join the per-N median of the current build (SSE: current and sse-extend, same code). 1 msg/s, 64-byte payload, one topic.\n"
        "Client and server share this machine's cores; latency uses intended send time. Backlog experiment runs are included at 100,000 (same server code).",
        c,
        "Time from intended publish to receipt, per delivery, for every run",
    )
    save(fig, "latency-vs-connections", theme)


def ramp(name):
    rows = [r for r in read(name) if r["phase"].startswith("connecting")]
    start = float(rows[0]["t_s"])
    return [(float(r["t_s"]) - start, float(r["gauge_connections"])) for r in rows if r["gauge_connections"] != ""]


def chart_connect(theme):
    c = style(theme)
    fig, (ax, bx) = plt.subplots(1, 2, figsize=(9.5, 4.9), gridspec_kw={"width_ratios": [1.1, 1]})
    fig.subplots_adjust(left=0.08, right=0.97, top=0.74, bottom=0.25, wspace=0.28)
    variant_style = {
        ("ws", "unpatched"): ("orange", "o", False, "WebSocket, unpatched build"),
        ("ws", "current"): ("blue", "s", True, "WebSocket, current build"),
        ("ws", "backlog4096"): ("blue", "D", True, "WebSocket, backlog 4096"),
        ("ws", "backlog1024"): ("blue", "P", True, "WebSocket, backlog 1024"),
        ("ws", "tokio-default-bind"): ("blue", "X", True, "WebSocket, tokio default bind"),
        ("sse", "unpatched"): ("aqua", "^", False, "SSE, unpatched build"),
        ("sse", "current"): ("aqua", "v", True, "SSE, current build"),
        ("sse", "sse-extend"): ("aqua", "d", True, "SSE, extended run"),
    }
    variant_style[("ws", "cloud-ws")] = ("blue", "D", False, f"WebSocket, {CLOUD_LABEL}")
    variant_style[("sse", "cloud-sse")] = ("aqua", "v", False, f"SSE, {CLOUD_LABEL}")
    seen = {}
    ws100 = []
    by_backlog = {}
    for r in RUNS + CLOUD_RUNS:
        if r["status"] != "ok" or num(r["connect_secs"]) is None:
            continue
        variant = r["variant"].removesuffix("-b")
        key = (r["transport"], variant)
        if key not in variant_style:
            continue
        color, marker, filled, label = variant_style[key]
        n = int(r["n"])
        i = seen.get((key, n), 0)
        seen[(key, n)] = i + 1
        x = n * (1 + 0.03 * i)
        ax.scatter(x, num(r["connect_secs"]), s=36, marker=marker, zorder=3, linewidths=1.4,
                   facecolors=c[color] if filled else c["surface"], edgecolors=c[color], label=label if i == 0 and (key, "l") not in seen else None)
        seen[(key, "l")] = 1
        if key[0] == "ws" and n == 100000:
            ws100.append(num(r["connect_secs"]))
            if variant in ("backlog1024", "backlog4096"):
                by_backlog.setdefault(variant, []).append(num(r["connect_secs"]))
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xticks([1000, 10000, 100000])
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: fmt_int(v)))
    ax.xaxis.set_minor_formatter(FuncFormatter(lambda v, _: ""))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
    ax.yaxis.set_minor_formatter(FuncFormatter(lambda v, _: ""))
    ax.set_xlim(700, 190_000)
    ax.set_xlabel("Connections requested")
    ax.set_ylabel("Connect phase (s, log scale)")
    ax.tick_params(axis="x", labelsize=8.5)
    ax.set_title("Time to establish all connections", loc="left", fontsize=10.5, color=c["ink"], pad=6)
    ax.legend(loc="upper left", fontsize=7, labelcolor=c["ink"], handlelength=1.2, borderaxespad=0.2)

    def table_secs(variant, run_id):
        row = next(r for r in RUNS if r["variant"] == variant and r["run_id"] == str(run_id) and r["transport"] == "ws")
        return num(row["connect_secs"])

    traces = [
        ("ramp_ws.csv", "-", "backlog 128, run 9", table_secs("current", 9)),
        ("ramp_ws_backlog4096.csv", "--", "backlog 4096, rep 1", table_secs("backlog4096", 1)),
        ("ramp_ws_backlog1024.csv", ":", "backlog 1024", table_secs("backlog1024", 2)),
    ]
    for name, dash, label, table in traces:
        pts = ramp(name)
        xs = [p[0] for p in pts]
        ys = [p[1] for p in pts]
        done = next((x for x, y in pts if y >= max(ys) - 1), xs[-1])
        bx.plot(xs, ys, color=c["blue"], lw=2.2, ls=dash, zorder=3, label=f"{label}: gauge full {done:.1f} s, table {table:.1f} s")
    bx.legend(loc="lower right", fontsize=7.5, labelcolor=c["ink"], handlelength=2.2, borderaxespad=0.2)
    bx.set_xlabel("Seconds since loadgen launch (about)")
    bx.set_ylabel("Established connections")
    bx.yaxis.set_major_formatter(FuncFormatter(lambda v, _: fmt_int(v)))
    bx.set_ylim(0, 105_000)
    bx.set_title("Three 100,000-connection ramps", loc="left", fontsize=10.5, color=c["ink"], pad=6)
    frame(
        fig,
        f"Connecting 100,000 WebSockets took between {min(ws100):.1f} and {max(ws100):.0f} s across {len(ws100)} runs",
        "Source: bench/results/runs.csv and bench/results/cloud/runs.csv, hollow diamonds and triangles (left), bench/results/ramp_ws.csv, ramp_ws_backlog4096.csv, ramp_ws_backlog1024.csv (right).\n"
        "Backlog 1024 runs (s): " + ", ".join(f"{t:.1f}" for t in by_backlog["backlog1024"])
        + ". Backlog 4096 runs (s): " + ", ".join(f"{t:.1f}" for t in by_backlog["backlog4096"])
        + ". Right panel: time zero is the first connecting-phase sample, within 0.5 s of launch; gauge samples only (blank scrapes dropped); \"table\" is connect_secs, which runs until every attempt connected or failed.",
        c,
        "In the slow ramps, progress falls from over 10,000 connections per second to a few hundred or fewer near 65,000",
    )
    save(fig, "connect-phase", theme)
    return ws100


def main():
    for theme in THEMES:
        chart_rss(theme)
        chart_per_conn(theme)
        chart_latency(theme)
        chart_connect(theme)
    print(f"wrote charts to {OUT}")


if __name__ == "__main__":
    sys.exit(main())
