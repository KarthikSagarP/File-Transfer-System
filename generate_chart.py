#!/usr/bin/env python3
"""
Generate benchmark visualization charts.

Reads from:
    benchmark_results.json       -> backend comparison bar chart (legacy)
    benchmark_suite_results.json -> full suite charts (4 panels + overview)

Outputs to assets/ directory.

Usage:
    python generate_chart.py
"""

import json
import os
import sys

try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
    import numpy as np
except ImportError:
    print("matplotlib required: pip install matplotlib")
    sys.exit(1)

os.makedirs("assets", exist_ok=True)

# ── Theme ────────────────────────────────────────────────────────────────────

COLORS = {
    "bg": "#0D1117", "panel": "#161B22", "grid": "#21262D",
    "border": "#30363D", "text": "#E6EDF3", "dim": "#8B949E",
    "label": "#C9D1D9",
    "blue": "#4A90D9", "green": "#50C878", "red": "#FF6B6B",
    "yellow": "#F0C040", "purple": "#B388FF", "cyan": "#4DD0E1",
    "orange": "#FF9800",
}

BACKEND = {
    "Threaded": ("#4A90D9", "#3570A8"),
    "Async":    ("#50C878", "#3DA85C"),
    "Hybrid":   ("#FF6B6B", "#D94545"),
}


def style_ax(ax):
    ax.set_facecolor(COLORS["panel"])
    ax.tick_params(colors=COLORS["label"], labelsize=9)
    for s in ['top', 'right']:
        ax.spines[s].set_visible(False)
    for s in ['bottom', 'left']:
        ax.spines[s].set_color(COLORS["border"])
    ax.grid(axis='y', color=COLORS["grid"], linewidth=0.8, zorder=0)


def bar_labels(ax, bars, fmt="{:.0f}", off=0):
    for b in bars:
        ax.text(b.get_x() + b.get_width()/2, b.get_height() + off,
                fmt.format(b.get_height()), ha='center', va='bottom',
                fontsize=10, fontweight='bold', color=COLORS["text"])


# ── Legacy Chart ─────────────────────────────────────────────────────────────

def gen_legacy():
    if not os.path.isfile("benchmark_results.json"):
        return
    with open("benchmark_results.json") as f:
        data = json.load(f)

    backends = [b for b in data if data[b] is not None]
    if not backends:
        return

    avg = [data[b]["avg_time"] * 1000 for b in backends]
    mn = [data[b]["min_time"] * 1000 for b in backends]
    mx = [data[b]["max_time"] * 1000 for b in backends]
    clients = data[backends[0]]["clients"]
    fkb = data[backends[0]]["file_size_kb"]
    tp = [(clients * fkb) / data[b]["avg_time"] for b in backends]
    bc = [BACKEND.get(b, ("#4A90D9", "#3570A8"))[0] for b in backends]
    be = [BACKEND.get(b, ("#4A90D9", "#3570A8"))[1] for b in backends]

    fig, (a1, a2) = plt.subplots(1, 2, figsize=(14, 5.5))
    fig.patch.set_facecolor(COLORS["bg"])
    for a in (a1, a2):
        style_ax(a)

    bars1 = a1.bar(backends, avg, color=bc, edgecolor=be, linewidth=1.5, width=0.55, zorder=3)
    a1.errorbar(backends, avg, yerr=[[a-m for a,m in zip(avg,mn)], [m-a for a,m in zip(avg,mx)]],
                fmt='none', ecolor=COLORS["dim"], elinewidth=1.5, capsize=8, capthick=1.5, zorder=4)
    a1.set_title("Avg Transfer Time (lower is better)", fontsize=14, fontweight='bold', color=COLORS["text"], pad=15)
    a1.set_ylabel("Time (ms)", fontsize=12, color=COLORS["label"])
    bar_labels(a1, bars1, "{:.0f} ms", max(avg)*0.04)

    bars2 = a2.bar(backends, tp, color=bc, edgecolor=be, linewidth=1.5, width=0.55, zorder=3)
    a2.set_title("Throughput (higher is better)", fontsize=14, fontweight='bold', color=COLORS["text"], pad=15)
    a2.set_ylabel("KB/s", fontsize=12, color=COLORS["label"])
    bar_labels(a2, bars2, "{:.0f}", max(tp)*0.03)

    fig.suptitle("Server Backend Benchmark", fontsize=18, fontweight='bold', color=COLORS["text"], y=1.02)
    fig.text(0.5, 0.97, f"{clients} clients  ·  {fkb} KB  ·  10% drop  ·  5 rounds",
             ha='center', fontsize=10, color=COLORS["dim"], transform=fig.transFigure)
    plt.tight_layout(rect=[0, 0, 1, 0.93])
    plt.savefig("assets/benchmark_chart.png", dpi=150, bbox_inches='tight',
                facecolor=COLORS["bg"], edgecolor='none', pad_inches=0.3)
    plt.close()
    print("  Generated: assets/benchmark_chart.png")


# ── Suite Charts ─────────────────────────────────────────────────────────────

def gen_suite():
    if not os.path.isfile("benchmark_suite_results.json"):
        print("  No benchmark_suite_results.json found.")
        return
    with open("benchmark_suite_results.json") as f:
        data = json.load(f)

    if "throughput" in data and data["throughput"]:
        _throughput(data["throughput"])
    if "scalability" in data and data["scalability"]:
        _scalability(data["scalability"])
    if "reliability" in data and data["reliability"]:
        _reliability(data["reliability"])
    if "backends" in data and data["backends"]:
        _backends(data["backends"])
    _overview(data)


def _throughput(d):
    sizes = list(d.keys())
    ms = [d[s]["avg_ms"] for s in sizes]
    tp = [d[s]["throughput_kbps"] for s in sizes]

    fig, (a1, a2) = plt.subplots(1, 2, figsize=(14, 5))
    fig.patch.set_facecolor(COLORS["bg"])
    for a in (a1, a2):
        style_ax(a)

    bars1 = a1.bar(sizes, ms, color=COLORS["blue"], edgecolor="#3570A8", linewidth=1.5, width=0.5, zorder=3)
    a1.set_title("Transfer Time vs File Size", fontsize=13, fontweight='bold', color=COLORS["text"], pad=12)
    a1.set_ylabel("Time (ms)", fontsize=11, color=COLORS["label"])
    bar_labels(a1, bars1, "{:.0f}", max(ms)*0.03)

    bars2 = a2.bar(sizes, tp, color=COLORS["green"], edgecolor="#3DA85C", linewidth=1.5, width=0.5, zorder=3)
    a2.set_title("Throughput vs File Size", fontsize=13, fontweight='bold', color=COLORS["text"], pad=12)
    a2.set_ylabel("KB/s", fontsize=11, color=COLORS["label"])
    bar_labels(a2, bars2, "{:.0f}", max(tp)*0.03)

    fig.suptitle("A. Throughput Benchmark (0% errors)", fontsize=16, fontweight='bold', color=COLORS["text"], y=1.01)
    plt.tight_layout(rect=[0, 0, 1, 0.94])
    plt.savefig("assets/bench_throughput.png", dpi=150, bbox_inches='tight', facecolor=COLORS["bg"], pad_inches=0.3)
    plt.close()
    print("  Generated: assets/bench_throughput.png")


def _scalability(d):
    labels = list(d.keys())
    clients = [l.split("_")[0] for l in labels]
    avg = [d[l]["avg_ms"] for l in labels]
    agg = [d[l]["aggregate_kbps"] for l in labels]

    fig, (a1, a2) = plt.subplots(1, 2, figsize=(14, 5))
    fig.patch.set_facecolor(COLORS["bg"])
    for a in (a1, a2):
        style_ax(a)

    a1.plot(clients, avg, 'o-', color=COLORS["cyan"], linewidth=2.5, markersize=8, zorder=3)
    a1.fill_between(clients, avg, alpha=0.15, color=COLORS["cyan"], zorder=2)
    a1.set_title("Avg Client Time vs Concurrency", fontsize=13, fontweight='bold', color=COLORS["text"], pad=12)
    a1.set_ylabel("Time (ms)", fontsize=11, color=COLORS["label"])
    a1.set_xlabel("Concurrent Clients", fontsize=11, color=COLORS["label"])
    for x, y in zip(clients, avg):
        a1.annotate(f"{y:.0f}", (x, y), textcoords="offset points", xytext=(0, 12),
                    ha='center', fontsize=9, fontweight='bold', color=COLORS["text"])

    a2.plot(clients, agg, 's-', color=COLORS["orange"], linewidth=2.5, markersize=8, zorder=3)
    a2.fill_between(clients, agg, alpha=0.15, color=COLORS["orange"], zorder=2)
    a2.set_title("Aggregate Throughput vs Concurrency", fontsize=13, fontweight='bold', color=COLORS["text"], pad=12)
    a2.set_ylabel("KB/s", fontsize=11, color=COLORS["label"])
    a2.set_xlabel("Concurrent Clients", fontsize=11, color=COLORS["label"])
    for x, y in zip(clients, agg):
        a2.annotate(f"{y:.0f}", (x, y), textcoords="offset points", xytext=(0, 12),
                    ha='center', fontsize=9, fontweight='bold', color=COLORS["text"])

    fig.suptitle("B. Scalability Benchmark (50KB, 0% errors)", fontsize=16, fontweight='bold', color=COLORS["text"], y=1.01)
    plt.tight_layout(rect=[0, 0, 1, 0.94])
    plt.savefig("assets/bench_scalability.png", dpi=150, bbox_inches='tight', facecolor=COLORS["bg"], pad_inches=0.3)
    plt.close()
    print("  Generated: assets/bench_scalability.png")


def _reliability(d):
    scenarios = list(d.keys())
    rates = [d[s]["success_rate"] for s in scenarios]
    times = [d[s]["avg_ms"] for s in scenarios]

    fig, (a1, a2) = plt.subplots(1, 2, figsize=(14, 5))
    fig.patch.set_facecolor(COLORS["bg"])
    for a in (a1, a2):
        style_ax(a)

    bc = [COLORS["green"] if r == 100 else COLORS["yellow"] if r >= 80 else COLORS["red"] for r in rates]
    bars1 = a1.bar(range(len(scenarios)), rates, color=bc, width=0.6, zorder=3)
    a1.set_xticks(range(len(scenarios)))
    a1.set_xticklabels(scenarios, rotation=30, ha='right', fontsize=9)
    a1.set_ylim(0, 115)
    a1.axhline(y=100, color=COLORS["green"], linewidth=1, linestyle='--', alpha=0.3, zorder=1)
    a1.set_title("Success Rate by Error Condition", fontsize=13, fontweight='bold', color=COLORS["text"], pad=12)
    a1.set_ylabel("Success %", fontsize=11, color=COLORS["label"])
    bar_labels(a1, bars1, "{:.0f}%", 1.5)

    bars2 = a2.bar(range(len(scenarios)), times, color=COLORS["purple"], edgecolor="#9060D0", linewidth=1.2, width=0.6, zorder=3)
    a2.set_xticks(range(len(scenarios)))
    a2.set_xticklabels(scenarios, rotation=30, ha='right', fontsize=9)
    a2.set_title("Avg Time by Error Condition", fontsize=13, fontweight='bold', color=COLORS["text"], pad=12)
    a2.set_ylabel("Time (ms)", fontsize=11, color=COLORS["label"])
    bar_labels(a2, bars2, "{:.0f}", max(times)*0.03 if times else 1)

    fig.suptitle("C. Reliability Benchmark (50KB, 5 clients)", fontsize=16, fontweight='bold', color=COLORS["text"], y=1.01)
    plt.tight_layout(rect=[0, 0, 1, 0.94])
    plt.savefig("assets/bench_reliability.png", dpi=150, bbox_inches='tight', facecolor=COLORS["bg"], pad_inches=0.3)
    plt.close()
    print("  Generated: assets/bench_reliability.png")


def _backends(d):
    backends = [b for b in d if d[b] is not None]
    if not backends:
        return

    all_sizes = set()
    for b in backends:
        all_sizes.update(d[b].keys())
    sizes = sorted(all_sizes, key=lambda s: int(s.replace("KB", "")))

    fig, (a1, a2) = plt.subplots(1, 2, figsize=(14, 5))
    fig.patch.set_facecolor(COLORS["bg"])
    for a in (a1, a2):
        style_ax(a)

    x = np.arange(len(sizes))
    w = 0.25
    offsets = np.linspace(-w, w, len(backends))

    for i, b in enumerate(backends):
        vals = [d[b].get(s, {}).get("avg_ms", 0) for s in sizes]
        c = BACKEND.get(b, ("#4A90D9", "#3570A8"))
        a1.bar(x + offsets[i], vals, w*0.9, label=b, color=c[0], edgecolor=c[1], linewidth=1.2, zorder=3)

    a1.set_xticks(x)
    a1.set_xticklabels(sizes)
    a1.set_title("Avg Time by Backend & Size", fontsize=13, fontweight='bold', color=COLORS["text"], pad=12)
    a1.set_ylabel("Time (ms)", fontsize=11, color=COLORS["label"])
    a1.legend(facecolor=COLORS["panel"], edgecolor=COLORS["border"], labelcolor=COLORS["text"], fontsize=9)

    for i, b in enumerate(backends):
        vals = [d[b].get(s, {}).get("throughput_kbps", 0) for s in sizes]
        c = BACKEND.get(b, ("#4A90D9", "#3570A8"))
        a2.bar(x + offsets[i], vals, w*0.9, label=b, color=c[0], edgecolor=c[1], linewidth=1.2, zorder=3)

    a2.set_xticks(x)
    a2.set_xticklabels(sizes)
    a2.set_title("Throughput by Backend & Size", fontsize=13, fontweight='bold', color=COLORS["text"], pad=12)
    a2.set_ylabel("KB/s", fontsize=11, color=COLORS["label"])
    a2.legend(facecolor=COLORS["panel"], edgecolor=COLORS["border"], labelcolor=COLORS["text"], fontsize=9)

    fig.suptitle("D. Backend Comparison (5 clients, 5% drop, 2% corrupt)", fontsize=16, fontweight='bold', color=COLORS["text"], y=1.01)
    plt.tight_layout(rect=[0, 0, 1, 0.94])
    plt.savefig("assets/bench_backends.png", dpi=150, bbox_inches='tight', facecolor=COLORS["bg"], pad_inches=0.3)
    plt.close()
    print("  Generated: assets/bench_backends.png")


def _overview(data):
    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    fig.patch.set_facecolor(COLORS["bg"])
    for row in axes:
        for ax in row:
            style_ax(ax)

    # A: Throughput
    ax = axes[0][0]
    if "throughput" in data and data["throughput"]:
        t = data["throughput"]
        sizes = list(t.keys())
        vals = [t[s]["throughput_kbps"] for s in sizes]
        bars = ax.bar(sizes, vals, color=COLORS["green"], edgecolor="#3DA85C", linewidth=1.2, width=0.5, zorder=3)
        bar_labels(ax, bars, "{:.0f}", max(vals)*0.03)
    ax.set_title("A. Throughput (KB/s)", fontsize=12, fontweight='bold', color=COLORS["text"], pad=10)

    # B: Scalability
    ax = axes[0][1]
    if "scalability" in data and data["scalability"]:
        s = data["scalability"]
        labels = list(s.keys())
        c = [l.split("_")[0] for l in labels]
        a = [s[l]["aggregate_kbps"] for l in labels]
        ax.plot(c, a, 'o-', color=COLORS["orange"], linewidth=2.5, markersize=7, zorder=3)
        ax.fill_between(c, a, alpha=0.15, color=COLORS["orange"], zorder=2)
        for xi, yi in zip(c, a):
            ax.annotate(f"{yi:.0f}", (xi, yi), textcoords="offset points", xytext=(0, 10),
                        ha='center', fontsize=9, fontweight='bold', color=COLORS["text"])
    ax.set_title("B. Scalability (agg KB/s)", fontsize=12, fontweight='bold', color=COLORS["text"], pad=10)
    ax.set_xlabel("Clients", fontsize=10, color=COLORS["label"])

    # C: Reliability
    ax = axes[1][0]
    if "reliability" in data and data["reliability"]:
        r = data["reliability"]
        sc = list(r.keys())
        rates = [r[s]["success_rate"] for s in sc]
        bc = [COLORS["green"] if v == 100 else COLORS["yellow"] if v >= 80 else COLORS["red"] for v in rates]
        bars = ax.bar(range(len(sc)), rates, color=bc, width=0.6, zorder=3)
        ax.set_xticks(range(len(sc)))
        ax.set_xticklabels(sc, rotation=35, ha='right', fontsize=8)
        ax.set_ylim(0, 115)
        ax.axhline(y=100, color=COLORS["green"], linewidth=1, linestyle='--', alpha=0.3)
        bar_labels(ax, bars, "{:.0f}%", 1.5)
    ax.set_title("C. Reliability (success %)", fontsize=12, fontweight='bold', color=COLORS["text"], pad=10)

    # D: Backend at largest size
    ax = axes[1][1]
    if "backends" in data and data["backends"]:
        b = data["backends"]
        bks = [k for k in b if b[k] is not None]
        if bks:
            all_s = set()
            for bk in bks:
                all_s.update(b[bk].keys())
            largest = max(all_s, key=lambda s: int(s.replace("KB", "")))
            vals = [b[bk].get(largest, {}).get("throughput_kbps", 0) for bk in bks]
            colors = [BACKEND.get(bk, ("#4A90D9",))[0] for bk in bks]
            edges = [BACKEND.get(bk, ("", "#3570A8"))[1] for bk in bks]
            bars = ax.bar(bks, vals, color=colors, edgecolor=edges, linewidth=1.5, width=0.5, zorder=3)
            bar_labels(ax, bars, "{:.0f}", max(vals)*0.03)
            ax.set_title(f"D. Backends at {largest} (KB/s)", fontsize=12, fontweight='bold', color=COLORS["text"], pad=10)
    ax.set_ylabel("KB/s", fontsize=10, color=COLORS["label"])

    fig.suptitle("Benchmark Suite Overview", fontsize=20, fontweight='bold', color=COLORS["text"], y=1.01)
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.savefig("assets/bench_overview.png", dpi=150, bbox_inches='tight', facecolor=COLORS["bg"], pad_inches=0.3)
    plt.close()
    print("  Generated: assets/bench_overview.png")


# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\nGenerating benchmark charts...")
    print("-" * 40)
    gen_legacy()
    gen_suite()
    print("\nDone! Charts saved to assets/")
