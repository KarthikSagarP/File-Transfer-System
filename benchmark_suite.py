"""
Comprehensive benchmark suite for the file transfer system.

Tests across 4 categories:
    A. Throughput  — raw speed at various file sizes
    B. Scalability — throughput vs client count
    C. Reliability — success rate under error conditions
    D. Backend     — threaded vs async vs hybrid comparison

Outputs results to benchmark_suite_results.json and prints a summary.

Usage:
    python benchmark_suite.py                    # run all benchmarks
    python benchmark_suite.py --quick            # fast subset (~30s)
    python benchmark_suite.py --category A       # single category
"""

import os
import sys
import time
import socket
import threading
import tempfile
import json
import argparse
import subprocess

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from protocol import DEFAULT_HOST
from client import transfer_file

RESULTS_FILE = "benchmark_suite_results.json"


# ── Helpers ──────────────────────────────────────────────────────────────────

def create_test_file(size_bytes):
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".bench")
    tmp.write(os.urandom(size_bytes))
    tmp.close()
    return tmp.name


def wait_for_server(host, port, timeout=5):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except (socket.error, OSError):
            time.sleep(0.1)
    return False


def start_server_process(script, port):
    proc = subprocess.Popen(
        [sys.executable, script, str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if not wait_for_server(DEFAULT_HOST, port, timeout=5):
        proc.terminate()
        return None
    return proc


def stop_server(proc):
    if proc:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
        time.sleep(0.3)


def run_transfers(host, port, test_files, timeout_per=60):
    """Run concurrent transfers, return list of (success, elapsed)."""
    results = [None] * len(test_files)

    def worker(idx, fpath):
        start = time.time()
        try:
            ok = transfer_file(fpath, host=host, port=port)
            results[idx] = (ok, time.time() - start)
        except Exception:
            results[idx] = (False, time.time() - start)

    threads = [threading.Thread(target=worker, args=(i, f))
               for i, f in enumerate(test_files)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=timeout_per)

    return [r for r in results if r is not None]


def summarize(results):
    """Compute stats from a list of (success, elapsed) tuples."""
    successes = [e for ok, e in results if ok]
    failures = sum(1 for ok, _ in results if not ok)
    if not successes:
        return {"success_rate": 0, "avg_ms": 0, "min_ms": 0,
                "max_ms": 0, "failures": failures}
    return {
        "success_rate": len(successes) / len(results) * 100,
        "avg_ms": round(sum(successes) / len(successes) * 1000, 1),
        "min_ms": round(min(successes) * 1000, 1),
        "max_ms": round(max(successes) * 1000, 1),
        "failures": failures,
    }


# ── Category A: Throughput ───────────────────────────────────────────────────

def bench_throughput(port, quick=False):
    """Measure throughput at various file sizes (0% errors)."""
    print("\n  Category A: Throughput (0% errors)")
    print("  " + "-" * 50)

    sizes_kb = [1, 10, 100, 1000] if not quick else [10, 100]
    rounds = 3 if not quick else 2
    results = {}

    proc = start_server_process("server.py", port)
    if not proc:
        print("  [SKIP] Server failed to start")
        return {}

    # Set 0% errors via a quick client connection
    _set_config(port, "drop_rate", 0)
    _set_config(port, "corrupt_rate", 0)
    _set_config(port, "shuffle_chunks", False)

    try:
        for size_kb in sizes_kb:
            fpath = create_test_file(size_kb * 1024)
            timings = []

            for r in range(rounds):
                res = run_transfers(DEFAULT_HOST, port, [fpath])
                if res and res[0][0]:
                    timings.append(res[0][1])

            os.unlink(fpath)

            if timings:
                avg = sum(timings) / len(timings)
                throughput = size_kb / avg if avg > 0 else 0
                results[f"{size_kb}KB"] = {
                    "avg_ms": round(avg * 1000, 1),
                    "throughput_kbps": round(throughput, 0),
                    "rounds": len(timings),
                }
                print(f"  {size_kb:>6} KB | {avg*1000:>8.1f} ms | "
                      f"{throughput:>8.0f} KB/s")
            else:
                print(f"  {size_kb:>6} KB | FAILED")

    finally:
        stop_server(proc)

    return results


# ── Category B: Scalability ──────────────────────────────────────────────────

def bench_scalability(port, quick=False):
    """Measure throughput vs concurrent client count."""
    print("\n  Category B: Scalability (50KB files, 0% errors)")
    print("  " + "-" * 50)

    client_counts = [1, 2, 5, 10, 20] if not quick else [1, 5, 10]
    file_size = 50 * 1024
    results = {}

    proc = start_server_process("server.py", port)
    if not proc:
        print("  [SKIP] Server failed to start")
        return {}

    _set_config(port, "drop_rate", 0)
    _set_config(port, "corrupt_rate", 0)

    try:
        for n in client_counts:
            files = [create_test_file(file_size) for _ in range(n)]

            overall_start = time.time()
            res = run_transfers(DEFAULT_HOST, port, files)
            overall = time.time() - overall_start

            for f in files:
                os.unlink(f)

            stats = summarize(res)
            total_kb = sum(1 for ok, _ in res if ok) * 50
            agg_throughput = total_kb / overall if overall > 0 else 0

            results[f"{n}_clients"] = {
                **stats,
                "total_elapsed_ms": round(overall * 1000, 1),
                "aggregate_kbps": round(agg_throughput, 0),
            }
            print(f"  {n:>3} clients | avg {stats['avg_ms']:>7.1f} ms | "
                  f"agg {agg_throughput:>6.0f} KB/s | "
                  f"{stats['success_rate']:.0f}% ok")

    finally:
        stop_server(proc)

    return results


# ── Category C: Reliability ──────────────────────────────────────────────────

def bench_reliability(port, quick=False):
    """Measure success rate under various error conditions."""
    print("\n  Category C: Reliability (50KB, 5 clients)")
    print("  " + "-" * 50)

    scenarios = [
        {"name": "clean",     "drop": 0,   "corrupt": 0,    "dup": 0},
        {"name": "5% drop",   "drop": 0.05, "corrupt": 0,   "dup": 0},
        {"name": "10% drop",  "drop": 0.10, "corrupt": 0,   "dup": 0},
        {"name": "20% drop",  "drop": 0.20, "corrupt": 0,   "dup": 0},
        {"name": "10% corrupt", "drop": 0, "corrupt": 0.10, "dup": 0},
        {"name": "mixed",     "drop": 0.10, "corrupt": 0.05, "dup": 0.05},
        {"name": "heavy",     "drop": 0.20, "corrupt": 0.10, "dup": 0.05},
    ]
    if quick:
        scenarios = scenarios[:4]

    n_clients = 5
    file_size = 50 * 1024
    results = {}

    proc = start_server_process("server.py", port)
    if not proc:
        print("  [SKIP] Server failed to start")
        return {}

    try:
        for sc in scenarios:
            _set_config(port, "drop_rate", sc["drop"])
            _set_config(port, "corrupt_rate", sc["corrupt"])
            _set_config(port, "duplicate_rate", sc["dup"])
            time.sleep(0.2)

            files = [create_test_file(file_size) for _ in range(n_clients)]
            res = run_transfers(DEFAULT_HOST, port, files)
            for f in files:
                os.unlink(f)

            stats = summarize(res)
            results[sc["name"]] = {**stats, "params": sc}
            print(f"  {sc['name']:>15} | {stats['success_rate']:>5.0f}% ok | "
                  f"avg {stats['avg_ms']:>7.1f} ms | "
                  f"{stats['failures']} failures")

    finally:
        stop_server(proc)

    return results


# ── Category D: Backend Comparison ───────────────────────────────────────────

def bench_backends(port_base, quick=False):
    """Compare threaded, async, hybrid across file sizes."""
    print("\n  Category D: Backend Comparison")
    print("  " + "-" * 50)

    backends = [
        ("Threaded", "server.py",        port_base),
        ("Async",    "server_async.py",  port_base + 1),
        ("Hybrid",   "server_hybrid.py", port_base + 2),
    ]

    sizes_kb = [10, 100, 1000] if not quick else [10, 100]
    n_clients = 5
    rounds = 3 if not quick else 2
    results = {}

    for name, script, port in backends:
        proc = start_server_process(script, port)
        if not proc:
            print(f"  [{name}] FAILED to start")
            results[name] = None
            continue

        _set_config(port, "drop_rate", 0.05)
        _set_config(port, "corrupt_rate", 0.02)

        backend_results = {}

        for size_kb in sizes_kb:
            round_times = []

            for r in range(rounds):
                files = [create_test_file(size_kb * 1024) for _ in range(n_clients)]
                res = run_transfers(DEFAULT_HOST, port, files)
                for f in files:
                    os.unlink(f)

                times = [e for ok, e in res if ok]
                if times:
                    round_times.append(sum(times) / len(times))

                time.sleep(0.3)

            if round_times:
                avg = sum(round_times) / len(round_times)
                throughput = (n_clients * size_kb) / avg if avg > 0 else 0
                backend_results[f"{size_kb}KB"] = {
                    "avg_ms": round(avg * 1000, 1),
                    "throughput_kbps": round(throughput, 0),
                }
                print(f"  {name:>8} | {size_kb:>5}KB | "
                      f"{avg*1000:>7.1f} ms | {throughput:>7.0f} KB/s")

        results[name] = backend_results
        stop_server(proc)

    return results


# ── Config helper ────────────────────────────────────────────────────────────

def _set_config(port, key, value):
    """Push a config change to a running server."""
    import json as _json
    from protocol import (MSG_QUERY_REQUEST, MSG_QUERY_RESPONSE,
                          send_message, recv_message)

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        sock.connect((DEFAULT_HOST, port))
        query = {"type": "config_set", "key": key, "value": value}
        send_message(sock, MSG_QUERY_REQUEST,
                     payload=_json.dumps(query).encode())
        recv_message(sock)
        sock.close()
    except Exception:
        pass  # best-effort


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="File Transfer Benchmark Suite")
    parser.add_argument("--quick", action="store_true",
                        help="Run a fast subset (~30s)")
    parser.add_argument("--category", choices=["A", "B", "C", "D"],
                        help="Run a single category")
    args = parser.parse_args()

    port_base = 18500
    all_results = {}

    print("\n" + "=" * 60)
    print("  FILE TRANSFER BENCHMARK SUITE")
    print("=" * 60)

    categories = {
        "A": ("throughput",   lambda: bench_throughput(port_base, args.quick)),
        "B": ("scalability",  lambda: bench_scalability(port_base + 3, args.quick)),
        "C": ("reliability",  lambda: bench_reliability(port_base + 4, args.quick)),
        "D": ("backends",     lambda: bench_backends(port_base + 5, args.quick)),
    }

    to_run = {args.category: categories[args.category]} if args.category else categories

    for key, (name, func) in to_run.items():
        try:
            all_results[name] = func()
        except Exception as e:
            print(f"  [ERROR] Category {key}: {e}")
            all_results[name] = {"error": str(e)}

    # Save results
    try:
        with open(RESULTS_FILE, "w") as f:
            json.dump(all_results, f, indent=2)
        print(f"\n  Results saved to {RESULTS_FILE}")
    except OSError as e:
        print(f"\n  Could not save: {e}")

    # Summary
    print("\n" + "=" * 60)
    print("  SUMMARY")
    print("=" * 60)

    if "throughput" in all_results:
        t = all_results["throughput"]
        if t:
            best = max(t.items(), key=lambda x: x[1].get("throughput_kbps", 0))
            print(f"  Peak throughput: {best[1]['throughput_kbps']:.0f} KB/s "
                  f"({best[0]} file)")

    if "scalability" in all_results:
        s = all_results["scalability"]
        if s:
            max_clients = max(s.keys(), key=lambda k: int(k.split("_")[0]))
            print(f"  Max tested: {max_clients} — "
                  f"{s[max_clients]['aggregate_kbps']:.0f} KB/s aggregate")

    if "reliability" in all_results:
        r = all_results["reliability"]
        if r:
            all_pass = all(v.get("success_rate", 0) == 100
                          for v in r.values() if isinstance(v, dict))
            print(f"  Reliability: {'ALL PASS' if all_pass else 'SOME FAILURES'}")

    if "backends" in all_results:
        b = all_results["backends"]
        valid = {k: v for k, v in b.items() if v}
        if valid:
            print(f"  Backends tested: {', '.join(valid.keys())}")

    print()


if __name__ == "__main__":
    main()
