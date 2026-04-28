"""
Benchmark comparing threaded, async, and hybrid server backends.

Runs configurable transfer tests against each backend and reports results.

Usage:
    python benchmark.py [--clients N] [--file-size KB] [--rounds N]
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
import signal
import hashlib
import asyncio
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from protocol import DEFAULT_HOST, CHUNK_SIZE
from client import transfer_file

# ── Helpers ──────────────────────────────────────────────────────────────────

def create_test_file(size_bytes):
    """Create a temp file with random binary data."""
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".bench")
    tmp.write(os.urandom(size_bytes))
    tmp.close()
    return tmp.name


def wait_for_server(host, port, timeout=5):
    """Block until the server is accepting connections."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except (socket.error, OSError):
            time.sleep(0.1)
    return False


def run_benchmark_round(host, port, test_files, concurrent_clients):
    """
    Run one benchmark round: transfer all test files concurrently.
    
    Returns list of (success, elapsed_seconds) tuples.
    """
    results = [None] * len(test_files)

    def worker(idx, fpath):
        start = time.time()
        try:
            ok = transfer_file(fpath, host=host, port=port)
            results[idx] = (ok, time.time() - start)
        except Exception as e:
            results[idx] = (False, time.time() - start)

    threads = [
        threading.Thread(target=worker, args=(i, f))
        for i, f in enumerate(test_files)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    return [r for r in results if r is not None]


# ── Server launchers ─────────────────────────────────────────────────────────

def start_threaded_server(port):
    """Start threaded server in a subprocess."""
    proc = subprocess.Popen(
        [sys.executable, "server.py", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return proc


def start_async_server(port):
    """Start async server in a subprocess."""
    proc = subprocess.Popen(
        [sys.executable, "server_async.py", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return proc


def start_hybrid_server(port):
    """Start hybrid server in a subprocess."""
    proc = subprocess.Popen(
        [sys.executable, "server_hybrid.py", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return proc


# ── Main benchmark ───────────────────────────────────────────────────────────

def run_benchmarks(clients=5, file_size_kb=50, rounds=3):
    """Run benchmarks for all three backends and return results."""
    host = DEFAULT_HOST
    base_port = 18000
    file_size = file_size_kb * 1024

    backends = [
        ("Threaded",  start_threaded_server, base_port),
        ("Async",     start_async_server,    base_port + 1),
        ("Hybrid",    start_hybrid_server,   base_port + 2),
    ]

    # Create test files
    test_files = [create_test_file(file_size) for _ in range(clients)]

    all_results = {}

    try:
        for name, launcher, port in backends:
            print(f"\n{'='*60}")
            print(f"  Backend: {name} | {clients} clients | "
                  f"{file_size_kb} KB | {rounds} rounds")
            print(f"{'='*60}")

            proc = launcher(port)

            try:
                if not wait_for_server(host, port, timeout=5):
                    print(f"  [SKIP] {name} server failed to start")
                    all_results[name] = None
                    continue

                round_times = []

                for r in range(rounds):
                    print(f"  Round {r + 1}/{rounds}...", end=" ", flush=True)
                    results = run_benchmark_round(
                        host, port, test_files, clients
                    )
                    successes = sum(1 for ok, _ in results if ok)
                    times = [t for ok, t in results if ok]

                    if times:
                        avg = sum(times) / len(times)
                        round_times.append(avg)
                        total_kb = successes * file_size_kb
                        throughput = total_kb / max(max(times), 0.001)
                        print(f"{successes}/{clients} ok | "
                              f"avg={avg:.3f}s | "
                              f"throughput={throughput:.0f} KB/s")
                    else:
                        print(f"FAILED")

                    time.sleep(0.5)  # cooldown

                if round_times:
                    all_results[name] = {
                        "avg_time": sum(round_times) / len(round_times),
                        "min_time": min(round_times),
                        "max_time": max(round_times),
                        "rounds": len(round_times),
                        "clients": clients,
                        "file_size_kb": file_size_kb,
                    }
                else:
                    all_results[name] = None

            finally:
                proc.terminate()
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    proc.kill()

                time.sleep(0.5)  # let port release

    finally:
        for f in test_files:
            try:
                os.unlink(f)
            except OSError:
                pass

    return all_results


def print_summary(results):
    """Print a comparison table of benchmark results."""
    print(f"\n{'='*60}")
    print(f"  BENCHMARK SUMMARY")
    print(f"{'='*60}")
    print(f"  {'Backend':<12} {'Avg (s)':>10} {'Min (s)':>10} "
          f"{'Max (s)':>10} {'Status':>10}")
    print(f"  {'-'*52}")

    for name, data in results.items():
        if data:
            print(f"  {name:<12} {data['avg_time']:>10.3f} "
                  f"{data['min_time']:>10.3f} "
                  f"{data['max_time']:>10.3f} {'OK':>10}")
        else:
            print(f"  {name:<12} {'—':>10} {'—':>10} {'—':>10} "
                  f"{'FAILED':>10}")

    # Determine winner
    valid = {k: v for k, v in results.items() if v}
    if valid:
        winner = min(valid, key=lambda k: valid[k]["avg_time"])
        print(f"\n  Fastest: {winner}")

    print()

    # Save raw results
    out_path = "benchmark_results.json"
    try:
        with open(out_path, "w") as f:
            json.dump(results, f, indent=2)
        print(f"  Raw results saved to {out_path}")
    except OSError as e:
        print(f"  Could not save results: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="File Transfer Benchmark")
    parser.add_argument("--clients", type=int, default=5,
                        help="Number of concurrent clients (default: 5)")
    parser.add_argument("--file-size", type=int, default=50,
                        help="File size in KB (default: 50)")
    parser.add_argument("--rounds", type=int, default=3,
                        help="Benchmark rounds per backend (default: 3)")
    args = parser.parse_args()

    results = run_benchmarks(
        clients=args.clients,
        file_size_kb=args.file_size,
        rounds=args.rounds,
    )
    print_summary(results)
