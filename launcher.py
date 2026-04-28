#!/usr/bin/env python3
"""
One-command launcher for the file transfer system.

Starts the hybrid server (asyncio + threadpool) in a background thread
and drops into the client TUI. No separate terminal needed.

Usage:
    python launcher.py [port]
    python launcher.py [port] --threaded   (use threaded backend instead)
"""

import sys
import os
import time
import threading
import asyncio

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from protocol import DEFAULT_HOST, DEFAULT_PORT
from client import run_tui, CLIENT_CONFIG


def _run_hybrid_server(host, port):
    """Run the hybrid async server in a new event loop (for threading)."""
    from server_hybrid import HybridServer
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    srv = HybridServer(host=host, port=port)
    loop.run_until_complete(srv.run())


def _run_threaded_server(host, port):
    """Run the threaded server."""
    from server import start_server, ServerState
    state = ServerState()
    start_server(host, port, state=state)


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_PORT
    use_threaded = "--threaded" in sys.argv

    backend_name = "threaded" if use_threaded else "hybrid (asyncio + threadpool)"
    target = _run_threaded_server if use_threaded else _run_hybrid_server

    # Start server in a daemon thread
    server_thread = threading.Thread(
        target=target,
        args=(DEFAULT_HOST, port),
        name="ServerMain",
        daemon=True,
    )
    server_thread.start()

    # Give the server a moment to bind
    time.sleep(0.5)

    # Point the client at our server
    CLIENT_CONFIG["host"] = DEFAULT_HOST
    CLIENT_CONFIG["port"] = port

    print(f"  Server backend: {backend_name}\n")

    # Run the interactive TUI (blocks until quit)
    try:
        run_tui()
    except KeyboardInterrupt:
        print("\nShutting down...")


if __name__ == "__main__":
    main()
