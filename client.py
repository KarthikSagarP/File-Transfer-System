"""
File transfer client with interactive TUI and session management.

Commands:
    help                             - Show available commands
    upload <path>                    - Upload a file to the server
    download <file>                  - Download a file from the server (TCP)
    ls                               - List files on the server
    lc                               - List files in local directory
    quit                             - Exit the application
    history                          - Show command history for this session
    IndexGet longlist                - Detailed server file listing
    IndexGet longlist *.ext word     - Filtered listing by type and keyword
    IndexGet shortlist <start> <end> - Files between timestamps
    FileHash checkall                - Hash all server files, compare with local
    FileHash verify <file>           - Hash single file, compare with local
    Cache show                       - Show session cache contents
    Cache verify <file>              - Check/download file to session cache
    status                           - Show server status and cache stats
    benchmark [n] [size]             - Run transfer benchmarks
    stresstest [c] [kb] [d%] [c%]    - Concurrent stress test
    clear                            - Clear the screen
"""

import socket
import os
import sys
import json
import time
import threading
import hashlib
import logging
import shutil
from collections import defaultdict

from protocol import (
    DEFAULT_HOST, DEFAULT_PORT,
    MSG_UPLOAD_REQUEST, MSG_FILE_META, MSG_CHUNK, MSG_ACK,
    MSG_RETRANSMIT_REQ, MSG_TRANSFER_DONE, MSG_ERROR,
    MSG_LIST_REQUEST, MSG_LIST_RESPONSE,
    MSG_DOWNLOAD_REQUEST,
    MSG_STATUS_REQUEST, MSG_STATUS_RESPONSE,
    MSG_QUERY_REQUEST, MSG_QUERY_RESPONSE,
    send_message, recv_message, recv_exactly,
    compute_checksum_bytes, unpack_chunk_with_crc,
)

# ── Suppress lower-level logs in TUI mode ────────────────────────────────────

logging.basicConfig(level=logging.WARNING)
log = logging.getLogger(__name__)

# ── Rich imports with fallback ───────────────────────────────────────────────

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.text import Text
    from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn
    from rich import box
    HAS_RICH = True
except ImportError:
    HAS_RICH = False


# ── Console helpers ──────────────────────────────────────────────────────────

if HAS_RICH:
    console = Console()

    def print_success(msg):
        console.print(f"[bold green]✓[/] {msg}")

    def print_error(msg):
        console.print(f"[bold red]✗[/] {msg}")

    def print_info(msg):
        console.print(f"[dim cyan]ℹ[/] {msg}")

    def print_warn(msg):
        console.print(f"[bold yellow]⚠[/] {msg}")

    def clear_screen():
        console.clear()
else:
    def print_success(msg): print(f"[OK] {msg}")
    def print_error(msg):   print(f"[ERROR] {msg}")
    def print_info(msg):    print(f"[INFO] {msg}")
    def print_warn(msg):    print(f"[WARN] {msg}")
    def clear_screen():     os.system("cls" if os.name == "nt" else "clear")


# ── Client configuration ────────────────────────────────────────────────────

CLIENT_CONFIG = {
    "host": DEFAULT_HOST,
    "port": DEFAULT_PORT,
    "output_dir": "received_files",
    "socket_timeout": 30,
    "max_retries": 10,
}


# ── Session management ──────────────────────────────────────────────────────

class ClientCache:
    """
    Session-specific file cache with LFU (Least Frequently Used) eviction.
    
    Stores up to max_size recently downloaded files in a cache folder.
    When full, the file with the fewest access requests is evicted.
    """

    def __init__(self, cache_dir="CacheFolder", max_size=3):
        self.cache_dir = cache_dir
        self.max_size = max_size
        self._access_count = defaultdict(int)  # filename -> request count
        os.makedirs(cache_dir, exist_ok=True)

    def has(self, filename):
        """Check if a file is in the cache."""
        return os.path.isfile(os.path.join(self.cache_dir, filename))

    def get_path(self, filename):
        """Get the cache path for a file."""
        return os.path.join(self.cache_dir, filename)

    def add(self, filename, data):
        """Add a file to the cache, evicting LFU entry if full."""
        # Evict if at capacity
        cached_files = self._list_cached()
        while len(cached_files) >= self.max_size:
            # Find least frequently used
            lfu_file = min(cached_files, key=lambda f: self._access_count.get(f, 0))
            evict_path = os.path.join(self.cache_dir, lfu_file)
            try:
                os.unlink(evict_path)
                print_info(f"Cache evicted: {lfu_file} "
                           f"({self._access_count.get(lfu_file, 0)} accesses)")
            except OSError:
                pass
            self._access_count.pop(lfu_file, None)
            cached_files.remove(lfu_file)

        # Write the file
        fpath = os.path.join(self.cache_dir, filename)
        with open(fpath, "wb") as f:
            f.write(data)
        self._access_count[filename] = 1

    def access(self, filename):
        """Record an access to a cached file."""
        self._access_count[filename] += 1

    def show(self):
        """Return list of cached files with sizes."""
        files = []
        for fname in self._list_cached():
            fpath = os.path.join(self.cache_dir, fname)
            try:
                size = os.path.getsize(fpath)
                files.append({
                    "filename": fname,
                    "size": size,
                    "accesses": self._access_count.get(fname, 0),
                })
            except OSError:
                pass
        return files

    def _list_cached(self):
        """List filenames currently in cache."""
        try:
            return [f for f in os.listdir(self.cache_dir)
                    if os.path.isfile(os.path.join(self.cache_dir, f))]
        except OSError:
            return []

    def clear(self):
        """Clear the cache folder."""
        for fname in self._list_cached():
            try:
                os.unlink(os.path.join(self.cache_dir, fname))
            except OSError:
                pass
        self._access_count.clear()


class Session:
    """
    Client session with username, command history, and per-session cache.
    """

    def __init__(self, username):
        self.username = username
        self.session_id = f"{username}_{int(time.time())}"
        self.start_time = time.time()
        self.history = []   # list of command strings
        self.cache = ClientCache(
            cache_dir=os.path.join("CacheFolder", self.session_id),
            max_size=3,
        )

    def record(self, command):
        """Record a command in session history."""
        self.history.append({
            "cmd": command,
            "time": time.strftime("%H:%M:%S"),
        })

    def get_history(self):
        """Return command history."""
        return self.history

    def elapsed(self):
        """Return session duration in seconds."""
        return time.time() - self.start_time


# ── Connection manager ──────────────────────────────────────────────────────

class Connection:
    """Manages a persistent TCP connection to the server."""

    def __init__(self):
        self._sock = None

    def connect(self, host=None, port=None):
        """Establish connection to the server."""
        host = host or CLIENT_CONFIG["host"]
        port = port or CLIENT_CONFIG["port"]

        self.disconnect()

        try:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._sock.settimeout(CLIENT_CONFIG["socket_timeout"])
            self._sock.connect((host, port))
            print_success(f"Connected to {host}:{port}")
            return True
        except (socket.error, OSError) as e:
            print_error(f"Connection failed: {e}")
            self._sock = None
            return False

    def disconnect(self):
        """Close the connection if open."""
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None

    @property
    def is_connected(self):
        """Check if the socket exists AND the connection is still alive."""
        if self._sock is None:
            return False
        # Peek at the socket to detect server-side close
        try:
            self._sock.setblocking(False)
            try:
                data = self._sock.recv(1, socket.MSG_PEEK)
                if data == b"":
                    # Server closed the connection
                    self.disconnect()
                    return False
            except BlockingIOError:
                # No data available — connection is alive
                pass
            except (ConnectionError, OSError):
                self.disconnect()
                return False
            finally:
                self._sock.setblocking(True)
                if self._sock:
                    self._sock.settimeout(CLIENT_CONFIG["socket_timeout"])
            return True
        except Exception:
            self.disconnect()
            return False

    @property
    def sock(self):
        if not self._sock:
            raise ConnectionError("Not connected to server")
        return self._sock

    def ensure_connected(self):
        """Reconnect if needed. Returns True if connected."""
        if self.is_connected:
            return True
        return self.connect()


# ── Transfer logic ───────────────────────────────────────────────────────────

def _receive_file_transfer(sock, show_progress=True):
    """
    Receive FILE_META + CHUNKs + TRANSFER_DONE, handle retransmission.
    
    Each chunk is verified via CRC32 on arrival. Chunks that fail CRC
    are treated the same as missing chunks — they are added to the
    retransmission request. This catches both drops AND corruption.
    
    Returns (success: bool, filename: str, elapsed_seconds: float).
    """
    start = time.time()

    # ── Receive FILE_META ────────────────────────────────────────────────
    msg_type, _, payload = recv_message(sock)
    if msg_type == MSG_ERROR:
        print_error(f"Server error: {payload.decode('utf-8', errors='replace')}")
        return False, "", 0

    if msg_type != MSG_FILE_META:
        print_error(f"Expected FILE_META, got 0x{msg_type:02X}")
        return False, "", 0

    try:
        meta = json.loads(payload.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        print_error(f"Bad metadata: {e}")
        return False, "", 0

    expected_checksum = meta["checksum"]
    total_chunks = meta["total_chunks"]
    filename = meta["filename"]
    print_info(f"Expecting {total_chunks} chunks | "
               f"Checksum: {expected_checksum[:16]}...")

    # ── Receive chunks with CRC verification ─────────────────────────────
    received_chunks = {}   # seq_num -> verified raw data
    corrupted_seqs = set() # chunks that arrived but failed CRC
    max_retries = CLIENT_CONFIG["max_retries"]

    # Set up progress bar if rich is available and progress is wanted
    progress = None
    task_id = None
    if show_progress and HAS_RICH and total_chunks > 5:
        progress = Progress(
            TextColumn("[bold cyan]{task.description}"),
            BarColumn(bar_width=40),
            TextColumn("[green]{task.completed}/{task.total}"),
            TextColumn("[dim]({task.fields[status]})"),
            TimeElapsedColumn(),
            console=console,
        )
        task_id = progress.add_task(
            f"Receiving {filename}", total=total_chunks, status="waiting..."
        )
        progress.start()

    try:
        for retry in range(max_retries + 1):
            chunks_this_round = 0
            corrupt_this_round = 0

            while True:
                try:
                    msg_type, seq_num, payload = recv_message(sock)
                except ConnectionError as e:
                    print_error(f"Lost connection: {e}")
                    return False, filename, time.time() - start

                if msg_type == MSG_CHUNK:
                    # Verify per-chunk CRC32 integrity
                    data, crc_valid = unpack_chunk_with_crc(payload)

                    if crc_valid:
                        received_chunks[seq_num] = data
                        corrupted_seqs.discard(seq_num)  # fixed on retransmit
                        chunks_this_round += 1
                        # Update progress bar
                        if progress and task_id is not None:
                            progress.update(
                                task_id,
                                completed=len(received_chunks),
                                status=f"{len(corrupted_seqs)} corrupted"
                                       if corrupted_seqs else "OK"
                            )
                    else:
                        # Chunk arrived but data is corrupted — request retransmit
                        corrupted_seqs.add(seq_num)
                        corrupt_this_round += 1
                        if progress and task_id is not None:
                            progress.update(
                                task_id,
                                status=f"{len(corrupted_seqs)} corrupted"
                            )

                elif msg_type == MSG_TRANSFER_DONE:
                    break
                elif msg_type == MSG_ERROR:
                    print_error(f"Server error: {payload.decode('utf-8', errors='replace')}")
                    return False, filename, time.time() - start

            # ── Check completeness ───────────────────────────────────────────
            expected_set = set(range(total_chunks))
            received_set = set(received_chunks.keys())
            missing_seqs = expected_set - received_set
            need_retransmit = sorted(missing_seqs | corrupted_seqs)

            if not progress and show_progress:
                print_info(f"Received {len(received_set)}/{total_chunks} valid chunks" +
                           (f" | {corrupt_this_round} corrupted" if corrupt_this_round else ""))

            if not need_retransmit:
                if progress and task_id is not None:
                    progress.update(task_id, completed=total_chunks, status="complete")
                break  # all chunks received and verified

            # Update progress bar for retransmission
            if progress and task_id is not None:
                progress.update(
                    task_id,
                    status=f"retransmit {len(need_retransmit)} chunks..."
                )

            if show_progress and not progress:
                print_warn(f"Requesting retransmission of {len(need_retransmit)} chunk(s): "
                           f"{len(missing_seqs)} missing, {len(corrupted_seqs)} corrupted "
                           f"(attempt {retry + 1}/{max_retries})")

            retransmit_payload = json.dumps(need_retransmit).encode("utf-8")
            send_message(sock, MSG_RETRANSMIT_REQ, payload=retransmit_payload)

        else:
            print_error(f"Max retries ({max_retries}) exceeded — transfer failed.")
            print_error(f"  Still missing: {len(missing_seqs)} chunks")
            print_error(f"  Still corrupted: {len(corrupted_seqs)} chunks")
            return False, filename, time.time() - start

    finally:
        if progress:
            progress.stop()

    # ── Reassemble and verify full-file SHA-256 ──────────────────────────
    print_info("All chunks received and CRC-verified. Reassembling...")

    reassembled = b"".join(
        received_chunks[seq] for seq in range(total_chunks)
    )
    actual_checksum = compute_checksum_bytes(reassembled)
    elapsed = time.time() - start

    if actual_checksum != expected_checksum:
        print_error(f"SHA-256 checksum mismatch!")
        print_error(f"  Expected: {expected_checksum}")
        print_error(f"  Actual:   {actual_checksum}")
        send_message(sock, MSG_ERROR, payload=b"Checksum mismatch")
        return False, filename, elapsed

    # ── Save file ────────────────────────────────────────────────────────
    out_dir = CLIENT_CONFIG["output_dir"]
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, filename)
    try:
        with open(out_path, "wb") as f:
            f.write(reassembled)
    except OSError as e:
        print_error(f"Failed to save file: {e}")
        send_message(sock, MSG_ERROR, payload=str(e).encode())
        return False, filename, elapsed

    send_message(sock, MSG_ACK)

    size_kb = len(reassembled) / 1024
    speed = size_kb / elapsed if elapsed > 0 else 0
    print_success(f"Transfer Successful — '{filename}' "
                  f"({size_kb:.1f} KB in {elapsed:.2f}s, {speed:.0f} KB/s)")
    print_info(f"Saved to {out_path}")
    return True, filename, elapsed


# ── Command implementations ─────────────────────────────────────────────────

def cmd_help():
    """Show available commands."""
    commands = [
        ("", "── File Operations ──"),
        ("upload <path>",             "Upload a file to the server"),
        ("download <filename>",       "Download a file from the server"),
        ("ls",                        "List files on the server"),
        ("lc",                        "List files in local directory"),
        ("", "── File Indexing ──"),
        ("IndexGet longlist",         "Detailed server file listing"),
        ("IndexGet longlist *.ext word", "Filter by type and keyword"),
        ("IndexGet shortlist <start> <end>", "Files between timestamps (YYYY-MM-DD_HH:MM:SS)"),
        ("IndexGet shortlist <s> <e> *.ext", "Date range + type filter"),
        ("", "── Integrity ──"),
        ("FileHash checkall",         "Hash all server files, compare with local"),
        ("FileHash verify <file>",    "Hash a single file, compare with local"),
        ("", "── Session Cache ──"),
        ("Cache show",                "Show files in session cache"),
        ("Cache verify <file>",       "Check/download file to session cache"),
        ("", "── Session ──"),
        ("history",                   "Show command history for this session"),
        ("status",                    "Show server status and cache stats"),
        ("config [key value]",        "View or change server config (drop_rate, corrupt_rate, etc.)"),
        ("", "── Testing ──"),
        ("benchmark [n] [size]",      "Run n transfer benchmarks"),
        ("stresstest [c] [kb] [d%] [c%]", "Concurrent stress test"),
        ("", "── General ──"),
        ("clear",                     "Clear the screen"),
        ("help",                      "Show this help message"),
        ("quit",                      "Exit the application"),
    ]

    if HAS_RICH:
        table = Table(title="Available Commands", box=box.ROUNDED,
                      title_style="bold cyan")
        table.add_column("Command", style="bold green", min_width=34)
        table.add_column("Description", style="white")
        for cmd, desc in commands:
            if cmd == "":
                table.add_row(f"[bold yellow]{desc}[/]", "")
            else:
                table.add_row(cmd, desc)
        console.print(table)
    else:
        print("\n  Available Commands:")
        print("  " + "-" * 60)
        for cmd, desc in commands:
            if cmd == "":
                print(f"\n  {desc}")
            else:
                print(f"  {cmd:<38} {desc}")
        print()


def cmd_upload(conn, filepath):
    """Upload a file to the server."""
    filepath = os.path.abspath(filepath)
    if not os.path.isfile(filepath):
        print_error(f"File not found: {filepath}")
        return False

    if not conn.ensure_connected():
        return False

    filename = os.path.basename(filepath)
    file_size = os.path.getsize(filepath)
    print_info(f"Uploading '{filename}' ({file_size / 1024:.1f} KB)...")

    try:
        # Send upload request
        request_payload = json.dumps({
            "filename": filename,
            "file_size": file_size,
        }).encode("utf-8")
        send_message(conn.sock, MSG_UPLOAD_REQUEST, payload=request_payload)

        # Wait for ACK
        msg_type, _, _ = recv_message(conn.sock)
        if msg_type != MSG_ACK:
            print_error("Server did not acknowledge upload request")
            return False

        # Stream file bytes
        with open(filepath, "rb") as f:
            while True:
                block = f.read(8192)
                if not block:
                    break
                conn.sock.sendall(block)

        print_info("Upload complete, receiving verified copy...")

        # Receive the file back with verification
        success, _, elapsed = _receive_file_transfer(conn.sock)
        return success

    except ConnectionError as e:
        print_error(f"Connection error: {e}")
        conn.disconnect()
        return False
    except Exception as e:
        print_error(f"Upload failed: {e}")
        return False


def cmd_download(conn, filename):
    """Download a file from the server."""
    if not conn.ensure_connected():
        return False

    print_info(f"Requesting download: '{filename}'...")

    try:
        send_message(conn.sock, MSG_DOWNLOAD_REQUEST,
                     payload=filename.encode("utf-8"))

        success, _, elapsed = _receive_file_transfer(conn.sock)
        return success

    except ConnectionError as e:
        print_error(f"Connection error: {e}")
        conn.disconnect()
        return False
    except Exception as e:
        print_error(f"Download failed: {e}")
        return False


def cmd_list(conn):
    """List files on the server."""
    if not conn.ensure_connected():
        return

    try:
        send_message(conn.sock, MSG_LIST_REQUEST)
        msg_type, _, payload = recv_message(conn.sock)

        if msg_type == MSG_ERROR:
            print_error(payload.decode("utf-8", errors="replace"))
            return

        if msg_type != MSG_LIST_RESPONSE:
            print_error(f"Unexpected response: 0x{msg_type:02X}")
            return

        files = json.loads(payload.decode("utf-8"))

        if not files:
            print_info("No files stored on server.")
            return

        if HAS_RICH:
            table = Table(title="Server Files", box=box.ROUNDED,
                          title_style="bold cyan")
            table.add_column("Filename", style="bold white")
            table.add_column("Size", style="green", justify="right")
            table.add_column("Modified", style="dim")
            for f in files:
                size = f"{f['size'] / 1024:.1f} KB"
                modified = time.strftime("%Y-%m-%d %H:%M",
                                        time.localtime(f["modified"]))
                table.add_row(f["filename"], size, modified)
            console.print(table)
        else:
            print(f"\n  {'Filename':<30} {'Size':>10}  Modified")
            print("  " + "-" * 60)
            for f in files:
                size = f"{f['size'] / 1024:.1f} KB"
                modified = time.strftime("%Y-%m-%d %H:%M",
                                        time.localtime(f["modified"]))
                print(f"  {f['filename']:<30} {size:>10}  {modified}")
            print()

    except ConnectionError as e:
        print_error(f"Connection error: {e}")
        conn.disconnect()
    except Exception as e:
        print_error(f"List failed: {e}")


def cmd_status(conn):
    """Show server status."""
    if not conn.ensure_connected():
        return

    try:
        send_message(conn.sock, MSG_STATUS_REQUEST)
        msg_type, _, payload = recv_message(conn.sock)

        if msg_type != MSG_STATUS_RESPONSE:
            print_error(f"Unexpected response: 0x{msg_type:02X}")
            return

        status = json.loads(payload.decode("utf-8"))

        if HAS_RICH:
            # Server info
            info = Table(title="Server Status", box=box.ROUNDED,
                         title_style="bold cyan", show_header=False)
            info.add_column("Key", style="bold")
            info.add_column("Value", style="green")
            info.add_row("Uptime", f"{status['uptime_seconds']:.0f}s")
            info.add_row("Active clients", str(status["active_clients"]))
            info.add_row("Total transfers", str(status["total_transfers"]))
            total_kb = status["total_bytes_transferred"] / 1024
            info.add_row("Total data", f"{total_kb:.1f} KB")
            console.print(info)

            # Cache info
            cache = status["cache"]
            cache_table = Table(title="Cache Stats", box=box.ROUNDED,
                                title_style="bold cyan", show_header=False)
            cache_table.add_column("Key", style="bold")
            cache_table.add_column("Value", style="green")
            cache_table.add_row("Entries",
                                f"{cache['entries']}/{cache['max_entries']}")
            cache_table.add_row("Hit rate", f"{cache['hit_rate_pct']}%")
            cache_table.add_row("Hits / Misses",
                                f"{cache['hits']} / {cache['misses']}")
            cache_table.add_row("Evictions", str(cache["evictions"]))
            console.print(cache_table)

            # Config
            cfg = status["config"]
            cfg_table = Table(title="Server Config", box=box.ROUNDED,
                              title_style="bold cyan", show_header=False)
            cfg_table.add_column("Key", style="bold")
            cfg_table.add_column("Value", style="yellow")
            for k, v in cfg.items():
                cfg_table.add_row(k, str(v))
            console.print(cfg_table)
        else:
            print(json.dumps(status, indent=2))

    except ConnectionError as e:
        print_error(f"Connection error: {e}")
        conn.disconnect()
    except Exception as e:
        print_error(f"Status failed: {e}")


def cmd_lc(args):
    """List files in the local/client directory."""
    target_dir = args.strip() if args else "."
    try:
        files = []
        for fname in os.listdir(target_dir):
            fpath = os.path.join(target_dir, fname)
            if os.path.isfile(fpath):
                stat = os.stat(fpath)
                files.append((fname, stat.st_size, stat.st_mtime))

        if not files:
            print_info(f"No files in '{target_dir}'")
            return

        if HAS_RICH:
            table = Table(title=f"Local Files ({target_dir})", box=box.ROUNDED,
                          title_style="bold cyan")
            table.add_column("Filename", style="bold white")
            table.add_column("Size", style="green", justify="right")
            table.add_column("Modified", style="dim")
            for name, size, mtime in sorted(files):
                size_str = f"{size / 1024:.1f} KB" if size >= 1024 else f"{size} B"
                mod_str = time.strftime("%Y-%m-%d %H:%M", time.localtime(mtime))
                table.add_row(name, size_str, mod_str)
            console.print(table)
        else:
            print(f"\n  {'Filename':<30} {'Size':>10}  Modified")
            print("  " + "-" * 60)
            for name, size, mtime in sorted(files):
                size_str = f"{size / 1024:.1f} KB"
                mod_str = time.strftime("%Y-%m-%d %H:%M", time.localtime(mtime))
                print(f"  {name:<30} {size_str:>10}  {mod_str}")
            print()

    except OSError as e:
        print_error(f"Cannot list directory: {e}")


def cmd_history(session):
    """Display command history for the current session."""
    history = session.get_history()
    if not history:
        print_info("No commands in history yet.")
        return

    if HAS_RICH:
        table = Table(title=f"Session History ({session.username})", box=box.ROUNDED,
                      title_style="bold cyan")
        table.add_column("#", style="dim", width=4)
        table.add_column("Time", style="dim cyan")
        table.add_column("Command", style="white")
        for i, entry in enumerate(history, 1):
            table.add_row(str(i), entry["time"], entry["cmd"])
        console.print(table)
    else:
        for i, entry in enumerate(history, 1):
            print(f"  {i:3d}  [{entry['time']}]  {entry['cmd']}")


def cmd_indexget(conn, args):
    """Handle IndexGet longlist and IndexGet shortlist commands."""
    if not conn.ensure_connected():
        return

    parts = args.split() if args else []
    if not parts:
        print_error("Usage: IndexGet longlist [*.ext] [keyword]")
        print_error("       IndexGet shortlist <start> <end> [*.ext]")
        return

    subcommand = parts[0].lower()

    try:
        if subcommand == "longlist":
            query = {"type": "index_longlist"}
            if len(parts) > 1:
                query["pattern"] = parts[1]
            if len(parts) > 2:
                query["keyword"] = parts[2]

        elif subcommand == "shortlist":
            if len(parts) < 3:
                print_error("Usage: IndexGet shortlist <YYYY-MM-DD_HH:MM:SS> <YYYY-MM-DD_HH:MM:SS> [*.ext]")
                return
            query = {
                "type": "index_shortlist",
                "start": parts[1],
                "end": parts[2],
            }
            if len(parts) > 3:
                query["pattern"] = parts[3]
        else:
            print_error(f"Unknown IndexGet subcommand: {subcommand}")
            return

        # Send query
        send_message(conn.sock, MSG_QUERY_REQUEST,
                     payload=json.dumps(query).encode())
        msg_type, _, payload = recv_message(conn.sock)

        if msg_type == MSG_ERROR:
            print_error(payload.decode("utf-8", errors="replace"))
            return
        if msg_type != MSG_QUERY_RESPONSE:
            print_error(f"Unexpected response: 0x{msg_type:02X}")
            return

        result = json.loads(payload.decode("utf-8"))

        if "error" in result:
            print_error(result["error"])
            return

        files = result.get("files", [])
        if not files:
            print_info("No files match the query.")
            return

        if HAS_RICH:
            table = Table(title=f"IndexGet {subcommand}", box=box.ROUNDED,
                          title_style="bold cyan")
            table.add_column("Filename", style="bold white")
            table.add_column("Size", style="green", justify="right")
            table.add_column("Type", style="yellow")
            table.add_column("Modified", style="dim")
            for f in files:
                size = f"{f['size'] / 1024:.1f} KB"
                table.add_row(f["filename"], size,
                              f.get("filetype", ""), f.get("modified_str", ""))
            console.print(table)
        else:
            print(f"\n  {'Filename':<25} {'Size':>8}  {'Type':<8}  Modified")
            print("  " + "-" * 65)
            for f in files:
                size = f"{f['size'] / 1024:.1f} KB"
                print(f"  {f['filename']:<25} {size:>8}  "
                      f"{f.get('filetype', ''):<8}  {f.get('modified_str', '')}")
            print()

    except ConnectionError as e:
        print_error(f"Connection error: {e}")
        conn.disconnect()
    except Exception as e:
        print_error(f"IndexGet failed: {e}")


def cmd_filehash(conn, args):
    """Handle FileHash checkall and FileHash verify commands."""
    if not conn.ensure_connected():
        return

    parts = args.split() if args else []
    if not parts:
        print_error("Usage: FileHash checkall")
        print_error("       FileHash verify <filename>")
        return

    subcommand = parts[0].lower()

    try:
        if subcommand == "checkall":
            query = {"type": "filehash_all"}
        elif subcommand == "verify":
            if len(parts) < 2:
                print_error("Usage: FileHash verify <filename>")
                return
            query = {"type": "filehash_verify", "filename": parts[1]}
        else:
            print_error(f"Unknown FileHash subcommand: {subcommand}")
            return

        send_message(conn.sock, MSG_QUERY_REQUEST,
                     payload=json.dumps(query).encode())
        msg_type, _, payload = recv_message(conn.sock)

        if msg_type == MSG_ERROR:
            print_error(payload.decode("utf-8", errors="replace"))
            return
        if msg_type != MSG_QUERY_RESPONSE:
            print_error(f"Unexpected response: 0x{msg_type:02X}")
            return

        result = json.loads(payload.decode("utf-8"))

        if "error" in result:
            print_error(result["error"])
            return

        files = result.get("files", [])
        if not files:
            print_info("No files found.")
            return

        # Compare with local files
        if HAS_RICH:
            table = Table(title=f"FileHash {subcommand}", box=box.ROUNDED,
                          title_style="bold cyan")
            table.add_column("Filename", style="bold white")
            table.add_column("Server MD5", style="cyan", max_width=18)
            table.add_column("Local MD5", style="cyan", max_width=18)
            table.add_column("Match", style="bold")
            table.add_column("Modified", style="dim")

            for f in files:
                server_md5 = f.get("md5", "N/A")
                local_md5 = _compute_local_md5(f["filename"])
                if local_md5 is None:
                    match = "[dim]no local file[/]"
                elif local_md5 == server_md5:
                    match = "[bold green]✓ MATCH[/]"
                else:
                    match = "[bold red]✗ MISMATCH[/]"
                table.add_row(f["filename"], server_md5[:16] + "...",
                              (local_md5[:16] + "...") if local_md5 else "—",
                              match, f.get("modified_str", ""))
            console.print(table)
        else:
            for f in files:
                server_md5 = f.get("md5", "N/A")
                local_md5 = _compute_local_md5(f["filename"])
                match = "MATCH" if local_md5 == server_md5 else "MISMATCH"
                if local_md5 is None:
                    match = "NO LOCAL FILE"
                print(f"  {f['filename']}: server={server_md5[:16]}... "
                      f"local={(local_md5[:16] + '...') if local_md5 else '—'} "
                      f"[{match}]")

    except ConnectionError as e:
        print_error(f"Connection error: {e}")
        conn.disconnect()
    except Exception as e:
        print_error(f"FileHash failed: {e}")


def _compute_local_md5(filename):
    """Compute MD5 of a local file, checking received_files and current dir."""
    for search_dir in [CLIENT_CONFIG["output_dir"], "."]:
        fpath = os.path.join(search_dir, filename)
        if os.path.isfile(fpath):
            md5 = hashlib.md5()
            try:
                with open(fpath, "rb") as f:
                    while True:
                        block = f.read(8192)
                        if not block:
                            break
                        md5.update(block)
                return md5.hexdigest()
            except OSError:
                pass
    return None


def cmd_cache(conn, session, args):
    """Handle Cache show and Cache verify commands."""
    parts = args.split() if args else []
    if not parts:
        print_error("Usage: Cache show")
        print_error("       Cache verify <filename>")
        return

    subcommand = parts[0].lower()

    if subcommand == "show":
        files = session.cache.show()
        if not files:
            print_info("Session cache is empty.")
            return

        if HAS_RICH:
            table = Table(title=f"Session Cache ({session.username})", box=box.ROUNDED,
                          title_style="bold cyan")
            table.add_column("Filename", style="bold white")
            table.add_column("Size", style="green", justify="right")
            table.add_column("Accesses", style="yellow", justify="right")
            for f in files:
                size = f"{f['size'] / 1024:.1f} KB"
                table.add_row(f["filename"], size, str(f["accesses"]))
            console.print(table)
        else:
            for f in files:
                print(f"  {f['filename']}: {f['size'] / 1024:.1f} KB "
                      f"({f['accesses']} accesses)")

    elif subcommand == "verify":
        if len(parts) < 2:
            print_error("Usage: Cache verify <filename>")
            return

        filename = parts[1]

        if session.cache.has(filename):
            # File is in cache — report it
            session.cache.access(filename)
            fpath = session.cache.get_path(filename)
            size = os.path.getsize(fpath)
            print_success(f"Cache HIT: '{filename}' ({size / 1024:.1f} KB)")
        else:
            # File not in cache — download it via TCP
            print_info(f"Cache MISS: '{filename}' — downloading via TCP...")
            if not conn.ensure_connected():
                return

            try:
                send_message(conn.sock, MSG_DOWNLOAD_REQUEST,
                             payload=filename.encode("utf-8"))
                success, _, elapsed = _receive_file_transfer(conn.sock)

                if success:
                    # Copy the downloaded file into session cache
                    received_path = os.path.join(CLIENT_CONFIG["output_dir"], filename)
                    if os.path.isfile(received_path):
                        with open(received_path, "rb") as f:
                            data = f.read()
                        session.cache.add(filename, data)
                        print_success(f"Cached: '{filename}' "
                                      f"({len(data) / 1024:.1f} KB)")
                else:
                    print_error(f"Download failed — cannot cache '{filename}'")

            except ConnectionError as e:
                print_error(f"Connection error: {e}")
                conn.disconnect()
            except Exception as e:
                print_error(f"Cache verify failed: {e}")
    else:
        print_error(f"Unknown Cache subcommand: {subcommand}")


def _set_server_config(conn, key, value):
    """Send a config change to the server. Returns True on success."""
    try:
        query = {"type": "config_set", "key": key, "value": value}
        send_message(conn.sock, MSG_QUERY_REQUEST,
                     payload=json.dumps(query).encode())
        msg_type, _, payload = recv_message(conn.sock)
        if msg_type == MSG_QUERY_RESPONSE:
            result = json.loads(payload.decode())
            if "error" in result:
                print_error(result["error"])
                return False
            return True
        elif msg_type == MSG_ERROR:
            print_error(payload.decode("utf-8", errors="replace"))
            return False
    except Exception as e:
        print_error(f"Config update failed: {e}")
    return False


def cmd_config(conn, args):
    """View or change server configuration remotely."""
    if not conn.ensure_connected():
        return

    parts = args.split() if args else []

    try:
        if not parts:
            # Show current config
            query = {"type": "config_get"}
            send_message(conn.sock, MSG_QUERY_REQUEST,
                         payload=json.dumps(query).encode())
            msg_type, _, payload = recv_message(conn.sock)

            if msg_type != MSG_QUERY_RESPONSE:
                print_error(f"Unexpected response: 0x{msg_type:02X}")
                return

            result = json.loads(payload.decode())
            config = result.get("config", {})

            if HAS_RICH:
                table = Table(title="Server Config", box=box.ROUNDED,
                              title_style="bold cyan", show_header=False)
                table.add_column("Key", style="bold")
                table.add_column("Value", style="yellow")
                for k, v in config.items():
                    table.add_row(k, str(v))
                console.print(table)
            else:
                for k, v in config.items():
                    print(f"  {k}: {v}")

        elif len(parts) == 2:
            key, value = parts
            if _set_server_config(conn, key, value):
                print_success(f"Server config updated: {key} = {value}")
        else:
            print_error("Usage: config              (view all)")
            print_error("       config <key> <value> (set a value)")

    except ConnectionError as e:
        print_error(f"Connection error: {e}")
        conn.disconnect()
    except Exception as e:
        print_error(f"Config failed: {e}")


def cmd_benchmark(conn, args):
    """Run transfer benchmarks."""
    parts = args.split() if args else []
    n = int(parts[0]) if len(parts) > 0 else 5
    size_kb = int(parts[1]) if len(parts) > 1 else 10

    if not conn.ensure_connected():
        return

    print_info(f"Running {n} transfer(s) of {size_kb} KB files...")

    # Create temp benchmark file
    import tempfile
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".bench",
                                      dir=".")
    try:
        tmp.write(os.urandom(size_kb * 1024))
        tmp.close()

        timings = []
        successes = 0

        for i in range(n):
            print_info(f"Run {i + 1}/{n}...")

            # Need a fresh connection for each transfer since
            # the server handles a session loop
            if not conn.ensure_connected():
                print_error("Cannot reconnect")
                break

            try:
                request_payload = json.dumps({
                    "filename": f"bench_{i}.dat",
                    "file_size": size_kb * 1024,
                }).encode("utf-8")
                send_message(conn.sock, MSG_UPLOAD_REQUEST,
                             payload=request_payload)

                msg_type, _, _ = recv_message(conn.sock)
                if msg_type != MSG_ACK:
                    print_error("Server NAK")
                    continue

                with open(tmp.name, "rb") as f:
                    data = f.read()
                conn.sock.sendall(data)

                success, _, elapsed = _receive_file_transfer(
                    conn.sock, show_progress=False)

                if success:
                    successes += 1
                    timings.append(elapsed)

            except Exception as e:
                print_error(f"Run {i + 1} failed: {e}")
                conn.disconnect()

        # Results
        if timings:
            avg = sum(timings) / len(timings)
            throughput = (size_kb / avg) if avg > 0 else 0

            if HAS_RICH:
                table = Table(title="Benchmark Results", box=box.ROUNDED,
                              title_style="bold cyan", show_header=False)
                table.add_column("Metric", style="bold")
                table.add_column("Value", style="green")
                table.add_row("File size", f"{size_kb} KB")
                table.add_row("Runs", f"{successes}/{n} succeeded")
                table.add_row("Avg time", f"{avg:.3f}s")
                table.add_row("Min time", f"{min(timings):.3f}s")
                table.add_row("Max time", f"{max(timings):.3f}s")
                table.add_row("Throughput", f"{throughput:.1f} KB/s")
                console.print(table)
            else:
                print(f"\n  Benchmark: {successes}/{n} ok, "
                      f"avg={avg:.3f}s, throughput={throughput:.1f} KB/s\n")
        else:
            print_error("All benchmark runs failed.")

    finally:
        try:
            os.unlink(tmp.name)
        except OSError:
            pass


def cmd_stresstest(conn, args):
    """
    Run a stress test: multiple concurrent clients with high error rates.
    
    Usage: /stresstest [clients] [size_kb] [drop%] [corrupt%]
    Defaults: 10 clients, 50 KB files, 20% drops, 10% corruption
    """
    parts = args.split() if args else []
    num_clients = int(parts[0]) if len(parts) > 0 else 10
    size_kb = int(parts[1]) if len(parts) > 1 else 50
    drop_pct = int(parts[2]) if len(parts) > 2 else 20
    corrupt_pct = int(parts[3]) if len(parts) > 3 else 10

    print_info(f"Stress test: {num_clients} concurrent clients, "
               f"{size_kb} KB files, {drop_pct}% drops, {corrupt_pct}% corruption")

    # Push drop/corrupt rates to the server
    if conn.ensure_connected():
        drop_rate = drop_pct / 100.0
        corrupt_rate = corrupt_pct / 100.0
        print_info("Configuring server error rates...")
        _set_server_config(conn, "drop_rate", drop_rate)
        _set_server_config(conn, "corrupt_rate", corrupt_rate)
        print_success(f"Server set to {drop_pct}% drops, {corrupt_pct}% corruption")

    # Create temp files — one per client
    import tempfile
    test_files = []
    for i in range(num_clients):
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=f"_stress{i}.dat")
        tmp.write(os.urandom(size_kb * 1024))
        tmp.close()
        test_files.append(tmp.name)

    # Run concurrent transfers
    results = {}  # filepath -> (success, elapsed)
    errors = {}   # filepath -> error message

    def stress_worker(fpath, idx):
        """Single stress test worker — connects, uploads, verifies."""
        start = time.time()
        host = CLIENT_CONFIG["host"]
        port = CLIENT_CONFIG["port"]

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(CLIENT_CONFIG["socket_timeout"])

        try:
            sock.connect((host, port))

            filename = os.path.basename(fpath)
            file_size = os.path.getsize(fpath)

            request_payload = json.dumps({
                "filename": filename,
                "file_size": file_size,
            }).encode("utf-8")
            send_message(sock, MSG_UPLOAD_REQUEST, payload=request_payload)

            msg_type, _, _ = recv_message(sock)
            if msg_type != MSG_ACK:
                errors[fpath] = "Server NAK"
                results[fpath] = (False, time.time() - start)
                return

            with open(fpath, "rb") as f:
                while True:
                    block = f.read(8192)
                    if not block:
                        break
                    sock.sendall(block)

            success, _, elapsed = _receive_file_transfer(sock, show_progress=False)
            results[fpath] = (success, elapsed)

        except Exception as e:
            errors[fpath] = str(e)
            results[fpath] = (False, time.time() - start)
        finally:
            try:
                sock.close()
            except Exception:
                pass

    # Launch all clients
    overall_start = time.time()

    if HAS_RICH:
        progress = Progress(
            TextColumn("[bold cyan]{task.description}"),
            BarColumn(bar_width=30),
            TextColumn("[green]{task.completed}/{task.total} done"),
            TimeElapsedColumn(),
            console=console,
        )
        task = progress.add_task("Stress test", total=num_clients)
        progress.start()

    threads = []
    for i, fpath in enumerate(test_files):
        t = threading.Thread(target=stress_worker, args=(fpath, i))
        threads.append(t)
        t.start()

    # Wait for completion with progress
    for t in threads:
        t.join(timeout=60)
        if HAS_RICH:
            progress.update(task, advance=1)

    if HAS_RICH:
        progress.stop()

    overall_elapsed = time.time() - overall_start

    # Compile results
    successes = sum(1 for ok, _ in results.values() if ok)
    failures = num_clients - successes
    timings = [e for ok, e in results.values() if ok]
    avg_time = sum(timings) / len(timings) if timings else 0
    total_data = successes * size_kb
    throughput = total_data / overall_elapsed if overall_elapsed > 0 else 0

    # Display results
    if HAS_RICH:
        table = Table(title="Stress Test Results", box=box.ROUNDED,
                      title_style="bold cyan", show_header=False)
        table.add_column("Metric", style="bold")
        table.add_column("Value")

        table.add_row("Concurrent clients", str(num_clients))
        table.add_row("File size", f"{size_kb} KB each")
        table.add_row("Error simulation", f"{drop_pct}% drops, {corrupt_pct}% corruption")
        table.add_row("", "")

        success_style = "bold green" if successes == num_clients else "bold yellow"
        table.add_row("Successful", f"[{success_style}]{successes}/{num_clients}[/]")
        if failures:
            table.add_row("Failed", f"[bold red]{failures}[/]")

        table.add_row("", "")
        if timings:
            table.add_row("Avg transfer time", f"{avg_time:.3f}s")
            table.add_row("Min time", f"{min(timings):.3f}s")
            table.add_row("Max time", f"{max(timings):.3f}s")
        table.add_row("Total elapsed", f"{overall_elapsed:.2f}s")
        table.add_row("Aggregate throughput", f"{throughput:.0f} KB/s")

        console.print(table)

        # Show individual errors
        if errors:
            err_table = Table(title="Errors", box=box.ROUNDED,
                              title_style="bold red", show_header=True)
            err_table.add_column("Client", style="bold")
            err_table.add_column("Error", style="red")
            for fpath, err in errors.items():
                err_table.add_row(os.path.basename(fpath), err)
            console.print(err_table)
    else:
        print(f"\n  Stress Test: {successes}/{num_clients} succeeded")
        print(f"  Total: {overall_elapsed:.2f}s | "
              f"Throughput: {throughput:.0f} KB/s")
        if timings:
            print(f"  Avg: {avg_time:.3f}s | "
                  f"Min: {min(timings):.3f}s | Max: {max(timings):.3f}s")
        if errors:
            for fpath, err in errors.items():
                print(f"  ERROR ({os.path.basename(fpath)}): {err}")
        print()

    # Verdict
    if successes == num_clients:
        print_success(f"All {num_clients} clients completed successfully under stress!")
    elif successes > 0:
        print_warn(f"{successes}/{num_clients} succeeded. "
                   f"Server handled partial load under extreme conditions.")
    else:
        print_error("All transfers failed under stress.")

    # Cleanup
    for f in test_files:
        try:
            os.unlink(f)
        except OSError:
            pass


# ── Banner and TUI loop ─────────────────────────────────────────────────────

BANNER = r"""
   _____ _ _        _____                     __
  |  ___(_) | ___  |_   _| __ __ _ _ __  ___ / _| ___ _ __
  | |_  | | |/ _ \   | || '__/ _` | '_ \/ __| |_ / _ \ '__|
  |  _| | | |  __/   | || | | (_| | | | \__ \  _|  __/ |
  |_|   |_|_|\___|   |_||_|  \__,_|_| |_|___/_|  \___|_|
"""


def show_banner():
    """Display the application banner."""
    if HAS_RICH:
        console.print(Panel(
            Text(BANNER, style="bold cyan") +
            Text("\n  Multi-Client File Transfer System", style="bold white") +
            Text("\n  Type /help to see available commands\n",
                 style="dim white"),
            border_style="cyan",
            box=box.DOUBLE,
        ))
    else:
        print(BANNER)
        print("  Multi-Client File Transfer System")
        print("  Type /help to see available commands\n")


def run_tui():
    """Main TUI event loop with session management."""
    show_banner()

    # ── Session setup ────────────────────────────────────────────────────
    if HAS_RICH:
        username = console.input("[bold cyan]Enter your name:[/] ").strip()
    else:
        username = input("Enter your name: ").strip()

    if not username:
        username = "user"

    session = Session(username)
    conn = Connection()

    if HAS_RICH:
        console.print(f"\n[bold green]Welcome, {username}![/] "
                      f"Session: [dim]{session.session_id}[/]")
    else:
        print(f"\nWelcome, {username}! Session: {session.session_id}")

    print_info("Type 'help' to see available commands.\n")

    # Auto-connect on start
    if conn.connect():
        print_info("Server bind complete.\n")

    prompt = f"[bold cyan]{username}@fts>[/] " if HAS_RICH else f"{username}@fts> "

    while True:
        try:
            if HAS_RICH:
                raw = console.input(prompt).strip()
            else:
                raw = input(f"{username}@fts> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not raw:
            continue

        # Record in session history
        session.record(raw)

        # Parse command — no leading / required
        # Handle multi-word commands like "IndexGet longlist"
        parts = raw.split()
        command = parts[0].lower()
        args = " ".join(parts[1:])

        # ── Dispatch ─────────────────────────────────────────────────────
        try:
            if command == "help":
                cmd_help()

            elif command == "upload":
                if not args:
                    print_error("Usage: upload <filepath>")
                else:
                    cmd_upload(conn, args)

            elif command in ("download", "filedownload"):
                if not args:
                    print_error("Usage: download <filename>")
                else:
                    cmd_download(conn, args)

            elif command == "ls":
                cmd_list(conn)

            elif command == "lc":
                cmd_lc(args)

            elif command == "history":
                cmd_history(session)

            elif command == "indexget":
                cmd_indexget(conn, args)

            elif command == "filehash":
                cmd_filehash(conn, args)

            elif command == "cache":
                cmd_cache(conn, session, args)

            elif command == "status":
                cmd_status(conn)

            elif command == "config":
                cmd_config(conn, args)

            elif command == "benchmark":
                cmd_benchmark(conn, args)

            elif command == "stresstest":
                cmd_stresstest(conn, args)

            elif command == "clear":
                clear_screen()

            elif command in ("quit", "exit", "q"):
                print_info(f"Session duration: {session.elapsed():.0f}s | "
                           f"Commands used: {len(session.history)}")
                print_info("Goodbye!")
                break

            else:
                print_error(f"Unknown command: {command}")
                print_info("Type 'help' for available commands.")

        except Exception as e:
            print_error(f"Command failed: {e}")

    # Cleanup
    conn.disconnect()
    session.cache.clear()


# ── Programmatic API (for tests and launcher) ────────────────────────────────

def transfer_file(filepath, host=DEFAULT_HOST, port=DEFAULT_PORT):
    """
    Upload a file and receive it back. Non-interactive API.
    
    Returns True on success, False on failure.
    """
    filepath = os.path.abspath(filepath)
    if not os.path.isfile(filepath):
        return False

    filename = os.path.basename(filepath)
    file_size = os.path.getsize(filepath)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(CLIENT_CONFIG["socket_timeout"])

    try:
        sock.connect((host, port))

        # Upload request
        request_payload = json.dumps({
            "filename": filename,
            "file_size": file_size,
        }).encode("utf-8")
        send_message(sock, MSG_UPLOAD_REQUEST, payload=request_payload)

        msg_type, _, _ = recv_message(sock)
        if msg_type != MSG_ACK:
            return False

        # Stream file
        with open(filepath, "rb") as f:
            while True:
                block = f.read(8192)
                if not block:
                    break
                sock.sendall(block)

        # Receive back
        success, _, _ = _receive_file_transfer(sock, show_progress=False)
        return success

    except Exception:
        return False
    finally:
        try:
            sock.close()
        except Exception:
            pass


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Non-interactive: just transfer a file
        success = transfer_file(sys.argv[1],
                                sys.argv[2] if len(sys.argv) > 2 else DEFAULT_HOST,
                                int(sys.argv[3]) if len(sys.argv) > 3 else DEFAULT_PORT)
        sys.exit(0 if success else 1)
    else:
        run_tui()
