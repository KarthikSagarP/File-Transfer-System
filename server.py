"""
Multi-client file transfer server (threaded).

Features:
    - Per-client threads for concurrent handling
    - LRU cache for repeated file transfers
    - File storage with list/download support
    - Configurable error simulation (drop rate, shuffle)
    - Robust error handling with try/except throughout
"""

import socket
import threading
import os
import sys
import json
import random
import time
import logging

from protocol import (
    DEFAULT_HOST, DEFAULT_PORT, CHUNK_SIZE,
    MSG_UPLOAD_REQUEST, MSG_FILE_META, MSG_CHUNK, MSG_ACK,
    MSG_RETRANSMIT_REQ, MSG_TRANSFER_DONE, MSG_ERROR,
    MSG_LIST_REQUEST, MSG_LIST_RESPONSE,
    MSG_DOWNLOAD_REQUEST,
    MSG_STATUS_REQUEST, MSG_STATUS_RESPONSE,
    MSG_QUERY_REQUEST, MSG_QUERY_RESPONSE,
    send_message, recv_message, recv_exactly,
    compute_checksum, compute_checksum_bytes, split_file, build_meta_payload,
    pack_chunk_with_crc, corrupt_data,
)
from cache import FileCache

# ── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SERVER %(threadName)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Server state (thread-safe) ──────────────────────────────────────────────

class ServerState:
    """Shared server state protected by locks."""

    def __init__(self, storage_dir="server_storage"):
        self.storage_dir = storage_dir
        os.makedirs(storage_dir, exist_ok=True)

        self.cache = FileCache(max_entries=64, ttl_seconds=300)
        self.config = {
            "drop_rate": 0.1,
            "corrupt_rate": 0.05,
            "duplicate_rate": 0.0,
            "latency_ms": 0,
            "shuffle_chunks": True,
            "chunk_size": CHUNK_SIZE,
            "socket_timeout": 30,
            "max_retransmit_rounds": 10,
        }

        self._lock = threading.Lock()
        self._active_clients = 0
        self._total_transfers = 0
        self._total_bytes = 0
        self._start_time = time.time()

    def client_connected(self):
        with self._lock:
            self._active_clients += 1

    def client_disconnected(self):
        with self._lock:
            self._active_clients -= 1

    def record_transfer(self, nbytes):
        with self._lock:
            self._total_transfers += 1
            self._total_bytes += nbytes

    def get_status(self):
        with self._lock:
            uptime = time.time() - self._start_time
            return {
                "uptime_seconds": round(uptime, 1),
                "active_clients": self._active_clients,
                "total_transfers": self._total_transfers,
                "total_bytes_transferred": self._total_bytes,
                "cache": self.cache.get_stats(),
                "config": self.config,
            }

    def list_files(self):
        """List all files in server storage."""
        files = []
        try:
            for fname in os.listdir(self.storage_dir):
                fpath = os.path.join(self.storage_dir, fname)
                if os.path.isfile(fpath):
                    stat = os.stat(fpath)
                    files.append({
                        "filename": fname,
                        "size": stat.st_size,
                        "modified": stat.st_mtime,
                    })
        except OSError as e:
            log.error(f"Error listing files: {e}")
        return sorted(files, key=lambda f: f["modified"], reverse=True)

    def store_file(self, filename, data):
        """Save uploaded file to server storage."""
        safe_name = os.path.basename(filename)
        fpath = os.path.join(self.storage_dir, safe_name)
        try:
            with open(fpath, "wb") as f:
                f.write(data)
            return fpath
        except OSError as e:
            log.error(f"Error storing file: {e}")
            raise

    def get_file_path(self, filename):
        """Get full path for a stored file, or None if not found."""
        safe_name = os.path.basename(filename)
        fpath = os.path.join(self.storage_dir, safe_name)
        return fpath if os.path.isfile(fpath) else None


# ── Client session handler ───────────────────────────────────────────────────

def handle_client(conn, addr, state):
    """
    Handle a single client session.
    
    The session stays open indefinitely until the client disconnects.
    Timeouts are only applied during data-intensive operations (file
    upload/download), not while waiting for the next command.
    """
    # No timeout on the session loop — client controls when to disconnect.
    # Timeouts are set per-operation inside _handle_upload etc.
    conn.settimeout(None)
    state.client_connected()
    log.info(f"Connected: {addr}")

    try:
        while True:
            try:
                msg_type, _, payload = recv_message(conn)
            except ConnectionError:
                break  # client disconnected
            except OSError:
                break  # socket error

            try:
                # Set a timeout for the duration of command processing
                conn.settimeout(state.config["socket_timeout"])

                if msg_type == MSG_UPLOAD_REQUEST:
                    _handle_upload(conn, payload, state)
                elif msg_type == MSG_DOWNLOAD_REQUEST:
                    _handle_download(conn, payload, state)
                elif msg_type == MSG_LIST_REQUEST:
                    _handle_list(conn, state)
                elif msg_type == MSG_STATUS_REQUEST:
                    _handle_status(conn, state)
                elif msg_type == MSG_QUERY_REQUEST:
                    _handle_query(conn, payload, state)
                else:
                    send_message(conn, MSG_ERROR,
                                 payload=f"Unknown command: {msg_type}".encode())
            except ConnectionError:
                break
            except Exception as e:
                log.error(f"Error processing command: {e}", exc_info=True)
                try:
                    send_message(conn, MSG_ERROR,
                                 payload=str(e).encode("utf-8"))
                except Exception:
                    break
            finally:
                # Reset to no timeout for the next command wait
                try:
                    conn.settimeout(None)
                except OSError:
                    break

    except Exception as e:
        log.error(f"Session error ({addr}): {e}", exc_info=True)
    finally:
        state.client_disconnected()
        try:
            conn.close()
        except Exception:
            pass
        log.info(f"Disconnected: {addr}")


# ── Command handlers ────────────────────────────────────────────────────────

def _handle_upload(conn, payload, state):
    """Receive a file upload, store it, then send it back with verification."""
    try:
        upload_meta = json.loads(payload.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        send_message(conn, MSG_ERROR, payload=f"Bad request: {e}".encode())
        return

    filename = os.path.basename(upload_meta.get("filename", "unknown"))
    file_size = upload_meta.get("file_size", 0)
    log.info(f"Upload: '{filename}' ({file_size} bytes)")

    # ACK ready to receive
    send_message(conn, MSG_ACK)

    # Receive raw file bytes
    try:
        file_data = recv_exactly(conn, file_size)
    except ConnectionError as e:
        log.error(f"Failed to receive file data: {e}")
        raise

    # Store the file
    try:
        stored_path = state.store_file(filename, file_data)
    except OSError as e:
        send_message(conn, MSG_ERROR,
                     payload=f"Storage error: {e}".encode())
        return

    log.info(f"Stored: {stored_path}")

    # Check cache for this file content
    content_hash = compute_checksum_bytes(file_data)
    cached = state.cache.get(content_hash)

    if cached:
        checksum = cached["checksum"]
        chunks = cached["chunks"]
        total_chunks = len(chunks)
        log.info(f"Cache hit — skipping re-split")
    else:
        checksum = content_hash  # same as SHA-256 of content
        chunks = split_file(stored_path, state.config["chunk_size"])
        total_chunks = len(chunks)
        state.cache.put(content_hash, checksum, chunks, filename)

    log.info(f"Checksum: {checksum[:16]}... | Chunks: {total_chunks}")

    # Send FILE_META
    meta_payload = build_meta_payload(filename, checksum, total_chunks)
    send_message(conn, MSG_FILE_META, payload=meta_payload)

    # Send chunks with error simulation
    _send_chunks(conn, chunks, state.config, is_initial=True)

    # Retransmission loop
    _retransmit_loop(conn, chunks, state.config)

    state.record_transfer(file_size)
    log.info(f"Upload+transfer complete: '{filename}'")


def _handle_download(conn, payload, state):
    """Send a stored file to the client."""
    try:
        filename = payload.decode("utf-8").strip()
    except UnicodeDecodeError:
        send_message(conn, MSG_ERROR, payload=b"Bad filename encoding")
        return

    fpath = state.get_file_path(filename)
    if fpath is None:
        send_message(conn, MSG_ERROR,
                     payload=f"File not found: {filename}".encode())
        return

    log.info(f"Download request: '{filename}'")

    # Try cache first
    try:
        checksum = compute_checksum(fpath)
    except OSError as e:
        send_message(conn, MSG_ERROR,
                     payload=f"Read error: {e}".encode())
        return

    cached = state.cache.get(checksum)
    if cached:
        chunks = cached["chunks"]
        log.info("Cache hit for download")
    else:
        chunks = split_file(fpath, state.config["chunk_size"])
        state.cache.put(checksum, checksum, chunks, filename)

    total_chunks = len(chunks)

    # Send meta, chunks, retransmit loop
    meta_payload = build_meta_payload(filename, checksum, total_chunks)
    send_message(conn, MSG_FILE_META, payload=meta_payload)
    _send_chunks(conn, chunks, state.config, is_initial=True)
    _retransmit_loop(conn, chunks, state.config)

    file_size = os.path.getsize(fpath)
    state.record_transfer(file_size)
    log.info(f"Download complete: '{filename}'")


def _handle_list(conn, state):
    """Send list of stored files to the client."""
    files = state.list_files()
    payload = json.dumps(files).encode("utf-8")
    send_message(conn, MSG_LIST_RESPONSE, payload=payload)


def _handle_status(conn, state):
    """Send server status to the client."""
    status = state.get_status()
    payload = json.dumps(status, indent=2).encode("utf-8")
    send_message(conn, MSG_STATUS_RESPONSE, payload=payload)


def _handle_query(conn, payload, state):
    """
    Handle generic query requests: IndexGet, FileHash.
    
    Payload is JSON with:
        {"type": "index_longlist", ...}
        {"type": "index_shortlist", "start": "...", "end": "...", ...}
        {"type": "filehash_all"}
        {"type": "filehash_verify", "filename": "..."}
    """
    try:
        query = json.loads(payload.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        send_message(conn, MSG_ERROR, payload=f"Bad query: {e}".encode())
        return

    qtype = query.get("type", "")

    try:
        if qtype == "index_longlist":
            result = _query_index_longlist(state, query)
        elif qtype == "index_shortlist":
            result = _query_index_shortlist(state, query)
        elif qtype == "filehash_all":
            result = _query_filehash_all(state)
        elif qtype == "filehash_verify":
            result = _query_filehash_verify(state, query)
        elif qtype == "config_get":
            result = {"config": state.config}
        elif qtype == "config_set":
            key = query.get("key", "")
            value = query.get("value")
            if key not in state.config:
                result = {"error": f"Unknown config key: {key}",
                          "config": state.config}
            else:
                # Type-coerce to match existing type
                current = state.config[key]
                if isinstance(current, bool):
                    state.config[key] = str(value).lower() in ("true", "1", "yes")
                elif isinstance(current, float):
                    state.config[key] = float(value)
                elif isinstance(current, int):
                    state.config[key] = int(value)
                else:
                    state.config[key] = value
                log.info(f"Config updated: {key} = {state.config[key]}")
                result = {"config": state.config}
        else:
            send_message(conn, MSG_ERROR,
                         payload=f"Unknown query type: {qtype}".encode())
            return

        resp = json.dumps(result).encode("utf-8")
        send_message(conn, MSG_QUERY_RESPONSE, payload=resp)

    except Exception as e:
        log.error(f"Query error: {e}", exc_info=True)
        send_message(conn, MSG_ERROR, payload=str(e).encode())


def _get_file_details(storage_dir):
    """Get detailed file info for all files in storage."""
    files = []
    try:
        for fname in os.listdir(storage_dir):
            fpath = os.path.join(storage_dir, fname)
            if os.path.isfile(fpath):
                stat = os.stat(fpath)
                ext = os.path.splitext(fname)[1] or "(none)"
                files.append({
                    "filename": fname,
                    "size": stat.st_size,
                    "modified": stat.st_mtime,
                    "modified_str": time.strftime(
                        "%Y-%m-%d_%H:%M:%S", time.localtime(stat.st_mtime)
                    ),
                    "filetype": ext,
                })
    except OSError as e:
        log.error(f"Error reading files: {e}")
    return sorted(files, key=lambda f: f["modified"], reverse=True)


def _query_index_longlist(state, query):
    """IndexGet longlist — optionally filtered by extension and keyword."""
    files = _get_file_details(state.storage_dir)

    # Filter by extension pattern (e.g., "*.txt")
    pattern = query.get("pattern")
    if pattern:
        import fnmatch
        files = [f for f in files if fnmatch.fnmatch(f["filename"], pattern)]

    # Filter by keyword in file content
    keyword = query.get("keyword")
    if keyword:
        matching = []
        for f in files:
            fpath = os.path.join(state.storage_dir, f["filename"])
            try:
                with open(fpath, "r", errors="ignore") as fh:
                    content = fh.read()
                if keyword.lower() in content.lower():
                    matching.append(f)
            except Exception:
                pass
        files = matching

    return {"files": files, "query": "index_longlist"}


def _query_index_shortlist(state, query):
    """IndexGet shortlist — files between two timestamps, optionally by type."""
    files = _get_file_details(state.storage_dir)

    start_str = query.get("start", "")
    end_str = query.get("end", "")

    try:
        start_ts = time.mktime(time.strptime(start_str, "%Y-%m-%d_%H:%M:%S"))
        end_ts = time.mktime(time.strptime(end_str, "%Y-%m-%d_%H:%M:%S"))
    except (ValueError, OverflowError) as e:
        return {"error": f"Bad timestamp format: {e}", "files": []}

    files = [f for f in files if start_ts <= f["modified"] <= end_ts]

    pattern = query.get("pattern")
    if pattern:
        import fnmatch
        files = [f for f in files if fnmatch.fnmatch(f["filename"], pattern)]

    return {"files": files, "query": "index_shortlist"}


def _query_filehash_all(state):
    """FileHash checkall — compute MD5 for all server files."""
    import hashlib
    files = _get_file_details(state.storage_dir)
    results = []
    for f in files:
        fpath = os.path.join(state.storage_dir, f["filename"])
        try:
            md5 = hashlib.md5()
            with open(fpath, "rb") as fh:
                while True:
                    block = fh.read(8192)
                    if not block:
                        break
                    md5.update(block)
            f["md5"] = md5.hexdigest()
        except OSError:
            f["md5"] = "error"
        results.append(f)
    return {"files": results, "query": "filehash_all"}


def _query_filehash_verify(state, query):
    """FileHash verify — compute MD5 for a specific file."""
    import hashlib
    filename = os.path.basename(query.get("filename", ""))
    fpath = os.path.join(state.storage_dir, filename)

    if not os.path.isfile(fpath):
        return {"error": f"File not found: {filename}", "files": []}

    stat = os.stat(fpath)
    md5 = hashlib.md5()
    try:
        with open(fpath, "rb") as fh:
            while True:
                block = fh.read(8192)
                if not block:
                    break
                md5.update(block)
    except OSError as e:
        return {"error": str(e), "files": []}

    return {
        "files": [{
            "filename": filename,
            "size": stat.st_size,
            "modified_str": time.strftime(
                "%Y-%m-%d_%H:%M:%S", time.localtime(stat.st_mtime)
            ),
            "md5": md5.hexdigest(),
        }],
        "query": "filehash_verify",
    }


# ── Chunk transmission ───────────────────────────────────────────────────────

def _send_chunks(conn, chunks, config, is_initial=True, seq_filter=None):
    """
    Send file chunks with per-chunk CRC32 integrity checks.
    
    Simulated network conditions (initial send only):
        - drop_rate:      Chunk never sent (packet loss)
        - corrupt_rate:   Bits flipped, CRC mismatch on client
        - duplicate_rate: Same chunk sent twice (duplicate packet)
        - latency_ms:     Delay between chunks (network latency)
        - shuffle_chunks: Randomized send order (out-of-order delivery)
    
    Retransmissions are always sent cleanly with no simulation.
    """
    to_send = chunks if seq_filter is None else [
        (s, d) for s, d in chunks if s in seq_filter
    ]

    if is_initial and config["shuffle_chunks"]:
        to_send = to_send.copy()
        random.shuffle(to_send)

    drop_rate = config["drop_rate"]
    corrupt_rate = config.get("corrupt_rate", 0.0)
    duplicate_rate = config.get("duplicate_rate", 0.0)
    latency_ms = config.get("latency_ms", 0)
    dropped = []
    corrupted = []
    duplicated = []

    for seq_num, data in to_send:
        # Simulate latency
        if is_initial and latency_ms > 0:
            jitter = random.uniform(0.5, 1.5)  # ±50% jitter
            time.sleep((latency_ms / 1000.0) * jitter)

        # Simulate packet drop
        if is_initial and random.random() < drop_rate:
            dropped.append(seq_num)
            continue

        # Simulate corruption
        if is_initial and random.random() < corrupt_rate:
            corrupted_data = corrupt_data(data, num_bits=2)
            payload = corrupted_data + pack_chunk_with_crc(data)[-4:]
            corrupted.append(seq_num)
        else:
            payload = pack_chunk_with_crc(data)

        send_message(conn, MSG_CHUNK, seq_num=seq_num, payload=payload)

        # Simulate duplicate packet
        if is_initial and random.random() < duplicate_rate:
            send_message(conn, MSG_CHUNK, seq_num=seq_num, payload=payload)
            duplicated.append(seq_num)

    sent = len(to_send) - len(dropped)
    parts = [f"Sent {sent}/{len(to_send)} chunks"]
    if dropped:
        parts.append(f"dropped {len(dropped)}")
    if corrupted:
        parts.append(f"corrupted {len(corrupted)}")
    if duplicated:
        parts.append(f"duplicated {len(duplicated)}")
    log.info(" | ".join(parts))


def _retransmit_loop(conn, chunks, config):
    """Send TRANSFER_DONE and handle retransmission requests."""
    max_rounds = config["max_retransmit_rounds"]

    for round_num in range(max_rounds):
        send_message(conn, MSG_TRANSFER_DONE)

        try:
            msg_type, _, payload = recv_message(conn)
        except ConnectionError:
            log.warning("Client disconnected during retransmit")
            return

        if msg_type == MSG_ACK:
            log.info("Client ACK — transfer verified.")
            return

        if msg_type == MSG_RETRANSMIT_REQ:
            try:
                missing = json.loads(payload.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                log.error("Bad retransmit request payload")
                return

            log.info(f"Retransmit round {round_num + 1}: "
                     f"{len(missing)} chunks: {missing}")
            _send_chunks(conn, chunks, config, is_initial=False,
                         seq_filter=set(missing))
            continue

        if msg_type == MSG_ERROR:
            log.warning(f"Client error: {payload.decode('utf-8', errors='replace')}")
            return

        log.warning(f"Unexpected message: {msg_type}")
        return

    log.error("Max retransmission rounds exceeded")
    try:
        send_message(conn, MSG_ERROR, payload=b"Max retransmissions exceeded")
    except Exception:
        pass


# ── Server entry point ───────────────────────────────────────────────────────

def start_server(host=DEFAULT_HOST, port=DEFAULT_PORT, state=None):
    """Start the multi-client file transfer server."""
    if state is None:
        state = ServerState()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        server.bind((host, port))
        server.listen(5)
        log.info(f"Listening on {host}:{port}")
        log.info(f"Storage: {os.path.abspath(state.storage_dir)}")
        log.info(f"Config: drop={state.config['drop_rate']}, "
                 f"shuffle={state.config['shuffle_chunks']}")

        while True:
            try:
                conn, addr = server.accept()
                t = threading.Thread(
                    target=handle_client,
                    args=(conn, addr, state),
                    name=f"Client-{addr[1]}",
                    daemon=True,
                )
                t.start()
            except OSError:
                break  # server socket closed

    except KeyboardInterrupt:
        log.info("Shutting down.")
    except OSError as e:
        log.error(f"Server error: {e}")
    finally:
        try:
            server.close()
        except Exception:
            pass


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_PORT
    start_server(port=port)
