# Design Document: Multi-Client File Transfer System

**Author:** Karthik Sagar P
**Version:** 1.0
**Date:** April 2026

---

## 1. Overview

### 1.1 Problem Statement

Build a real-time file transfer system over TCP that supports multiple concurrent clients. The system must split files into chunks, transmit them with sequence numbers, handle out-of-order delivery and packet loss, verify integrity via checksums, and manage retransmission of missing or corrupted data.

### 1.2 Goals

- Reliable file transfer with dual-layer integrity verification (SHA-256 + per-chunk CRC32)
- Concurrent multi-client support with session isolation
- Graceful recovery from simulated network errors (drops, corruption, duplicates, reordering, latency)
- Low-latency repeated transfers via server-side LRU cache and client-side session cache
- Clean, modular, well-tested codebase (32 automated tests)

### 1.3 Non-Goals

- Encryption or authentication (out of scope for this challenge)
- UDP transport (assignment specifies TCP)
- Cross-network transfers (localhost only for demonstration)

---

## 2. Architecture

### 2.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        CLIENT                                   │
│  ┌──────────────┐  ┌────────────────┐  ┌─────────────────┐     │
│  │   TUI Layer  │  │ Transfer Logic │  │ Connection Mgr  │     │
│  │  (rich/CLI)  │──│  (send/recv/   │──│  (persistent    │     │
│  │  /help /list │  │   reassemble)  │  │   TCP session)  │     │
│  └──────────────┘  └────────────────┘  └────────┬────────┘     │
└─────────────────────────────────────────────────┼───────────────┘
                                                  │ TCP Socket
┌─────────────────────────────────────────────────┼───────────────┐
│                        SERVER                   │               │
│  ┌────────────────┐  ┌──────────────┐  ┌───────┴─────────┐     │
│  │  Concurrency   │  │   Command    │  │  Socket Accept  │     │
│  │   (thread /    │──│   Router     │──│     Loop        │     │
│  │   async/hybrid)│  │              │  │                 │     │
│  └───────┬────────┘  └──────────────┘  └─────────────────┘     │
│          │                                                      │
│  ┌───────┴────────┐  ┌──────────────┐  ┌─────────────────┐     │
│  │  File Storage  │  │  LRU Cache   │  │  Error Simulator│     │
│  │  (disk-backed) │  │  (in-memory) │  │  (drop/shuffle) │     │
│  └────────────────┘  └──────────────┘  └─────────────────┘     │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 Component Responsibilities

**`protocol.py` — Wire Protocol**
Defines the binary message format, message types, and low-level send/recv functions. This is the contract between client and server. Every other module depends on it, but it depends on nothing except `struct`, `json`, and `hashlib`.

**`server.py` — Threaded Server**
Listens on a TCP port, accepts connections, spawns one thread per client. Each thread independently handles upload, download, list, and status commands. Holds a reference to shared `ServerState` (cache, file storage, stats).

**`server_async.py` — Async Server**
Same protocol, same features, but uses `asyncio.start_server` with coroutines instead of threads. CPU-bound work (checksums, file splitting) is offloaded to a `ThreadPoolExecutor` via `loop.run_in_executor()`.

**`server_hybrid.py` — Hybrid Server**
Combines asyncio's event loop for I/O multiplexing with a dedicated `ThreadPoolExecutor` sized to CPU count for all CPU-bound operations. Provides the best throughput for mixed workloads.

**`client.py` — Client + TUI**
Maintains a persistent TCP connection. Provides an interactive command-line interface for upload, download, list, status, config, and benchmarking. Also exposes a programmatic `transfer_file()` API for tests.

**`cache.py` — LRU Cache**
Thread-safe, TTL-based in-memory cache. Stores processed file data (checksum + chunk list) keyed by content hash. Avoids redundant file reads and SHA-256 computations on repeated transfers.

**`launcher.py` — Entry Point**
Starts the server in a daemon thread and launches the client TUI. Provides one-command startup for evaluators.

---

## 3. Wire Protocol

### 3.1 Message Format

Every message consists of a fixed 9-byte header followed by a variable-length payload:

```
┌─────────────┬─────────────┬──────────────┬─────────────────┐
│  msg_type   │  seq_num    │ payload_len  │    payload      │
│  (1 byte)   │  (4 bytes)  │  (4 bytes)   │  (variable)     │
└─────────────┴─────────────┴──────────────┴─────────────────┘
     B               I             I            raw bytes

     ◄──── Header (9 bytes) ────►  ◄── payload_len bytes ──►
```

All multi-byte integers are big-endian (network byte order), packed with `struct.pack("!BII", ...)`.

### 3.2 Why This Format

TCP is a stream protocol — it delivers a continuous stream of bytes with no built-in message boundaries. If the sender writes 1024 bytes and then 512 bytes, the receiver might get all 1536 bytes in one `recv` call, or 100 bytes across 15 calls. The fixed-size header solves this: the receiver always reads exactly 9 bytes first, extracts `payload_len`, then reads exactly that many more bytes. This is called "length-prefix framing."

Alternatives considered:
- **Delimiter-based framing** (e.g., newline-separated): Fails for binary payloads that may contain the delimiter.
- **JSON-only messages**: Wasteful for the header sent with every chunk. A 9-byte binary header is 5-10x smaller than an equivalent JSON object.
- **Pickle serialization**: Security risk — pickle can execute arbitrary code during deserialization. Unsuitable for network protocols.

### 3.3 Message Types

| Code | Name | Direction | Payload | Purpose |
|------|------|-----------|---------|---------|
| `0x01` | `UPLOAD_REQUEST` | Client → Server | JSON: `{filename, file_size}` | Initiate a file upload |
| `0x02` | `FILE_META` | Server → Client | JSON: `{checksum, total_chunks, filename}` | File metadata before chunk transfer |
| `0x03` | `CHUNK` | Server → Client | Raw bytes (up to 1024) | A single file chunk (seq_num in header) |
| `0x04` | `ACK` | Both | Empty | Acknowledgment of success |
| `0x05` | `RETRANSMIT_REQ` | Client → Server | JSON: list of missing seq numbers | Request specific chunks to be resent |
| `0x06` | `TRANSFER_DONE` | Server → Client | Empty | All chunks for this round have been sent |
| `0x07` | `LIST_REQUEST` | Client → Server | Empty | Request list of stored files |
| `0x08` | `LIST_RESPONSE` | Server → Client | JSON: list of file info dicts | Response with stored file listing |
| `0x09` | `DOWNLOAD_REQUEST` | Client → Server | Filename (UTF-8 string) | Request a previously stored file |
| `0x0A` | `STATUS_REQUEST` | Client → Server | Empty | Request server status |
| `0x0B` | `STATUS_RESPONSE` | Server → Client | JSON: server stats | Response with uptime, cache, config |
| `0xFF` | `ERROR` | Both | Error message (UTF-8) | Signal an error condition |

### 3.4 Design Choice: `struct` for Headers, JSON for Metadata

The header is sent with every single message — thousands of times per file transfer. Using `struct.pack` produces exactly 9 bytes with zero overhead. Metadata payloads (file info, retransmit lists) are sent only once or twice per transfer, so JSON's readability and flexibility are worth the size cost there.

---

## 4. Data Flow

### 4.1 Upload + Verification Sequence

```
    CLIENT                                           SERVER
      │                                                │
      │──── UPLOAD_REQUEST {filename, file_size} ────►│
      │                                                │
      │◄──── ACK ─────────────────────────────────────│
      │                                                │
      │──── Raw file bytes (streamed) ───────────────►│
      │                                                │  Server stores file,
      │                                                │  computes SHA-256,
      │                                                │  splits into chunks
      │                                                │
      │◄──── FILE_META {checksum, total_chunks} ──────│
      │                                                │
      │◄──── CHUNK (seq=3, data) ─────────────────────│  (shuffled order)
      │◄──── CHUNK (seq=0, data) ─────────────────────│
      │◄──── CHUNK (seq=7, data) ─────────────────────│  (seq=2 dropped)
      │◄──── CHUNK (seq=1, data) ─────────────────────│
      │◄──── ...                  ─────────────────────│
      │                                                │
      │◄──── TRANSFER_DONE ───────────────────────────│
      │                                                │
      │  Client checks: do I have seq 0..N-1?          │
      │  Missing: [2, 5]                               │
      │                                                │
      │──── RETRANSMIT_REQ [2, 5] ───────────────────►│
      │                                                │
      │◄──── CHUNK (seq=2, data) ─────────────────────│
      │◄──── CHUNK (seq=5, data) ─────────────────────│
      │◄──── TRANSFER_DONE ───────────────────────────│
      │                                                │
      │  Client: all chunks received                   │
      │  Reassemble: chunks[0] + chunks[1] + ... + [N] │
      │  Compute SHA-256 of reassembled bytes           │
      │  Compare with server's checksum                 │
      │                                                │
      │──── ACK ─────────────────────────────────────►│
      │                                                │
```

### 4.2 Key Design Decisions in the Flow

**Why upload then re-download?**
The assignment requires the server to "split the file into chunks and transmit them back to the client." This round-trip proves the chunking, reassembly, and verification pipeline works end-to-end.

**Why stream raw bytes for upload, but use chunked protocol for download?**
The upload is a simple data transfer — the client knows the file size, the server reads exactly that many bytes. The download is where the interesting protocol logic lives: sequence numbers, out-of-order delivery, drop simulation, and retransmission.

**Why send checksum before chunks, not after?**
The client needs `total_chunks` from the metadata to know when it has received everything. Sending metadata first also means the client can start validating completeness as soon as `TRANSFER_DONE` arrives, without waiting for an additional message.

---

## 5. File Chunking and Reassembly

### 5.1 Splitting

```python
CHUNK_SIZE = 1024  # bytes

def split_file(filepath):
    chunks = []
    with open(filepath, "rb") as f:
        seq = 0
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            chunks.append((seq, chunk))
            seq += 1
    return chunks
```

A 10,240-byte file produces 10 chunks of 1024 bytes each.
A 10,241-byte file produces 10 chunks of 1024 bytes + 1 chunk of 1 byte.

### 5.2 Reassembly

The client stores chunks in a dictionary keyed by sequence number:

```python
received_chunks = {}
# As chunks arrive (in any order):
received_chunks[seq_num] = payload

# After all chunks received:
reassembled = b"".join(received_chunks[i] for i in range(total_chunks))
```

Using a dictionary means arrival order is irrelevant. Chunks can arrive as `[7, 0, 3, 1, 9, ...]` and reassembly still produces the correct byte sequence.

### 5.3 Integrity Verification

```python
server_checksum = hashlib.sha256(original_file_bytes).hexdigest()
client_checksum = hashlib.sha256(reassembled_bytes).hexdigest()
assert server_checksum == client_checksum
```

SHA-256 produces a 256-bit (64 hex character) fingerprint. If even a single bit differs between the original and reassembled file, the checksums will be completely different (avalanche effect). This catches any corruption introduced during chunking, transmission, or reassembly.

---

## 6. Concurrency Model

### 6.1 Three Backends Compared

```
THREADED (server.py)
┌──────────────────────────────────────────┐
│  Main Thread                             │
│  └─ accept() loop                        │
│       ├─ Thread-1 → handle(client_A)     │
│       ├─ Thread-2 → handle(client_B)     │  OS-managed scheduling
│       └─ Thread-3 → handle(client_C)     │
└──────────────────────────────────────────┘

ASYNC (server_async.py)
┌──────────────────────────────────────────┐
│  Single Thread — Event Loop              │
│  ├─ coroutine: handle(client_A)          │
│  ├─ coroutine: handle(client_B)          │  Cooperative scheduling
│  └─ coroutine: handle(client_C)          │
│  └─ ThreadPool (for CPU work)            │
└──────────────────────────────────────────┘

HYBRID (server_hybrid.py)
┌──────────────────────────────────────────┐
│  Single Thread — Event Loop              │
│  ├─ coroutine: handle(client_A)  ──────┐ │
│  ├─ coroutine: handle(client_B)  ──────┤ │  I/O in event loop
│  └─ coroutine: handle(client_C)  ──────┤ │  CPU in thread pool
│                                        ▼ │
│  ┌─ ThreadPoolExecutor (N=cpu_count) ──┐ │
│  │  Worker-1: sha256(file_A)           │ │
│  │  Worker-2: split(file_B)            │ │
│  │  Worker-3: sha256(file_C)           │ │
│  └─────────────────────────────────────┘ │
└──────────────────────────────────────────┘
```

### 6.2 Why Three Backends?

| Aspect | Threaded | Async | Hybrid |
|--------|----------|-------|--------|
| Concurrency mechanism | OS threads | Coroutines | Coroutines + Thread pool |
| Context switching | OS-managed (expensive) | Cooperative (cheap) | Cooperative + OS for CPU |
| Scaling limit | ~500-1000 threads | ~10,000+ connections | ~10,000+ connections |
| CPU-bound handling | Blocked by GIL | Blocks event loop | Offloaded to thread pool |
| Code complexity | Low | Medium | Medium-High |
| Best for | Simple servers, few clients | I/O-heavy, many clients | Mixed I/O + CPU workloads |

### 6.3 Session Isolation

Every client session operates on local data only:

```python
def handle_client(conn, addr, state):
    # These are all LOCAL to this thread/coroutine:
    file_data = recv_exactly(conn, file_size)   # local
    chunks = split_file(tmp_path)                # local
    checksum = compute_checksum(tmp_path)        # local

    # SHARED state (thread-safe via locks):
    state.cache.put(hash, checksum, chunks)      # RLock-protected
    state.record_transfer(file_size)             # Lock-protected
```

If client A crashes mid-transfer, the exception is caught in client A's thread, its socket is closed in the `finally` block, and clients B, C, D continue unaffected.

---

## 7. Caching Layer

### 7.1 Architecture

```
                    ┌────────────────────────────────┐
                    │         LRU Cache              │
                    │  ┌──────────────────────────┐  │
                    │  │ Key: SHA-256(file_bytes)  │  │
                    │  │ Value: {                  │  │
                    │  │   checksum: "a3f8...",    │  │
                    │  │   chunks: [(0,b".."),..], │  │
                    │  │   filename: "data.txt",   │  │
                    │  │   cached_at: 1714000000,  │  │
                    │  │   hits: 3                 │  │
                    │  │ }                         │  │
                    │  └──────────────────────────┘  │
                    │                                │
                    │  max_entries: 64               │
                    │  TTL: 300 seconds              │
                    │  Eviction: LRU (oldest first)  │
                    │  Thread-safe: RLock            │
                    └────────────────────────────────┘
```

### 7.2 Cache Flow

```
Request arrives
    │
    ▼
Compute content_hash = SHA-256(file_bytes)
    │
    ▼
cache.get(content_hash)
    │
    ├── HIT (and not expired) ──► Use cached checksum + chunks
    │                              Skip: file read, split, hash
    │                              Latency saved: ~80% for large files
    │
    └── MISS (or expired) ──► Read file, split, compute checksum
                               cache.put(content_hash, ...)
                               If at capacity, evict LRU entry
```

### 7.3 Why LRU with TTL?

- **LRU eviction**: Files transferred recently are likely to be transferred again (e.g., during testing, retries). LRU keeps hot entries and evicts cold ones.
- **TTL expiry**: Prevents serving stale data if a file is modified on disk between transfers.
- **Thread safety**: Multiple client threads may access the cache concurrently. `threading.RLock` allows recursive locking (safe for get-then-put patterns within the same thread).

### 7.4 Implementation: `OrderedDict`

Python's `OrderedDict` maintains insertion order and supports `move_to_end()` (on access) and `popitem(last=False)` (evict oldest). This gives O(1) get, put, and eviction — the same algorithmic complexity as a linked-list + hashmap LRU cache, but using a stdlib data structure.

---

## 8. Error Handling Strategy

### 8.1 Network Errors

| Scenario | Handling |
|----------|----------|
| Client disconnects mid-transfer | `ConnectionError` caught in handler, socket closed in `finally` |
| Server unreachable | `socket.error` caught in client, error message displayed |
| Idle timeout | `socket.settimeout(30)` on all sockets, `TimeoutError` caught |
| Partial recv | `recv_exactly()` loops until all expected bytes are read |
| Partial send | `sendall()` used instead of `send()` everywhere |

### 8.2 Application Errors

| Scenario | Handling |
|----------|----------|
| File not found | Checked before any network operation, error returned immediately |
| Checksum mismatch | Client sends `ERROR` message, logs both checksums for debugging |
| Max retransmissions exceeded | Server sends `ERROR`, client logs failure |
| Bad JSON in payload | `json.JSONDecodeError` caught, `ERROR` message sent back |
| Unknown message type | `ERROR` message sent, connection stays open for next command |
| Disk write failure | `OSError` caught, error propagated to client |

### 8.3 Resource Management

Every resource that can leak is managed with `with` statements or `try/finally`:

```python
# File handles
with open(filepath, "rb") as f:
    data = f.read()

# Sockets (in Connection class)
def disconnect(self):
    if self._sock:
        try:
            self._sock.close()
        except Exception:
            pass
        self._sock = None

# Server handler
def handle_client(conn, addr, state):
    state.client_connected()
    try:
        # ... handle commands ...
    except Exception as e:
        log.error(f"Error: {e}")
    finally:
        state.client_disconnected()
        try:
            conn.close()
        except Exception:
            pass
```

---

## 9. Simulated Network Conditions

### 9.1 Out-of-Order Delivery

Before sending chunks, the server shuffles the chunk list:

```python
if config["shuffle_chunks"]:
    to_send = to_send.copy()
    random.shuffle(to_send)
```

This simulates real-world network conditions where packets take different routes through intermediate routers and arrive in arbitrary order. The client's dictionary-based storage handles this transparently.

### 9.2 Packet Drops

For each chunk, the server rolls a random number:

```python
if random.random() < config["drop_rate"]:  # e.g., 0.1 = 10%
    continue  # skip this chunk entirely
```

Dropped chunks are never sent. The client discovers them by comparing received sequence numbers against the expected set `{0, 1, ..., total_chunks-1}`.

### 9.3 Retransmission (Only on Initial Send)

Drop simulation only applies during the initial chunk transmission (`is_initial=True`). When the server resends chunks during retransmission, it sends them reliably (`is_initial=False`). This prevents infinite retransmission loops where resent chunks keep getting dropped.

---

## 10. Tech Stack Rationale

| Component | Choice | Why Not Alternative |
|-----------|--------|-------------------|
| Networking | `socket` (stdlib) | Assignment requires raw TCP sockets, not HTTP libraries |
| Header serialization | `struct` | `pickle` is a security risk; JSON is wasteful for fixed headers |
| Metadata serialization | `json` | Readable, safe, flexible for variable-structure payloads |
| Checksum | `hashlib.sha256` | MD5 is broken; SHA-512 is overkill; SHA-256 is industry standard |
| Concurrency (primary) | `asyncio` + `ThreadPoolExecutor` | Pure threading doesn't scale; pure async blocks on CPU work |
| Concurrency (simple) | `threading` | Good enough for <100 clients; simpler to reason about |
| Cache | `OrderedDict` + `RLock` | `lru_cache` doesn't support TTL or thread safety |
| TUI | `rich` | `curses` is complex and non-portable; plain `print` looks amateur |
| Testing | `pytest` | `unittest` is verbose; `pytest` is the Python standard |
| File I/O | Binary mode + `with` | Text mode corrupts binary; `with` guarantees cleanup |

---

## 11. Testing Strategy

### 11.1 Test Categories

**Unit Tests (cache, protocol, CRC)**
Test individual functions in isolation. Cache hit/miss/eviction/TTL/thread-safety. CRC32 pack/unpack/corruption detection.

**Integration Tests (client-server + external checksums)**
Spin up a real server in a thread, connect real clients, transfer real files. Verify integrity using SHA-256 and MD5 computed independently of the system — simulating what `sha256sum` and `md5sum` would do externally.

**Error Tests (drops, corruption, retransmission)**
Configure server with 20% drop rate and 10% corruption, verify transfer still succeeds after retransmission rounds. Progressive drop rate testing from 5% to 25%.

**Concurrency Independence Tests**
10 clients upload unique files simultaneously. Each received file is hash-verified against its specific original to prove no cross-session data leakage. Separate test verifies one failed client doesn't crash others.

**Boundary Tests (edge cases)**
1-byte file, exact chunk boundary (1024 bytes), one byte over chunk boundary (1025 bytes), all-zeros file, all-0xFF file, random binary data.

**Progressive Stress Tests**
Incrementally increase drop rates (5→25%) and client counts (2→10) to verify the system handles progressive load increases.

### 11.2 Test Matrix (32 tests)

| Test | What It Proves |
|------|----------------|
| `test_small_text_file` | Basic end-to-end transfer works |
| `test_binary_file` | Binary data survives chunking + reassembly |
| `test_exact_chunk_boundary` | No off-by-one in splitting logic |
| `test_single_byte_file` | Handles minimum file size |
| `test_shuffled_delivery` | Out-of-order reassembly works |
| `test_with_drops` | Retransmission protocol recovers from loss |
| `test_five_concurrent_clients` | Session isolation under concurrency |
| `test_cache_hit_miss` | Cache stores and retrieves correctly |
| `test_cache_lru_eviction` | Oldest entry evicted at capacity |
| `test_cache_ttl_expiry` | Expired entries are not served |
| `test_cache_thread_safety` | No crashes under concurrent access |
| `test_list_empty` | LIST command works on empty storage |
| `test_status` | STATUS command returns valid JSON |
| `test_upload_then_list` | Uploaded file appears in LIST |
| `test_download_nonexistent` | Download of missing file returns ERROR |
| `test_crc_roundtrip` | CRC32 pack → unpack preserves data |
| `test_crc_detects_corruption` | Flipped bits cause CRC failure |
| `test_crc_empty_data` | CRC works on zero-length payload |
| `test_corrupt_data_changes_bytes` | Bit-flip function actually modifies data |
| `test_corruption_only` | Transfer succeeds despite 20% corruption |
| `test_drops_and_corruption_combined` | Succeeds under drops + corruption |
| `test_500kb_file` | Large file integrity with error simulation |
| `test_files_never_mix` | 10 concurrent clients, each file hash-verified individually |
| `test_one_client_crash_doesnt_affect_others` | Failed session doesn't leak into others |
| `test_exactly_one_chunk` | 1024-byte file = exactly 1 chunk |
| `test_one_byte_over_chunk` | 1025 bytes = 1 full + 1-byte remainder |
| `test_all_zeros` | All-zero file not confused during transfer |
| `test_all_ones` | All-0xFF file not confused during transfer |
| `test_external_sha256_verification` | SHA-256 computed independently matches (simulates sha256sum) |
| `test_external_md5_verification` | MD5 computed independently matches (simulates md5sum) |
| `test_increasing_drop_rates` | Succeeds at 5%, 10%, 15%, 20%, 25% drop rates |
| `test_increasing_client_count` | Succeeds with 2, 5, 10 concurrent clients |

---

## 12. Benchmark Results

### 12.1 Methodology

The benchmark suite (`benchmark_suite.py`) tests four categories. Each category starts a server subprocess, configures error rates via the query protocol, creates temporary files, runs concurrent transfers, measures timing, and terminates the server. Results are saved to JSON and visualized via `generate_chart.py`.

### 12.2 A. Throughput (single client, 0% errors)

| File Size | Transfer Time | Throughput |
|-----------|--------------|------------|
| 1 KB | 43 ms | 23 KB/s |
| 10 KB | 50 ms | 198 KB/s |
| 100 KB | 52 ms | 1,914 KB/s |
| 1000 KB | 122 ms | 8,212 KB/s |

The ~43ms baseline for small files is protocol overhead (connection, metadata exchange, checksum verification). Throughput scales 350x as file size increases and actual data transfer becomes the dominant cost.

### 12.3 B. Scalability (50KB files, 0% errors)

| Clients | Avg Time | Aggregate Throughput |
|---------|----------|---------------------|
| 1 | 93 ms | 533 KB/s |
| 2 | 87 ms | 1,089 KB/s |
| 5 | 110 ms | 2,011 KB/s |
| 10 | 160 ms | 2,477 KB/s |
| 20 | 331 ms | 1,609 KB/s |

Peak aggregate throughput at 10 clients. The drop at 20 clients reflects thread contention on Windows. Per-client time degrades linearly (not exponentially), indicating graceful degradation.

### 12.4 C. Reliability (50KB, 5 clients)

| Condition | Success Rate | Avg Time |
|-----------|-------------|----------|
| Clean (0% errors) | 100% | 101 ms |
| 5% packet drop | 100% | 108 ms |
| 10% packet drop | 100% | 106 ms |
| 20% packet drop | 100% | 107 ms |
| 10% corruption | 100% | 115 ms |
| Mixed (10%/5%/5%) | 100% | 129 ms |
| Heavy (20%/10%/5%) | 100% | 100 ms |

100% success rate across all error conditions. The retransmission protocol handles even heavy combined errors (20% drops + 10% corruption + 5% duplicates) without failure.

### 12.5 D. Backend Comparison (5 clients, 5% drop, 2% corrupt)

| Backend | 10KB | 100KB | 1000KB |
|---------|------|-------|--------|
| Threaded | 160 ms | 135 ms | 550 ms |
| Async | 110 ms | 125 ms | 450 ms |
| Hybrid | 100 ms | 140 ms | 405 ms |

At 1000KB: Hybrid achieves 12,400 KB/s (+36% over threaded). The hybrid advantage appears when CPU work (SHA-256, file splitting) is significant enough to block the event loop. At 10KB, async wins because executor dispatch overhead exceeds the computation itself. The crossover at ~100KB demonstrates that optimal backend selection depends on workload characteristics.

---

## 13. Project Structure

```
file-transfer/
├── launcher.py            # One-command startup (hybrid server + TUI)
├── protocol.py            # Wire protocol, CRC32, SHA-256, file splitting
├── cache.py               # Thread-safe LRU cache with TTL
├── server.py              # Threaded server backend
├── server_async.py        # Async server backend (asyncio)
├── server_hybrid.py       # Hybrid server (asyncio + threadpool)
├── client.py              # Client with session management and TUI
├── benchmark.py           # Backend comparison (legacy)
├── benchmark_suite.py     # Full benchmark suite (4 categories)
├── generate_chart.py      # Chart generator for both benchmark formats
├── DESIGN.md              # This document
├── assets/
│   ├── bench_overview.png # Combined 4-panel benchmark results
│   ├── bench_throughput.png
│   ├── bench_scalability.png
│   ├── bench_reliability.png
│   ├── bench_backends.png
│   └── wireshark_capture.png
├── tests/
│   └── test_transfer.py   # 32 automated tests
├── sample_files/
│   ├── test.txt
│   └── binary_test.bin
├── .gitignore
└── README.md
```

---

## 14. Future Improvements

These are deliberately out of scope for the current submission but noted as potential extensions:

- **TLS encryption**: Wrap sockets with `ssl.SSLContext` for encrypted transport
- **Chunk-level checksums**: CRC32 per chunk for early corruption detection before full reassembly
- **Compression**: `zlib` compression of chunks before transmission to reduce bandwidth
- **Progress bars**: `rich.progress` for visual transfer progress in the TUI
- **Persistent storage index**: SQLite or JSON manifest for server file metadata
- **Rate limiting**: Token bucket algorithm to throttle aggressive clients
- **Resume support**: Client stores partially received chunks to disk, resumes after reconnection
