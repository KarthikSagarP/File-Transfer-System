# Multi-Client Real-Time File Transfer System

A production-ready TCP socket-based file transfer system with chunked transmission, dual-layer integrity verification (SHA-256 + per-chunk CRC32), simulated network error recovery, LRU caching, and three concurrent server backends. Built for **Challenge 2: The Multi-Client Mayhem**.

---

## 1. Solution Overview

The system enables multiple clients to simultaneously upload files to a server over raw TCP sockets. The server splits files into 1024-byte chunks, tags each with a sequence number and CRC32 checksum, and transmits them back to the respective clients with simulated network errors (packet drops, corruption, duplication, reordering, latency). Clients detect missing and corrupted chunks via CRC32 verification, request retransmission, reassemble the file, and verify end-to-end integrity via SHA-256.

**Key capabilities:**

- Concurrent multi-client handling with complete session isolation
- Three server backends: Threaded, Async (asyncio), Hybrid (asyncio + ThreadPoolExecutor)
- Five configurable network error simulations with automatic retransmission recovery
- Interactive terminal (TUI) with 16 commands including file indexing, hash verification, and session caching
- Comprehensive benchmark suite (throughput, scalability, reliability, backend comparison)
- 32 automated tests covering functional, error, boundary, concurrency, stress, and integration scenarios
- Containerized deployment via Docker with CI/CD pipeline via GitHub Actions

---

## 2. Architecture

### 2.1 System Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                           CLIENT(S)                                      │
│  ┌──────────────┐  ┌────────────────┐  ┌──────────┐  ┌──────────────┐    │
│  │  Session Mgr │  │ Transfer Engine│  │  TUI     │  │ Session Cache│    │
│  │  (username,  │  │ (send/recv/    │  │ (16 cmds │  │ (3-slot LFU) │    │
│  │   history)   │  │  CRC, reassem) │  │  + help) │  │              │    │
│  └──────────────┘  └───────┬────────┘  └──────────┘  └──────────────┘    │
│                            │                                             │
└────────────────────────────┼─────────────────────────────────────────────┘
                             │ TCP Socket (persistent session)
┌────────────────────────────┼─────────────────────────────────────────────┐
│                    SERVER  │                                             │
│  ┌──────────────┐  ┌──────┴───────┐  ┌──────────────┐  ┌────────────┐    │
│  │  Concurrency │  │   Command    │  │  LRU Cache   │  │   Error    │    │
│  │  Engine      │  │   Router     │  │  (SHA-256    │  │   Simulator│    │
│  │  (thread/    │  │  (upload,    │  │   keyed,     │  │  (drop,    │    │
│  │   async/     │  │   download,  │  │   TTL+evict) │  │   corrupt, │    │
│  │   hybrid)    │  │   query...)  │  │              │  │   dup,lag) │    │
│  └──────────────┘  └──────────────┘  └──────────────┘  └────────────┘    │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐    │
│  │                    File Storage (disk)                           │    │
│  └──────────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Wire Protocol

Every message uses length-prefix framing with a 9-byte binary header:

```
┌──────────────┬──────────────┬────────────────┬─────────────────┐
│  msg_type    │  seq_num     │  payload_len   │    payload      │
│  (1 byte)    │  (4 bytes)   │  (4 bytes)     │  (variable)     │
└──────────────┴──────────────┴────────────────┴─────────────────┘
     B                I              I             raw bytes
     ◄──────── Header (9 bytes) ────────►
```

Packed with `struct.pack("!BII", ...)` (big-endian). 14 message types support upload, download, chunk transfer, retransmission, file listing, hash queries, config updates, and error reporting.

### 2.3 End-to-End Transfer Flow (Sample Scenario)

**Scenario:** Client A uploads `report.pdf` (10 KB) to the server with 10% packet drop and 5% corruption active.

```
    CLIENT A                                              SERVER
      │                                                     │
      │── 1. UPLOAD_REQUEST {filename, file_size} ─────────►│
      │                                                     │
      │◄── 2. ACK (ready to receive) ───────────────────────│
      │                                                     │
      │── 3. Raw file bytes (streamed in 8KB blocks) ──────►│
      │                                                     │  4. Server stores file to disk
      │                                                     │  5. Computes SHA-256 checksum
      │                                                     │  6. Splits into 10 chunks × 1024 bytes
      │                                                     │  7. Checks LRU cache (miss → store)
      │                                                     │
      │◄── 8. FILE_META {checksum, total_chunks=10} ────────│
      │                                                     │
      │◄── 9. CHUNK seq=7 [CRC32 appended] ─────────────────│  (shuffled order)
      │◄── CHUNK seq=2 [CRC32 appended] ────────────────────│
      │◄── CHUNK seq=0 [CRC32 appended] ────────────────────│
      │    ... (seq=4 DROPPED, seq=8 CORRUPTED) ...         │
      │◄── CHUNK seq=9 [CRC32 appended] ────────────────────│
      │                                                     │
      │◄── 10. TRANSFER_DONE ───────────────────────────────│
      │                                                     │
      │  11. Client checks: received {0,2,3,5,6,7,9}        │
      │      CRC fail on seq=8 → treat as missing           │
      │      Missing: [1, 4, 8]                             │
      │                                                     │
      │── 12. RETRANSMIT_REQ [1, 4, 8] ────────────────────►│
      │                                                     │
      │◄── 13. CHUNK seq=1 (clean, no drop sim) ────────────│
      │◄── CHUNK seq=4 ─────────────────────────────────────│
      │◄── CHUNK seq=8 ─────────────────────────────────────│
      │◄── TRANSFER_DONE ───────────────────────────────────│
      │                                                     │
      │  14. All 10 chunks received + CRC verified          │
      │  15. Reassemble: chunks[0]+[1]+...+[9]              │
      │  16. Compute SHA-256 of reassembled bytes           │
      │  17. Compare with server's checksum → MATCH         │
      │                                                     │
      │── 18. ACK (Transfer Successful) ───────────────────►│
      │                                                     │
```

**Concurrent scenario:** If Client B uploads simultaneously, the server handles it in a separate thread/coroutine with completely isolated state. Client A's chunks never appear in Client B's session.

### 2.4 Network Packet Verification

TCP communication verified with Wireshark capturing on the loopback adapter:

![Wireshark Capture](assets/wireshark_capture.png)

---

## 3. Running the Solution

### 3.1 Prerequisites

- **Python 3.10+** (tested on 3.10, 3.11, 3.12)
- **pip** (for optional dependencies)
- **Docker** (optional, for containerized deployment)

### 3.2 Local Setup

```bash
# Clone the repository
git clone https://github.com/KarthikSagarP/file-transfer.git
cd file-transfer

# Option A: One-command launch (starts server + TUI)
python launcher.py

# Option B: Separate terminals
python server_hybrid.py              # Terminal 1: start server
python client.py                     # Terminal 2: start client TUI

# Optional: install rich for formatted TUI output
pip install rich

# Optional: install test/benchmark dependencies
pip install pytest matplotlib
```

### 3.3 Docker Deployment

```bash
# Build images
docker build -f docker/Dockerfile --target server -t fts-server .
docker build -f docker/Dockerfile --target client -t fts-client .

# Run with docker-compose
docker compose -f docker/docker-compose.yml up -d server

# Verify health
docker compose -f docker/docker-compose.yml ps

# Run client against containerized server
docker compose -f docker/docker-compose.yml run --rm client
```

### 3.4 Remote Server Deployment

```bash
# On the remote host (Ubuntu/Debian)
ssh user@your-server-ip

# Install Python and clone
sudo apt update && sudo apt install -y python3.12 python3-pip
git clone <repo-url> /opt/file-transfer
cd /opt/file-transfer

# Run directly
python3 server_hybrid.py

# OR run as a systemd service (recommended for production)
sudo nano /etc/systemd/system/file-transfer.service
```

**Systemd service file:**
```ini
[Unit]
Description=File Transfer Server (Hybrid)
After=network.target

[Service]
Type=simple
User=ftuser
WorkingDirectory=/opt/file-transfer
ExecStart=/usr/bin/python3 server_hybrid.py
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now file-transfer

# Open firewall
sudo ufw allow 9000/tcp

# Client connects from any machine
python client.py   # then: config host <server-ip>
```

### 3.5 Make Targets

```
make launch           Start server + TUI (one command)
make server           Start hybrid server standalone
make test             Run all 32 tests
make test-quick       Run core tests only
make benchmark        Run full benchmark suite
make charts           Generate benchmark charts
make docker-build     Build Docker images
make docker-up        Start server container
make clean            Remove generated files
```

---

## 4. Security Considerations

| Concern | Mitigation |
|---------|------------|
| **No authentication** | Protocol does not include auth. For production, wrap in SSH tunnel (see below) or add TLS |
| **No encryption** | Data travels in plaintext. Use SSH tunnel for sensitive transfers |
| **Input sanitization** | All filenames are sanitized with `os.path.basename()` — prevents path traversal attacks |
| **Serialization safety** | Uses `struct` + `json` — never `pickle`, which can execute arbitrary code |
| **Resource limits** | LRU cache bounded to 64 entries. Socket timeouts prevent connection exhaustion |
| **Non-root execution** | Systemd service runs as dedicated `ftuser`, not root |
| **Firewall** | Deployment guide includes `ufw` rules to restrict port 9000 access |
| **Session isolation** | Each client handler uses local variables only — no shared mutable state between sessions |

**SSH tunnel for encrypted transfers:**
```bash
# On client machine — encrypts all traffic over SSH
ssh -L 9000:localhost:9000 user@server-ip -N &
python client.py   # connects to localhost:9000, tunneled to server
```

---

## 5. Logging

The server uses Python's `logging` module with structured output:

```
14:33:02 [SERVER Client-57705] Connected: ('127.0.0.1', 57705)
14:33:02 [SERVER Client-57705] Upload: 'report.pdf' (10240 bytes)
14:33:02 [SERVER Client-57705] Cache STORE: a3f8c1... (12/64 entries)
14:33:02 [SERVER Client-57705] Sent 9/10 chunks | dropped 1
14:33:02 [SERVER Client-57705] Retransmit round 1: 1 chunks: [4]
14:33:02 [SERVER Client-57705] Client ACK — transfer verified.
```

**Log characteristics:**
- **Per-thread identification:** Each log line includes the client's thread name (`Client-57705`) for tracing concurrent sessions
- **Timestamp:** Every entry is timestamped (`HH:MM:SS`)
- **Structured data:** Chunk counts, cache stats, drop/corrupt counts, and checksums are logged at each transfer phase
- **Error visibility:** Connection errors, checksum mismatches, and retransmission failures are logged with `exc_info=True` for full stack traces
- **Configurable level:** `logging.basicConfig(level=logging.INFO)` — change to `DEBUG` for per-chunk tracing

**For production deployment**, logs should be directed to a file with rotation. When running as a systemd service, output goes to `journalctl`:
```bash
sudo journalctl -u file-transfer -f          # live tail
sudo journalctl -u file-transfer --since today  # today's logs
```

---

## 6. CI/CD Pipeline

### 6.1 Continuous Integration (`.github/workflows/ci.yml`)

Triggered on every push and pull request:

```
Push/PR
  │
  ├─► Unit + Integration + Error Tests
  │     ├── Python 3.10
  │     ├── Python 3.11        (matrix strategy)
  │     └── Python 3.12
  │           │
  │           ▼
  │     32 tests must pass
  │           │
  └─► Benchmark Suite (quick)
        │
        ▼
    Charts generated + artifacts uploaded
```

**Test stages in CI:**
1. **All 32 automated tests** — functional, error, boundary, concurrency independence, stress, integration
2. **Benchmark suite (quick mode)** — throughput and backend comparison for regression detection
3. **Artifact upload** — test reports (JUnit XML) and benchmark charts preserved per build

### 6.2 Continuous Deployment (`.github/workflows/cd.yml`)

Triggered on push to `main` or version tags:

```
Push to main / tag v*
  │
  ├─► Build Docker image (multi-stage)
  │     └── Push to GitHub Container Registry (ghcr.io)
  │
  └─► Deploy to remote server via SSH
        ├── docker compose pull
        ├── docker compose up -d (rolling restart)
        └── Health check verification
```

### 6.3 Test Suite for CI/CD Pipeline

| Stage | Tests Run | Purpose |
|-------|----------|---------|
| **Commit** | 32 pytest tests across 3 Python versions | Catch regressions before merge |
| **Pre-merge** | Benchmark suite (quick) | Detect performance regressions |
| **Post-merge (main)** | Docker build + health check | Verify containerized deployment works |
| **Production deploy** | SSH deploy + `docker compose ps` health | Confirm live service is responding |

---

## 7. Benchmark Results

Run: `python benchmark_suite.py` then `python generate_chart.py`

![Benchmark Suite Overview](assets/bench_overview.png)

### 7.1 Throughput

| File Size | Time | Throughput |
|-----------|------|-----------|
| 1 KB | 43 ms | 23 KB/s |
| 10 KB | 50 ms | 198 KB/s |
| 100 KB | 52 ms | 1,914 KB/s |
| 1 MB | 122 ms | 8,212 KB/s |

### 7.2 Scalability

Peak aggregate throughput at 10 concurrent clients (2,477 KB/s). Graceful linear degradation at 20 clients — no crashes or failures.

### 7.3 Reliability

**100% success rate** across all 7 error conditions (clean, 5% drop, 10% drop, 20% drop, 10% corrupt, mixed, heavy).

### 7.4 Backend Comparison (1 MB files)

| Backend | Throughput | Advantage |
|---------|-----------|-----------|
| Threaded | 9,104 KB/s | Baseline |
| Async | 11,331 KB/s | +24% |
| **Hybrid** (default) | **12,400 KB/s** | **+36%** |

The hybrid backend wins for large files where CPU-bound work (SHA-256, file splitting) blocks the async event loop. For small files (<100KB), pure async has lowest overhead.

---

## 8. Testing

```bash
python -m pytest tests/test_transfer.py -v     # all 32 tests
make test-quick                                 # core tests only
```

**32 tests across 10 categories:**

| Category | Count | What it proves |
|----------|-------|---------------|
| Core transfer | 4 | Text, binary, chunk boundary, 1-byte edge cases |
| Out-of-order | 1 | Shuffled chunks reassemble correctly |
| Retransmission | 1 | Recovery from 20% drops + 10% corruption |
| Concurrent clients | 1 | 5 simultaneous transfers succeed |
| Cache | 4 | Hit/miss, LRU eviction, TTL expiry, thread safety |
| Server commands | 4 | List, status, upload-then-list, download nonexistent |
| CRC32 integrity | 4 | Pack/unpack roundtrip, corruption detection |
| Concurrency independence | 2 | 10-client file isolation + crash isolation |
| Boundary cases | 4 | Exact chunk, 1-byte over, all-zeros, all-0xFF |
| Integration | 2 | External SHA-256 and MD5 verification |
| Progressive stress | 2 | Drop rates 5→25%, client counts 2→10 |

---

## 9. Project Structure

```
file-transfer/
├── launcher.py              # One-command startup (hybrid server + TUI)
├── protocol.py              # Wire protocol, CRC32, SHA-256, file splitting
├── cache.py                 # Thread-safe LRU cache with TTL
├── server.py                # Threaded server backend
├── server_async.py          # Async server backend (asyncio)
├── server_hybrid.py         # Hybrid server (asyncio + ThreadPoolExecutor)
├── client.py                # Client with session management and TUI
├── benchmark.py             # Backend comparison (legacy)
├── benchmark_suite.py       # Full benchmark suite (4 categories)
├── generate_chart.py        # Chart generator
├── requirements.txt         # Python dependencies
├── Makefile                 # Common targets (test, benchmark, docker, etc.)
├── DESIGN.md                # Detailed design document (14 sections)
├── docker/
│   ├── Dockerfile           # Multi-stage build (server + client targets)
│   └── docker-compose.yml   # Container orchestration with healthcheck
├── .github/workflows/
│   ├── ci.yml               # CI: tests on 3 Python versions + benchmarks
│   └── cd.yml               # CD: Docker build → GHCR → SSH deploy
├── assets/
│   ├── bench_overview.png   # Combined 4-panel benchmark chart
│   ├── bench_throughput.png
│   ├── bench_scalability.png
│   ├── bench_reliability.png
│   ├── bench_backends.png
│   └── wireshark_capture.png
├── tests/
│   └── test_transfer.py     # 32 automated tests
├── sample_files/
│   ├── test.txt
│   └── binary_test.bin
├── .gitignore
└── README.md
```

---

## 10. Design Documentation

For detailed technical documentation, see [DESIGN.md](DESIGN.md) covering:

- Wire protocol specification with message type table
- Data flow sequence diagrams
- File chunking and reassembly algorithms
- Concurrency model comparison (threaded vs async vs hybrid)
- Caching architecture (server LRU + client LFU)
- Error handling strategy with scenario matrix
- Network error simulation methodology
- Technology stack rationale with alternatives considered
- Full test matrix (32 tests)
- Benchmark methodology and analysis
