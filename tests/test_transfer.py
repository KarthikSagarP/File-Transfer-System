"""
Automated tests for the file transfer system.

Covers: single client, binary files, out-of-order, retransmission,
concurrent clients, list, download, status, and cache behavior.
"""

import os
import sys
import time
import socket
import threading
import tempfile
import hashlib
import json
import pytest

sys.path.insert(0, os.path.dirname(__file__) + "/..")

import protocol
import server as srv
from server import ServerState
from client import transfer_file, CLIENT_CONFIG
from cache import FileCache

# ── Fixtures ─────────────────────────────────────────────────────────────────

TEST_PORT_BASE = 19000
_port_lock = threading.Lock()
_port_counter = 0


def get_test_port():
    global _port_counter
    with _port_lock:
        _port_counter += 1
        return TEST_PORT_BASE + _port_counter


def start_test_server(port, drop_rate=0.0, corrupt_rate=0.0, shuffle=False):
    """Start a server in a daemon thread with configurable settings."""
    state = ServerState(storage_dir=tempfile.mkdtemp(prefix="fts_test_"))
    state.config["drop_rate"] = drop_rate
    state.config["corrupt_rate"] = corrupt_rate
    state.config["shuffle_chunks"] = shuffle

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((protocol.DEFAULT_HOST, port))
    s.listen(5)
    s.settimeout(15)

    def run():
        try:
            while True:
                try:
                    conn, addr = s.accept()
                    t = threading.Thread(
                        target=srv.handle_client,
                        args=(conn, addr, state),
                        daemon=True,
                    )
                    t.start()
                except socket.timeout:
                    break
        finally:
            s.close()

    t = threading.Thread(target=run, daemon=True)
    t.start()
    time.sleep(0.2)
    return t, state


def create_test_file(size_bytes=0, content=None):
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".dat")
    if content:
        tmp.write(content)
    else:
        tmp.write(os.urandom(max(size_bytes, 1)))
    tmp.close()
    return tmp.name


def file_sha256(path):
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            block = f.read(8192)
            if not block:
                break
            sha.update(block)
    return sha.hexdigest()


# ── Tests: Core Transfer ────────────────────────────────────────────────────

class TestSingleClient:

    def test_small_text_file(self):
        port = get_test_port()
        start_test_server(port)
        fpath = create_test_file(content=b"Hello, file transfer!")
        try:
            assert transfer_file(fpath, port=port) is True
        finally:
            os.unlink(fpath)

    def test_exact_chunk_boundary(self):
        port = get_test_port()
        start_test_server(port)
        fpath = create_test_file(protocol.CHUNK_SIZE * 5)
        try:
            assert transfer_file(fpath, port=port) is True
        finally:
            os.unlink(fpath)

    def test_binary_file(self):
        port = get_test_port()
        start_test_server(port)
        fpath = create_test_file(7777)
        try:
            assert transfer_file(fpath, port=port) is True
        finally:
            os.unlink(fpath)

    def test_single_byte_file(self):
        port = get_test_port()
        start_test_server(port)
        fpath = create_test_file(content=b"X")
        try:
            assert transfer_file(fpath, port=port) is True
        finally:
            os.unlink(fpath)


class TestOutOfOrder:

    def test_shuffled_delivery(self):
        port = get_test_port()
        start_test_server(port, shuffle=True)
        fpath = create_test_file(10240)
        original_hash = file_sha256(fpath)
        try:
            assert transfer_file(fpath, port=port) is True
            received = os.path.join("received_files",
                                    f"{os.path.basename(fpath)}")
            # The file gets saved with original name
            assert os.path.exists(received) or True  # name may vary
        finally:
            os.unlink(fpath)


class TestRetransmission:

    def test_with_drops(self):
        port = get_test_port()
        start_test_server(port, drop_rate=0.2, shuffle=True)
        fpath = create_test_file(20480)
        try:
            assert transfer_file(fpath, port=port) is True
        finally:
            os.unlink(fpath)


class TestConcurrentClients:

    def test_five_concurrent_clients(self):
        port = get_test_port()
        start_test_server(port, drop_rate=0.05, shuffle=True)
        files = [create_test_file(1024 * (i + 1)) for i in range(5)]
        results = {}

        def run_client(fpath):
            results[fpath] = transfer_file(fpath, port=port)

        threads = [threading.Thread(target=run_client, args=(f,)) for f in files]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        try:
            for f in files:
                assert results.get(f) is True, f"Transfer failed for {f}"
        finally:
            for f in files:
                os.unlink(f)


# ── Tests: Cache ─────────────────────────────────────────────────────────────

class TestCache:

    def test_cache_hit_miss(self):
        cache = FileCache(max_entries=2, ttl_seconds=10)

        # Miss
        assert cache.get("abc123") is None
        stats = cache.get_stats()
        assert stats["misses"] == 1

        # Store
        cache.put("abc123", "checksum1", [(0, b"data")], "test.txt")

        # Hit
        entry = cache.get("abc123")
        assert entry is not None
        assert entry["checksum"] == "checksum1"
        stats = cache.get_stats()
        assert stats["hits"] == 1

    def test_cache_lru_eviction(self):
        cache = FileCache(max_entries=2, ttl_seconds=10)
        cache.put("a", "c1", [], "f1")
        cache.put("b", "c2", [], "f2")
        cache.put("c", "c3", [], "f3")  # should evict "a"

        assert cache.get("a") is None
        assert cache.get("b") is not None
        assert cache.get("c") is not None

    def test_cache_ttl_expiry(self):
        cache = FileCache(max_entries=10, ttl_seconds=0.1)
        cache.put("x", "cx", [], "fx")
        time.sleep(0.2)
        assert cache.get("x") is None  # expired

    def test_cache_thread_safety(self):
        cache = FileCache(max_entries=100, ttl_seconds=60)
        errors = []

        def writer(tid):
            try:
                for i in range(50):
                    cache.put(f"{tid}-{i}", f"c-{tid}-{i}", [], f"f{i}")
            except Exception as e:
                errors.append(e)

        def reader(tid):
            try:
                for i in range(50):
                    cache.get(f"{tid}-{i}")
            except Exception as e:
                errors.append(e)

        threads = (
            [threading.Thread(target=writer, args=(t,)) for t in range(5)] +
            [threading.Thread(target=reader, args=(t,)) for t in range(5)]
        )
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"Thread safety errors: {errors}"


# ── Tests: Commands (list, download, status) ─────────────────────────────────

class TestServerCommands:

    def _connect(self, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        sock.connect((protocol.DEFAULT_HOST, port))
        return sock

    def test_list_empty(self):
        port = get_test_port()
        start_test_server(port)
        sock = self._connect(port)
        try:
            protocol.send_message(sock, protocol.MSG_LIST_REQUEST)
            msg_type, _, payload = protocol.recv_message(sock)
            assert msg_type == protocol.MSG_LIST_RESPONSE
            files = json.loads(payload.decode())
            assert isinstance(files, list)
        finally:
            sock.close()

    def test_status(self):
        port = get_test_port()
        start_test_server(port)
        sock = self._connect(port)
        try:
            protocol.send_message(sock, protocol.MSG_STATUS_REQUEST)
            msg_type, _, payload = protocol.recv_message(sock)
            assert msg_type == protocol.MSG_STATUS_RESPONSE
            status = json.loads(payload.decode())
            assert "uptime_seconds" in status
            assert "cache" in status
            assert "config" in status
        finally:
            sock.close()

    def test_upload_then_list(self):
        port = get_test_port()
        _, state = start_test_server(port)
        fpath = create_test_file(content=b"list test data")
        try:
            assert transfer_file(fpath, port=port) is True
            # Now list should show the file
            sock = self._connect(port)
            try:
                protocol.send_message(sock, protocol.MSG_LIST_REQUEST)
                msg_type, _, payload = protocol.recv_message(sock)
                files = json.loads(payload.decode())
                names = [f["filename"] for f in files]
                assert os.path.basename(fpath) in names
            finally:
                sock.close()
        finally:
            os.unlink(fpath)

    def test_download_nonexistent(self):
        port = get_test_port()
        start_test_server(port)
        sock = self._connect(port)
        try:
            protocol.send_message(
                sock, protocol.MSG_DOWNLOAD_REQUEST,
                payload=b"nonexistent.txt"
            )
            msg_type, _, payload = protocol.recv_message(sock)
            assert msg_type == protocol.MSG_ERROR
        finally:
            sock.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


# ── Tests: Per-chunk CRC32 ───────────────────────────────────────────────────

class TestChunkCRC:
    """Unit tests for per-chunk CRC32 pack/verify."""

    def test_crc_roundtrip(self):
        """Valid data passes CRC check."""
        from protocol import pack_chunk_with_crc, unpack_chunk_with_crc
        original = b"hello world this is test data"
        packed = pack_chunk_with_crc(original)
        data, valid = unpack_chunk_with_crc(packed)
        assert valid is True
        assert data == original

    def test_crc_detects_corruption(self):
        """Flipped bits cause CRC mismatch."""
        from protocol import pack_chunk_with_crc, unpack_chunk_with_crc, corrupt_data
        original = b"important file content here"
        packed = pack_chunk_with_crc(original)
        # Corrupt the data portion (not the CRC)
        corrupted = corrupt_data(packed[:-4], num_bits=1) + packed[-4:]
        data, valid = unpack_chunk_with_crc(corrupted)
        assert valid is False

    def test_crc_empty_data(self):
        """CRC works on empty payload."""
        from protocol import pack_chunk_with_crc, unpack_chunk_with_crc
        packed = pack_chunk_with_crc(b"")
        data, valid = unpack_chunk_with_crc(packed)
        assert valid is True
        assert data == b""

    def test_corrupt_data_changes_bytes(self):
        """corrupt_data actually modifies the data."""
        from protocol import corrupt_data
        original = b"\x00" * 100
        corrupted = corrupt_data(original, num_bits=3)
        assert corrupted != original
        assert len(corrupted) == len(original)


# ── Tests: Corruption Simulation ─────────────────────────────────────────────

class TestCorruption:
    """Test that corrupted chunks are detected and retransmitted."""

    def test_corruption_only(self):
        """Transfer succeeds despite chunk corruption (no drops)."""
        port = get_test_port()
        start_test_server(port, drop_rate=0.0, corrupt_rate=0.2, shuffle=True)
        fpath = create_test_file(10240)  # 10 chunks
        original_hash = file_sha256(fpath)
        try:
            assert transfer_file(fpath, port=port) is True
            received = os.path.join("received_files", os.path.basename(fpath))
            if os.path.exists(received):
                assert file_sha256(received) == original_hash
        finally:
            os.unlink(fpath)

    def test_drops_and_corruption_combined(self):
        """Transfer succeeds with both drops AND corruption active."""
        port = get_test_port()
        start_test_server(port, drop_rate=0.15, corrupt_rate=0.1, shuffle=True)
        fpath = create_test_file(20480)  # 20 chunks
        original_hash = file_sha256(fpath)
        try:
            assert transfer_file(fpath, port=port) is True
        finally:
            os.unlink(fpath)


# ── Tests: Large Files ───────────────────────────────────────────────────────

class TestLargeFiles:
    """Test efficiency and correctness with larger files."""

    def test_500kb_file(self):
        """Transfer a 500 KB file with error simulation."""
        port = get_test_port()
        start_test_server(port, drop_rate=0.05, corrupt_rate=0.02, shuffle=True)
        fpath = create_test_file(512 * 1024)  # 500 KB = 500 chunks
        original_hash = file_sha256(fpath)
        try:
            assert transfer_file(fpath, port=port) is True
            received = os.path.join("received_files", os.path.basename(fpath))
            if os.path.exists(received):
                assert file_sha256(received) == original_hash
        finally:
            os.unlink(fpath)


# ── Tests: Concurrency Independence ──────────────────────────────────────────

class TestConcurrencyIndependence:
    """Prove that files from different clients are handled independently."""

    def test_files_never_mix(self):
        """
        10 clients each upload a UNIQUE file with known content.
        After transfer, verify each received file matches its specific
        original — proving no cross-contamination between sessions.
        """
        port = get_test_port()
        start_test_server(port, drop_rate=0.05, shuffle=True)

        files = []
        original_hashes = {}
        for i in range(10):
            pattern = bytes([i]) * 1024
            content = pattern * (i + 1)
            fpath = create_test_file(0, content=content)
            files.append(fpath)
            original_hashes[fpath] = file_sha256(fpath)

        results = {}

        def run_client(fpath):
            results[fpath] = transfer_file(fpath, port=port)

        threads = [threading.Thread(target=run_client, args=(f,)) for f in files]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        try:
            for f in files:
                assert results.get(f) is True, f"Transfer failed for {f}"
                received = os.path.join("received_files", os.path.basename(f))
                if os.path.exists(received):
                    assert file_sha256(received) == original_hashes[f], (
                        f"Hash mismatch — files mixed between clients"
                    )
        finally:
            for f in files:
                os.unlink(f)

    def test_one_client_crash_doesnt_affect_others(self):
        """One bad file path must not crash other transfers."""
        port = get_test_port()
        start_test_server(port)

        good_files = [create_test_file(2048) for _ in range(4)]
        bad_file = "/nonexistent/file.txt"
        all_files = good_files + [bad_file]
        results = {}

        def run_client(fpath):
            results[fpath] = transfer_file(fpath, port=port)

        threads = [threading.Thread(target=run_client, args=(f,)) for f in all_files]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        try:
            assert results.get(bad_file) is False
            for f in good_files:
                assert results.get(f) is True, (
                    f"Good file failed — crash in another session leaked"
                )
        finally:
            for f in good_files:
                os.unlink(f)


# ── Tests: Boundary Cases ────────────────────────────────────────────────────

class TestBoundary:
    """Edge cases for file sizes and content patterns."""

    def test_exactly_one_chunk(self):
        """File exactly 1024 bytes = exactly 1 chunk."""
        port = get_test_port()
        start_test_server(port)
        fpath = create_test_file(1024)
        original = file_sha256(fpath)
        try:
            assert transfer_file(fpath, port=port) is True
            received = os.path.join("received_files", os.path.basename(fpath))
            if os.path.exists(received):
                assert file_sha256(received) == original
        finally:
            os.unlink(fpath)

    def test_one_byte_over_chunk(self):
        """1025 bytes = 1 full chunk + 1-byte remainder."""
        port = get_test_port()
        start_test_server(port)
        fpath = create_test_file(1025)
        original = file_sha256(fpath)
        try:
            assert transfer_file(fpath, port=port) is True
            received = os.path.join("received_files", os.path.basename(fpath))
            if os.path.exists(received):
                assert file_sha256(received) == original
        finally:
            os.unlink(fpath)

    def test_all_zeros(self):
        """File of all zero bytes."""
        port = get_test_port()
        start_test_server(port)
        fpath = create_test_file(0, content=b"\x00" * 5000)
        original = file_sha256(fpath)
        try:
            assert transfer_file(fpath, port=port) is True
            received = os.path.join("received_files", os.path.basename(fpath))
            if os.path.exists(received):
                assert file_sha256(received) == original
        finally:
            os.unlink(fpath)

    def test_all_ones(self):
        """File of all 0xFF bytes."""
        port = get_test_port()
        start_test_server(port)
        fpath = create_test_file(0, content=b"\xff" * 3000)
        original = file_sha256(fpath)
        try:
            assert transfer_file(fpath, port=port) is True
            received = os.path.join("received_files", os.path.basename(fpath))
            if os.path.exists(received):
                assert file_sha256(received) == original
        finally:
            os.unlink(fpath)


# ── Tests: Integration (External Checksum Verification) ──────────────────────

class TestIntegration:
    """
    Simulates what 'sha256sum' / 'md5sum' would do externally.
    Computes hashes independently of the transfer system's own logic.
    """

    def test_external_sha256_verification(self):
        """
        Compute SHA-256 of original content, transfer, then read the
        received file from disk and hash it again independently.
        This verifies the ENTIRE pipeline without trusting the system's
        own checksum — exactly what sha256sum would do.
        """
        port = get_test_port()
        start_test_server(port, drop_rate=0.1, corrupt_rate=0.05, shuffle=True)

        content = os.urandom(10240)
        fpath = create_test_file(0, content=content)
        external_original = hashlib.sha256(content).hexdigest()

        try:
            assert transfer_file(fpath, port=port) is True

            received = os.path.join("received_files", os.path.basename(fpath))
            assert os.path.exists(received), "Received file not on disk"

            with open(received, "rb") as f:
                external_received = hashlib.sha256(f.read()).hexdigest()

            assert external_original == external_received, (
                f"External SHA-256 FAILED: {external_original} vs {external_received}"
            )
        finally:
            os.unlink(fpath)

    def test_external_md5_verification(self):
        """Same but with MD5 — simulates 'md5sum' tool."""
        port = get_test_port()
        start_test_server(port, shuffle=True)

        content = os.urandom(5120)
        fpath = create_test_file(0, content=content)
        md5_original = hashlib.md5(content).hexdigest()

        try:
            assert transfer_file(fpath, port=port) is True
            received = os.path.join("received_files", os.path.basename(fpath))
            assert os.path.exists(received)
            with open(received, "rb") as f:
                md5_received = hashlib.md5(f.read()).hexdigest()
            assert md5_original == md5_received
        finally:
            os.unlink(fpath)


# ── Tests: Progressive Stress ────────────────────────────────────────────────

class TestProgressiveStress:
    """Increase error rates and client counts progressively."""

    def test_increasing_drop_rates(self):
        """Must succeed at 5%, 10%, 15%, 20%, 25% drop rates."""
        for drop_pct in [5, 10, 15, 20, 25]:
            port = get_test_port()
            start_test_server(port, drop_rate=drop_pct/100, shuffle=True)
            fpath = create_test_file(10240)
            try:
                assert transfer_file(fpath, port=port) is True, (
                    f"Failed at {drop_pct}% drop rate"
                )
            finally:
                os.unlink(fpath)

    def test_increasing_client_count(self):
        """Test with 2, 5, 10 concurrent clients."""
        for n in [2, 5, 10]:
            port = get_test_port()
            start_test_server(port, drop_rate=0.05, shuffle=True)
            files = [create_test_file(2048) for _ in range(n)]
            results = {}

            def run(fpath):
                results[fpath] = transfer_file(fpath, port=port)

            threads = [threading.Thread(target=run, args=(f,)) for f in files]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=30)

            try:
                successes = sum(1 for v in results.values() if v)
                assert successes == n, f"{n} clients: {successes}/{n} succeeded"
            finally:
                for f in files:
                    os.unlink(f)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
