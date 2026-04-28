"""
Hybrid file transfer server: asyncio event loop + thread pool.

Uses asyncio for I/O multiplexing and delegates CPU-bound operations
(checksum computation, file splitting) to a ThreadPoolExecutor.
This gives the best of both worlds for mixed I/O + CPU workloads.

Usage:
    python server_hybrid.py [port]
"""

import asyncio
import os
import sys
import json
import random
import time
import logging
import struct
from concurrent.futures import ThreadPoolExecutor

from protocol import (
    DEFAULT_HOST, DEFAULT_PORT, CHUNK_SIZE, HEADER_SIZE, HEADER_FORMAT,
    MSG_UPLOAD_REQUEST, MSG_FILE_META, MSG_CHUNK, MSG_ACK,
    MSG_RETRANSMIT_REQ, MSG_TRANSFER_DONE, MSG_ERROR,
    MSG_LIST_REQUEST, MSG_LIST_RESPONSE,
    MSG_DOWNLOAD_REQUEST,
    MSG_STATUS_REQUEST, MSG_STATUS_RESPONSE,
    MSG_QUERY_REQUEST, MSG_QUERY_RESPONSE,
    compute_checksum, compute_checksum_bytes, split_file,
    build_meta_payload, pack_chunk_with_crc, corrupt_data,
)
from cache import FileCache

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [HYBRID-SERVER] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


class HybridServer:
    """
    Hybrid async + threaded server.
    
    Key difference from pure async server:
        - Maintains a dedicated ThreadPoolExecutor sized to CPU count
        - ALL file I/O and crypto operations go through the executor
        - The event loop only handles network I/O and coordination
    """

    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT, workers=None):
        self.host = host
        self.port = port
        self.storage_dir = "server_storage"
        os.makedirs(self.storage_dir, exist_ok=True)

        cpu_count = os.cpu_count() or 4
        self.executor = ThreadPoolExecutor(
            max_workers=workers or cpu_count,
            thread_name_prefix="cpu-worker",
        )
        self.cache = FileCache(max_entries=64, ttl_seconds=300)
        self.config = {
            "drop_rate": 0.1,
            "corrupt_rate": 0.05,
            "shuffle_chunks": True,
            "chunk_size": CHUNK_SIZE,
            "max_retransmit_rounds": 10,
        }
        self._active = 0
        self._transfers = 0
        self._bytes = 0
        self._start = time.time()

    def get_status(self):
        return {
            "uptime_seconds": round(time.time() - self._start, 1),
            "active_clients": self._active,
            "total_transfers": self._transfers,
            "total_bytes_transferred": self._bytes,
            "cpu_workers": self.executor._max_workers,
            "cache": self.cache.get_stats(),
            "config": self.config,
            "backend": "hybrid (asyncio + threadpool)",
        }

    def list_files(self):
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

    # ── Async protocol helpers ───────────────────────────────────────────

    async def _recv_exactly(self, reader, n):
        data = bytearray()
        while len(data) < n:
            chunk = await reader.read(n - len(data))
            if not chunk:
                raise ConnectionError("Connection closed")
            data.extend(chunk)
        return bytes(data)

    async def _recv_message(self, reader):
        header = await self._recv_exactly(reader, HEADER_SIZE)
        msg_type, seq_num, payload_len = struct.unpack(HEADER_FORMAT, header)
        payload = (await self._recv_exactly(reader, payload_len)
                   if payload_len > 0 else b"")
        return msg_type, seq_num, payload

    async def _send_message(self, writer, msg_type, seq_num=0, payload=b""):
        header = struct.pack(HEADER_FORMAT, msg_type, seq_num, len(payload))
        writer.write(header + payload)
        await writer.drain()

    # ── CPU-bound operations (run in thread pool) ────────────────────────

    async def _compute_hash(self, data):
        """Offload SHA-256 to thread pool."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.executor, compute_checksum_bytes, data
        )

    async def _split_file_async(self, filepath):
        """Offload file splitting to thread pool."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.executor, split_file, filepath, self.config["chunk_size"]
        )

    async def _write_file(self, filepath, data):
        """Offload file write to thread pool."""
        def _write():
            with open(filepath, "wb") as f:
                f.write(data)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(self.executor, _write)

    # ── Client handler ───────────────────────────────────────────────────

    async def handle_client(self, reader, writer):
        addr = writer.get_extra_info("peername")
        self._active += 1
        log.info(f"Connected: {addr}")

        try:
            while True:
                try:
                    msg_type, _, payload = await asyncio.wait_for(
                        self._recv_message(reader), timeout=30
                    )
                except (ConnectionError, asyncio.TimeoutError):
                    break

                try:
                    if msg_type == MSG_UPLOAD_REQUEST:
                        await self._handle_upload(reader, writer, payload)
                    elif msg_type == MSG_DOWNLOAD_REQUEST:
                        await self._handle_download(reader, writer, payload)
                    elif msg_type == MSG_LIST_REQUEST:
                        files = self.list_files()
                        await self._send_message(
                            writer, MSG_LIST_RESPONSE,
                            payload=json.dumps(files).encode()
                        )
                    elif msg_type == MSG_STATUS_REQUEST:
                        status = self.get_status()
                        await self._send_message(
                            writer, MSG_STATUS_RESPONSE,
                            payload=json.dumps(status, indent=2).encode()
                        )
                    elif msg_type == MSG_QUERY_REQUEST:
                        from server import (
                            _query_index_longlist, _query_index_shortlist,
                            _query_filehash_all, _query_filehash_verify,
                        )
                        query = json.loads(payload.decode())
                        qtype = query.get("type", "")
                        handler_map = {
                            "index_longlist": _query_index_longlist,
                            "index_shortlist": _query_index_shortlist,
                            "filehash_all": _query_filehash_all,
                            "filehash_verify": _query_filehash_verify,
                        }
                        handler = handler_map.get(qtype)
                        if handler:
                            loop = asyncio.get_event_loop()
                            if qtype == "filehash_all":
                                result = await loop.run_in_executor(
                                    self.executor, handler, self)
                            else:
                                result = await loop.run_in_executor(
                                    self.executor, handler, self, query)
                            await self._send_message(
                                writer, MSG_QUERY_RESPONSE,
                                payload=json.dumps(result).encode()
                            )
                        else:
                            await self._send_message(
                                writer, MSG_ERROR,
                                payload=f"Unknown query: {qtype}".encode()
                            )
                    else:
                        await self._send_message(
                            writer, MSG_ERROR,
                            payload=f"Unknown: {msg_type}".encode()
                        )
                except ConnectionError:
                    break
                except Exception as e:
                    log.error(f"Error: {e}", exc_info=True)
                    try:
                        await self._send_message(
                            writer, MSG_ERROR, payload=str(e).encode()
                        )
                    except Exception:
                        break
        finally:
            self._active -= 1
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            log.info(f"Disconnected: {addr}")

    async def _handle_upload(self, reader, writer, payload):
        meta = json.loads(payload.decode())
        filename = os.path.basename(meta.get("filename", "unknown"))
        file_size = meta.get("file_size", 0)
        log.info(f"Upload: '{filename}' ({file_size} bytes)")

        await self._send_message(writer, MSG_ACK)
        file_data = await self._recv_exactly(reader, file_size)

        fpath = os.path.join(self.storage_dir, filename)
        await self._write_file(fpath, file_data)

        # Offload CPU work
        content_hash = await self._compute_hash(file_data)

        cached = self.cache.get(content_hash)
        if cached:
            checksum, chunks = cached["checksum"], cached["chunks"]
        else:
            checksum = content_hash
            chunks = await self._split_file_async(fpath)
            self.cache.put(content_hash, checksum, chunks, filename)

        total = len(chunks)
        await self._send_message(
            writer, MSG_FILE_META,
            payload=build_meta_payload(filename, checksum, total)
        )
        await self._send_chunks(writer, chunks, is_initial=True)
        await self._retransmit_loop(reader, writer, chunks)

        self._transfers += 1
        self._bytes += file_size

    async def _handle_download(self, reader, writer, payload):
        filename = payload.decode().strip()
        fpath = os.path.join(self.storage_dir, os.path.basename(filename))

        if not os.path.isfile(fpath):
            await self._send_message(
                writer, MSG_ERROR,
                payload=f"Not found: {filename}".encode()
            )
            return

        loop = asyncio.get_event_loop()
        checksum = await loop.run_in_executor(
            self.executor, compute_checksum, fpath
        )

        cached = self.cache.get(checksum)
        if cached:
            chunks = cached["chunks"]
        else:
            chunks = await self._split_file_async(fpath)
            self.cache.put(checksum, checksum, chunks, filename)

        total = len(chunks)
        await self._send_message(
            writer, MSG_FILE_META,
            payload=build_meta_payload(filename, checksum, total)
        )
        await self._send_chunks(writer, chunks, is_initial=True)
        await self._retransmit_loop(reader, writer, chunks)

        self._transfers += 1
        self._bytes += os.path.getsize(fpath)

    async def _send_chunks(self, writer, chunks, is_initial=True,
                           seq_filter=None):
        to_send = chunks if seq_filter is None else [
            (s, d) for s, d in chunks if s in seq_filter
        ]
        if is_initial and self.config["shuffle_chunks"]:
            to_send = list(to_send)
            random.shuffle(to_send)

        drop_rate = self.config["drop_rate"]
        corrupt_rate = self.config.get("corrupt_rate", 0.0)
        for seq_num, data in to_send:
            if is_initial and random.random() < drop_rate:
                continue
            if is_initial and random.random() < corrupt_rate:
                corrupted = corrupt_data(data, num_bits=2)
                payload = corrupted + pack_chunk_with_crc(data)[-4:]
            else:
                payload = pack_chunk_with_crc(data)
            await self._send_message(writer, MSG_CHUNK, seq_num=seq_num,
                                     payload=payload)

    async def _retransmit_loop(self, reader, writer, chunks):
        for _ in range(self.config["max_retransmit_rounds"]):
            await self._send_message(writer, MSG_TRANSFER_DONE)
            try:
                msg_type, _, payload = await asyncio.wait_for(
                    self._recv_message(reader), timeout=30
                )
            except (ConnectionError, asyncio.TimeoutError):
                return

            if msg_type == MSG_ACK:
                log.info("Client ACK")
                return
            if msg_type == MSG_RETRANSMIT_REQ:
                missing = json.loads(payload.decode())
                log.info(f"Retransmitting {len(missing)} chunks")
                await self._send_chunks(writer, chunks, is_initial=False,
                                        seq_filter=set(missing))
                continue
            return

    async def run(self):
        async def cb(reader, writer):
            await self.handle_client(reader, writer)

        server = await asyncio.start_server(cb, self.host, self.port)
        workers = self.executor._max_workers
        log.info(f"Hybrid server on {self.host}:{self.port} "
                 f"({workers} CPU workers)")

        async with server:
            await server.serve_forever()


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_PORT
    srv = HybridServer(port=port)
    try:
        asyncio.run(srv.run())
    except KeyboardInterrupt:
        log.info("Shutting down.")
