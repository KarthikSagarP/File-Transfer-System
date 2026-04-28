"""
Async file transfer server using asyncio.

Same protocol and features as the threaded server, but uses
asyncio streams instead of threads for concurrent client handling.
CPU-bound work (checksum, file splitting) is offloaded to a thread pool
via loop.run_in_executor().

Usage:
    python server_async.py [port]
"""

import asyncio
import os
import sys
import json
import random
import time
import logging
import hashlib
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

import struct

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ASYNC-SERVER] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# Thread pool for CPU-bound operations
_executor = ThreadPoolExecutor(max_workers=4)


# ── Async protocol helpers ───────────────────────────────────────────────────

async def async_recv_exactly(reader, n):
    """Read exactly n bytes from an asyncio StreamReader."""
    data = bytearray()
    while len(data) < n:
        chunk = await reader.read(n - len(data))
        if not chunk:
            raise ConnectionError("Connection closed")
        data.extend(chunk)
    return bytes(data)


async def async_recv_message(reader):
    """Receive a protocol message from an asyncio StreamReader."""
    header = await async_recv_exactly(reader, HEADER_SIZE)
    msg_type, seq_num, payload_len = struct.unpack(HEADER_FORMAT, header)
    payload = await async_recv_exactly(reader, payload_len) if payload_len > 0 else b""
    return msg_type, seq_num, payload


def pack_message(msg_type, seq_num=0, payload=b""):
    """Pack a protocol message into bytes."""
    header = struct.pack(HEADER_FORMAT, msg_type, seq_num, len(payload))
    return header + payload


async def async_send_message(writer, msg_type, seq_num=0, payload=b""):
    """Send a protocol message over an asyncio StreamWriter."""
    writer.write(pack_message(msg_type, seq_num, payload))
    await writer.drain()


# ── Server state ─────────────────────────────────────────────────────────────

class AsyncServerState:
    """Server state for the async server."""

    def __init__(self, storage_dir="server_storage"):
        self.storage_dir = storage_dir
        os.makedirs(storage_dir, exist_ok=True)
        self.cache = FileCache(max_entries=64, ttl_seconds=300)
        self.config = {
            "drop_rate": 0.1,
            "corrupt_rate": 0.05,
            "shuffle_chunks": True,
            "chunk_size": CHUNK_SIZE,
            "max_retransmit_rounds": 10,
        }
        self._active_clients = 0
        self._total_transfers = 0
        self._total_bytes = 0
        self._start_time = time.time()

    def get_status(self):
        uptime = time.time() - self._start_time
        return {
            "uptime_seconds": round(uptime, 1),
            "active_clients": self._active_clients,
            "total_transfers": self._total_transfers,
            "total_bytes_transferred": self._total_bytes,
            "cache": self.cache.get_stats(),
            "config": self.config,
            "backend": "asyncio",
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


# ── Client handler ───────────────────────────────────────────────────────────

async def handle_client(reader, writer, state):
    """Handle a single async client session."""
    addr = writer.get_extra_info("peername")
    state._active_clients += 1
    log.info(f"Connected: {addr}")

    try:
        while True:
            try:
                msg_type, _, payload = await asyncio.wait_for(
                    async_recv_message(reader), timeout=30
                )
            except (ConnectionError, asyncio.TimeoutError):
                break

            try:
                if msg_type == MSG_UPLOAD_REQUEST:
                    await _handle_upload(reader, writer, payload, state)
                elif msg_type == MSG_DOWNLOAD_REQUEST:
                    await _handle_download(reader, writer, payload, state)
                elif msg_type == MSG_LIST_REQUEST:
                    files = state.list_files()
                    await async_send_message(
                        writer, MSG_LIST_RESPONSE,
                        payload=json.dumps(files).encode()
                    )
                elif msg_type == MSG_STATUS_REQUEST:
                    status = state.get_status()
                    await async_send_message(
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
                                _executor, handler, state)
                        else:
                            result = await loop.run_in_executor(
                                _executor, handler, state, query)
                        await async_send_message(
                            writer, MSG_QUERY_RESPONSE,
                            payload=json.dumps(result).encode()
                        )
                    else:
                        await async_send_message(
                            writer, MSG_ERROR,
                            payload=f"Unknown query: {qtype}".encode()
                        )
                else:
                    await async_send_message(
                        writer, MSG_ERROR,
                        payload=f"Unknown: {msg_type}".encode()
                    )
            except ConnectionError:
                break
            except Exception as e:
                log.error(f"Error: {e}", exc_info=True)
                try:
                    await async_send_message(
                        writer, MSG_ERROR, payload=str(e).encode()
                    )
                except Exception:
                    break

    except Exception as e:
        log.error(f"Session error: {e}", exc_info=True)
    finally:
        state._active_clients -= 1
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
        log.info(f"Disconnected: {addr}")


async def _handle_upload(reader, writer, payload, state):
    """Handle file upload asynchronously."""
    try:
        meta = json.loads(payload.decode())
    except Exception as e:
        await async_send_message(writer, MSG_ERROR,
                                 payload=f"Bad request: {e}".encode())
        return

    filename = os.path.basename(meta.get("filename", "unknown"))
    file_size = meta.get("file_size", 0)
    log.info(f"Upload: '{filename}' ({file_size} bytes)")

    await async_send_message(writer, MSG_ACK)

    file_data = await async_recv_exactly(reader, file_size)

    # Offload CPU work to thread pool
    loop = asyncio.get_event_loop()

    fpath = os.path.join(state.storage_dir, filename)
    with open(fpath, "wb") as f:
        f.write(file_data)

    content_hash = await loop.run_in_executor(
        _executor, compute_checksum_bytes, file_data
    )

    cached = state.cache.get(content_hash)
    if cached:
        checksum = cached["checksum"]
        chunks = cached["chunks"]
    else:
        checksum = content_hash
        chunks = await loop.run_in_executor(
            _executor, split_file, fpath, state.config["chunk_size"]
        )
        state.cache.put(content_hash, checksum, chunks, filename)

    total_chunks = len(chunks)
    meta_payload = build_meta_payload(filename, checksum, total_chunks)
    await async_send_message(writer, MSG_FILE_META, payload=meta_payload)

    await _send_chunks(writer, chunks, state.config, is_initial=True)
    await _retransmit_loop(reader, writer, chunks, state.config)

    state._total_transfers += 1
    state._total_bytes += file_size
    log.info(f"Transfer complete: '{filename}'")


async def _handle_download(reader, writer, payload, state):
    """Handle file download asynchronously."""
    filename = payload.decode().strip()
    fpath = os.path.join(state.storage_dir, os.path.basename(filename))

    if not os.path.isfile(fpath):
        await async_send_message(
            writer, MSG_ERROR,
            payload=f"Not found: {filename}".encode()
        )
        return

    loop = asyncio.get_event_loop()
    checksum = await loop.run_in_executor(_executor, compute_checksum, fpath)

    cached = state.cache.get(checksum)
    if cached:
        chunks = cached["chunks"]
    else:
        chunks = await loop.run_in_executor(
            _executor, split_file, fpath, state.config["chunk_size"]
        )
        state.cache.put(checksum, checksum, chunks, filename)

    total_chunks = len(chunks)
    meta_payload = build_meta_payload(filename, checksum, total_chunks)
    await async_send_message(writer, MSG_FILE_META, payload=meta_payload)

    await _send_chunks(writer, chunks, state.config, is_initial=True)
    await _retransmit_loop(reader, writer, chunks, state.config)

    state._total_transfers += 1
    state._total_bytes += os.path.getsize(fpath)


async def _send_chunks(writer, chunks, config, is_initial=True, seq_filter=None):
    """Send chunks asynchronously with CRC32 and corruption simulation."""
    to_send = chunks if seq_filter is None else [
        (s, d) for s, d in chunks if s in seq_filter
    ]

    if is_initial and config["shuffle_chunks"]:
        to_send = list(to_send)
        random.shuffle(to_send)

    drop_rate = config["drop_rate"]
    corrupt_rate = config.get("corrupt_rate", 0.0)
    for seq_num, data in to_send:
        if is_initial and random.random() < drop_rate:
            continue
        if is_initial and random.random() < corrupt_rate:
            corrupted = corrupt_data(data, num_bits=2)
            payload = corrupted + pack_chunk_with_crc(data)[-4:]
        else:
            payload = pack_chunk_with_crc(data)
        await async_send_message(writer, MSG_CHUNK, seq_num=seq_num,
                                 payload=payload)


async def _retransmit_loop(reader, writer, chunks, config):
    """Handle retransmission asynchronously."""
    max_rounds = config["max_retransmit_rounds"]

    for _ in range(max_rounds):
        await async_send_message(writer, MSG_TRANSFER_DONE)

        try:
            msg_type, _, payload = await asyncio.wait_for(
                async_recv_message(reader), timeout=30
            )
        except (ConnectionError, asyncio.TimeoutError):
            return

        if msg_type == MSG_ACK:
            log.info("Client ACK — success")
            return
        if msg_type == MSG_RETRANSMIT_REQ:
            try:
                missing = json.loads(payload.decode())
            except Exception:
                return
            log.info(f"Retransmitting {len(missing)} chunks")
            await _send_chunks(writer, chunks, config, is_initial=False,
                               seq_filter=set(missing))
            continue
        return


# ── Entry point ──────────────────────────────────────────────────────────────

async def run_server(host=DEFAULT_HOST, port=DEFAULT_PORT):
    state = AsyncServerState()

    async def client_cb(reader, writer):
        await handle_client(reader, writer, state)

    server = await asyncio.start_server(client_cb, host, port)
    log.info(f"Async server listening on {host}:{port}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_PORT
    try:
        asyncio.run(run_server(port=port))
    except KeyboardInterrupt:
        log.info("Shutting down.")
