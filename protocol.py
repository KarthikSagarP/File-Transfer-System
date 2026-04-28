"""
Wire protocol for the file transfer system.

Message format:
    [Header: 9 bytes] [Payload: variable]

Header layout (network byte order / big-endian):
    - msg_type   : 1 byte  (unsigned char)
    - seq_num    : 4 bytes (unsigned int)
    - payload_len: 4 bytes (unsigned int)

Message types:
    UPLOAD_REQUEST   (0x01): Client -> Server.  Payload = JSON {filename, file_size}.
    FILE_META        (0x02): Server -> Client.  Payload = JSON {checksum, total_chunks, filename}.
    CHUNK            (0x03): Server -> Client.  Payload = raw file bytes.
    ACK              (0x04): Either direction.   Payload = empty.
    RETRANSMIT_REQ   (0x05): Client -> Server.  Payload = JSON list of missing seq numbers.
    TRANSFER_DONE    (0x06): Server -> Client.  Payload = empty.
    LIST_REQUEST     (0x07): Client -> Server.  Payload = empty.
    LIST_RESPONSE    (0x08): Server -> Client.  Payload = JSON list of file info dicts.
    DOWNLOAD_REQUEST (0x09): Client -> Server.  Payload = filename (UTF-8).
    STATUS_REQUEST   (0x0A): Client -> Server.  Payload = empty.
    STATUS_RESPONSE  (0x0B): Server -> Client.  Payload = JSON server stats.
    ERROR            (0xFF): Either direction.   Payload = error message (UTF-8).
"""

import struct
import json
import hashlib
import zlib

# ── Constants ────────────────────────────────────────────────────────────────

HEADER_FORMAT = "!BII"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)  # 9 bytes
CHUNK_SIZE = 1024
CRC_SIZE = 4  # CRC32 appended to each chunk payload
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 9000

# ── Message types ────────────────────────────────────────────────────────────

MSG_UPLOAD_REQUEST   = 0x01
MSG_FILE_META        = 0x02
MSG_CHUNK            = 0x03
MSG_ACK              = 0x04
MSG_RETRANSMIT_REQ   = 0x05
MSG_TRANSFER_DONE    = 0x06
MSG_LIST_REQUEST     = 0x07
MSG_LIST_RESPONSE    = 0x08
MSG_DOWNLOAD_REQUEST = 0x09
MSG_STATUS_REQUEST   = 0x0A
MSG_STATUS_RESPONSE  = 0x0B
MSG_QUERY_REQUEST    = 0x0C  # Generic query: IndexGet, FileHash, etc.
MSG_QUERY_RESPONSE   = 0x0D
MSG_ERROR            = 0xFF

MSG_NAMES = {
    MSG_UPLOAD_REQUEST:   "UPLOAD_REQUEST",
    MSG_FILE_META:        "FILE_META",
    MSG_CHUNK:            "CHUNK",
    MSG_ACK:              "ACK",
    MSG_RETRANSMIT_REQ:   "RETRANSMIT_REQ",
    MSG_TRANSFER_DONE:    "TRANSFER_DONE",
    MSG_LIST_REQUEST:     "LIST_REQUEST",
    MSG_LIST_RESPONSE:    "LIST_RESPONSE",
    MSG_DOWNLOAD_REQUEST: "DOWNLOAD_REQUEST",
    MSG_STATUS_REQUEST:   "STATUS_REQUEST",
    MSG_STATUS_RESPONSE:  "STATUS_RESPONSE",
    MSG_QUERY_REQUEST:    "QUERY_REQUEST",
    MSG_QUERY_RESPONSE:   "QUERY_RESPONSE",
    MSG_ERROR:            "ERROR",
}

# ── Low-level send / recv ────────────────────────────────────────────────────

def send_message(sock, msg_type, seq_num=0, payload=b""):
    """Pack and send a single protocol message over a socket."""
    header = struct.pack(HEADER_FORMAT, msg_type, seq_num, len(payload))
    sock.sendall(header + payload)


def recv_exactly(sock, n):
    """Read exactly n bytes from a socket, raising on premature close."""
    data = bytearray()
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            raise ConnectionError("Connection closed while reading data")
        data.extend(packet)
    return bytes(data)


def recv_message(sock):
    """Receive a single protocol message. Returns (msg_type, seq_num, payload)."""
    header = recv_exactly(sock, HEADER_SIZE)
    msg_type, seq_num, payload_len = struct.unpack(HEADER_FORMAT, header)
    payload = recv_exactly(sock, payload_len) if payload_len > 0 else b""
    return msg_type, seq_num, payload


# ── Helpers ──────────────────────────────────────────────────────────────────

def compute_checksum(filepath):
    """Compute SHA-256 hex digest of a file."""
    sha = hashlib.sha256()
    with open(filepath, "rb") as f:
        while True:
            block = f.read(8192)
            if not block:
                break
            sha.update(block)
    return sha.hexdigest()


def compute_checksum_bytes(data: bytes) -> str:
    """Compute SHA-256 hex digest from raw bytes."""
    return hashlib.sha256(data).hexdigest()


def split_file(filepath, chunk_size=CHUNK_SIZE):
    """Split a file into a list of (seq_num, chunk_bytes) tuples."""
    chunks = []
    with open(filepath, "rb") as f:
        seq = 0
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            chunks.append((seq, chunk))
            seq += 1
    return chunks


def build_meta_payload(filename, checksum, total_chunks):
    """Build a JSON payload for FILE_META messages."""
    return json.dumps({
        "filename": filename,
        "checksum": checksum,
        "total_chunks": total_chunks,
    }).encode("utf-8")


def parse_meta_payload(payload):
    """Parse a FILE_META JSON payload. Returns dict."""
    return json.loads(payload.decode("utf-8"))


# ── Per-chunk CRC32 ─────────────────────────────────────────────────────────

def pack_chunk_with_crc(data):
    """
    Append a 4-byte CRC32 checksum to chunk data.
    
    Format: [raw_data][crc32 as 4 bytes big-endian unsigned]
    The CRC lets the receiver detect bit-level corruption in individual
    chunks without waiting for full-file SHA-256 verification.
    """
    crc = zlib.crc32(data) & 0xFFFFFFFF  # ensure unsigned
    return data + struct.pack("!I", crc)


def unpack_chunk_with_crc(payload):
    """
    Verify and strip the CRC32 from a chunk payload.
    
    Returns (data, is_valid) where is_valid is False if corruption detected.
    """
    if len(payload) < CRC_SIZE:
        return payload, False

    data = payload[:-CRC_SIZE]
    received_crc = struct.unpack("!I", payload[-CRC_SIZE:])[0]
    computed_crc = zlib.crc32(data) & 0xFFFFFFFF
    return data, (received_crc == computed_crc)


def corrupt_data(data, num_bits=1):
    """
    Flip random bits in data to simulate corruption.
    
    Used by the server's error simulator to produce chunks that arrive
    but contain incorrect data, testable via CRC32 verification.
    """
    import random
    data = bytearray(data)
    for _ in range(num_bits):
        byte_idx = random.randint(0, len(data) - 1)
        bit_idx = random.randint(0, 7)
        data[byte_idx] ^= (1 << bit_idx)
    return bytes(data)
