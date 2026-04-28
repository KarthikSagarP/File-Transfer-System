"""
Thread-safe LRU cache for processed file data.

Caches checksum and chunk lists keyed by file content hash,
so repeated transfers of the same file skip re-reading and re-splitting.
"""

import threading
import time
import logging
from collections import OrderedDict

log = logging.getLogger(__name__)


class FileCache:
    """
    LRU cache storing processed file data (checksum + chunks).

    Keys:   SHA-256 hash of file contents
    Values: {checksum, chunks, filename, cached_at, hits}

    Thread-safe via a reentrant lock.
    """

    def __init__(self, max_entries=64, ttl_seconds=300):
        self._max_entries = max_entries
        self._ttl = ttl_seconds
        self._cache = OrderedDict()  # key -> entry dict
        self._lock = threading.RLock()
        self._stats = {"hits": 0, "misses": 0, "evictions": 0}

    def get(self, content_hash):
        """
        Look up cached file data by content hash.

        Returns the cached entry dict or None on miss / expiry.
        """
        with self._lock:
            entry = self._cache.get(content_hash)
            if entry is None:
                self._stats["misses"] += 1
                return None

            # Check TTL expiry
            if time.time() - entry["cached_at"] > self._ttl:
                self._cache.pop(content_hash, None)
                self._stats["misses"] += 1
                self._stats["evictions"] += 1
                log.debug(f"Cache expired: {content_hash[:12]}...")
                return None

            # Move to end (most recently used)
            self._cache.move_to_end(content_hash)
            entry["hits"] += 1
            self._stats["hits"] += 1
            log.info(f"Cache HIT: {content_hash[:12]}... "
                     f"(hit #{entry['hits']})")
            return entry

    def put(self, content_hash, checksum, chunks, filename):
        """
        Store processed file data in the cache.

        Evicts the least recently used entry if at capacity.
        """
        with self._lock:
            # If already present, update and move to end
            if content_hash in self._cache:
                self._cache.move_to_end(content_hash)
                return

            # Evict LRU if at capacity
            while len(self._cache) >= self._max_entries:
                evicted_key, _ = self._cache.popitem(last=False)
                self._stats["evictions"] += 1
                log.debug(f"Cache evicted: {evicted_key[:12]}...")

            self._cache[content_hash] = {
                "checksum": checksum,
                "chunks": chunks,
                "filename": filename,
                "cached_at": time.time(),
                "hits": 0,
            }
            log.info(f"Cache STORE: {content_hash[:12]}... "
                     f"({len(self._cache)}/{self._max_entries} entries)")

    def invalidate(self, content_hash):
        """Remove a specific entry from the cache."""
        with self._lock:
            self._cache.pop(content_hash, None)

    def clear(self):
        """Flush the entire cache."""
        with self._lock:
            count = len(self._cache)
            self._cache.clear()
            log.info(f"Cache cleared ({count} entries removed)")

    def get_stats(self):
        """Return cache statistics as a dict."""
        with self._lock:
            total = self._stats["hits"] + self._stats["misses"]
            hit_rate = (self._stats["hits"] / total * 100) if total > 0 else 0
            return {
                "entries": len(self._cache),
                "max_entries": self._max_entries,
                "ttl_seconds": self._ttl,
                "hits": self._stats["hits"],
                "misses": self._stats["misses"],
                "evictions": self._stats["evictions"],
                "hit_rate_pct": round(hit_rate, 1),
            }
