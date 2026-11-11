"""Automatically admit public verification Workers without issuing trusted node identities."""
from collections import deque
from contextlib import closing, contextmanager
import hashlib
import hmac
import re
import secrets
import sqlite3
from threading import Lock

from public_pool.worker import MATRIX_SIZE, matrix_parameters


CHALLENGE_TTL = 60
HEARTBEAT_SECONDS = 20
OFFLINE_TTL = 90
MAX_WORKERS = 64
MAX_WORKERS_PER_SOURCE = 12
HEX_32 = re.compile(r"[0-9a-f]{32}")
HEX_64 = re.compile(r"[0-9a-f]{64}")
TOKEN = re.compile(r"[A-Za-z0-9_-]{43}")


class AdmissionError(Exception):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


def expected_checksum(seed):
    a, b, c, d = matrix_parameters(seed)
    n = MATRIX_SIZE
    # Sum the product via column sums of A and row sums of B, in O(n²).
    return sum(
        sum(((i * n + k) * a + b) % 31 for i in range(n))
        * sum(((k * n + j) * c + d) % 31 for j in range(n))
        for k in range(n)
    )


class PublicPool:
    def __init__(self, store):
        self.store = store
        self.clock = store.clock
        self.path = store.path + ".public.sqlite"
        self.rate_lock = Lock()
        self.rate_windows = {action: deque() for action in ("challenge", "join", "heartbeat")}
        self.source_windows = {}
        with self._db() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
        with self._db(write=True) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS public_workers (
                    id TEXT PRIMARY KEY, token_hash TEXT NOT NULL, resource TEXT NOT NULL,
                    cores INTEGER NOT NULL, ram_mb INTEGER NOT NULL, vram_mb INTEGER NOT NULL,
                    seed TEXT NOT NULL, previous_seed TEXT, source_hash TEXT, last_seen REAL NOT NULL,
                    checks INTEGER NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS public_config (
                    id INTEGER PRIMARY KEY CHECK (id=1), signing_key BLOB NOT NULL
                )
            """)
            conn.execute("INSERT OR IGNORE INTO public_config VALUES(1,?)", (secrets.token_bytes(32),))
            self.key = conn.execute("SELECT signing_key FROM public_config WHERE id=1").fetchone()[0]

    @contextmanager
    def _db(self, *, write=False):
        with closing(sqlite3.connect(self.path, timeout=2)) as conn, conn:
            conn.row_factory = sqlite3.Row
            if write:
                conn.execute("BEGIN IMMEDIATE")
            yield conn

    def _rate(self, action, maximum, now, source):
        with self.rate_lock:
            events = self.rate_windows[action]
            while events and events[0] <= now - 60:
                events.popleft()
            if len(events) >= maximum:
                raise AdmissionError(429, "Public pool is busy. Retry shortly.")
            if source is not None:
                for key, bucket in tuple(self.source_windows.items()):
                    if bucket[-1] <= now - 60:
                        del self.source_windows[key]
                bucket = self.source_windows.setdefault((action, source), deque())
                while bucket and bucket[0] <= now - 60:
                    bucket.popleft()
                if len(bucket) >= 12:
                    raise AdmissionError(429, "Too many requests from this source. Retry shortly.")
                bucket.append(now)
            events.append(now)

    def _signature(self, seed, resource, expires):
        return hmac.new(self.key, f"{seed}:{resource}:{expires}".encode(), hashlib.sha256).hexdigest()

    def challenge(self, resource, source=None):
        now = self.clock()
        self._rate("challenge", 60, now, source)
        if resource not in ("cpu", "cuda"):
            raise AdmissionError(422, "Choose CPU or CUDA")
        seed = secrets.token_hex(16)
        expires = int(now) + CHALLENGE_TTL
        return {"seed": seed, "resource": resource, "expires": expires,
                "signature": self._signature(seed, resource, expires)}

    def join(self, challenge, resources, checksum, source=None):
        now = self.clock()
        self._rate("join", 60, now, source)
        seed, resource, expires, signature = (challenge[key] for key in (
            "seed", "resource", "expires", "signature"))
        if (not HEX_32.fullmatch(seed) or resource not in ("cpu", "cuda")
                or not HEX_64.fullmatch(signature) or not now < expires <= now + CHALLENGE_TTL
                or not hmac.compare_digest(signature, self._signature(seed, resource, expires))):
            raise AdmissionError(422, "Invalid or expired verification challenge")
        cores, ram, vram = (resources[key] for key in ("cores", "ram_mb", "vram_mb"))
        if not (2 <= cores <= 1024 and 2048 <= ram <= 4194304):
            raise AdmissionError(422, "CPU admission requires at least 2 cores and 2 GiB RAM")
        if (resource == "cuda" and not 4096 <= vram <= 1048576) or (resource == "cpu" and vram != 0):
            raise AdmissionError(422, "CUDA admission requires at least 4 GiB VRAM")
        if checksum != expected_checksum(seed):
            raise AdmissionError(422, "Verification workload returned the wrong result")
        token, next_seed = secrets.token_urlsafe(32), secrets.token_hex(16)
        source_hash = hmac.digest(self.key, b"source:" + source.encode(), "sha256").hex() if source else None
        with self._db(write=True) as conn:
            now = self.clock()
            if expires <= now:
                raise AdmissionError(422, "Verification challenge expired")
            conn.execute("DELETE FROM public_workers WHERE last_seen<=?", (now - OFFLINE_TTL,))
            if conn.execute("SELECT 1 FROM public_workers WHERE id=?", (seed,)).fetchone():
                raise AdmissionError(409, "Verification challenge has already been used")
            if conn.execute("SELECT COUNT(*) FROM public_workers").fetchone()[0] >= MAX_WORKERS:
                raise AdmissionError(429, "Public pool is at capacity. Retry shortly.")
            if source_hash and conn.execute("""
                SELECT COUNT(*) FROM public_workers WHERE source_hash=?
            """, (source_hash,)).fetchone()[0] >= MAX_WORKERS_PER_SOURCE:
                raise AdmissionError(429, "Too many online Workers from this source.")
            conn.execute("""
                INSERT INTO public_workers(
                    id,token_hash,resource,cores,ram_mb,vram_mb,seed,source_hash,last_seen,checks
                ) VALUES(?,?,?,?,?,?,?,?,?,1)
            """, (seed, hashlib.sha256(token.encode()).hexdigest(), resource, cores, ram,
                  vram, next_seed, source_hash, now))
        if resource == "cuda":
            try:
                self.store.register_public_gpu(
                    seed, token, name=f"public-gpu-{seed[:8]}", platform="unknown",
                    cpu_cores=cores, ram_mb=ram,
                )
            except (ValueError, PermissionError, sqlite3.Error) as exc:
                with self._db(write=True) as conn:
                    conn.execute("DELETE FROM public_workers WHERE id=?", (seed,))
                raise AdmissionError(409, "Could not register public GPU Worker identity") from exc
        return {"node_id": seed, "token": token, "seed": next_seed,
                "heartbeat_seconds": HEARTBEAT_SECONDS}

    def heartbeat(self, node_id, token, seed, checksum):
        if not TOKEN.fullmatch(token):
            raise AdmissionError(401, "Invalid public Worker token")
        with self._db() as conn:
            worker = conn.execute("SELECT * FROM public_workers WHERE id=?", (node_id,)).fetchone()
        now = self.clock()
        if (worker is None or worker["last_seen"] <= now - OFFLINE_TTL
                or not hmac.compare_digest(worker["token_hash"], hashlib.sha256(token.encode()).hexdigest())):
            raise AdmissionError(401, "Public Worker session expired or invalid")
        self._rate("heartbeat", 600, now, node_id)
        if seed not in (worker["seed"], worker["previous_seed"]):
            raise AdmissionError(409, "Verification challenge has changed")
        if checksum != expected_checksum(seed):
            raise AdmissionError(422, "Verification workload returned the wrong result")
        with self._db(write=True) as conn:
            now = self.clock()
            worker = conn.execute("SELECT * FROM public_workers WHERE id=?", (node_id,)).fetchone()
            if worker is None or worker["last_seen"] <= now - OFFLINE_TTL:
                raise AdmissionError(401, "Public Worker session expired or invalid")
            if seed == worker["previous_seed"]:
                next_seed = worker["seed"]
                conn.execute("UPDATE public_workers SET last_seen=? WHERE id=?", (now, node_id))
            else:
                if seed != worker["seed"]:
                    raise AdmissionError(409, "Verification challenge has changed")
                if now - worker["last_seen"] < HEARTBEAT_SECONDS / 2:
                    raise AdmissionError(429, "Verification was sent too early")
                next_seed = secrets.token_hex(16)
                conn.execute("""
                    UPDATE public_workers
                    SET seed=?,previous_seed=?,last_seen=?,checks=checks+1 WHERE id=?
                """, (next_seed, seed, now, node_id))
        if worker["resource"] == "cuda":
            try:
                self.store.heartbeat_public_gpu(node_id, token)
            except PermissionError as exc:
                raise AdmissionError(401, "Public GPU Worker identity expired") from exc
        return {"seed": next_seed}

    def status(self):
        with self._db() as conn:
            row = conn.execute("""
                SELECT COUNT(CASE WHEN resource='cpu' THEN 1 END) AS cpu_workers,
                       COUNT(CASE WHEN resource='cuda' THEN 1 END) AS gpu_workers,
                       COALESCE(SUM(cores),0) AS cpu_cores,
                       COALESCE(SUM(vram_mb),0) AS gpu_vram_mb,
                       COALESCE(SUM(checks),0) AS verified_checks
                FROM public_workers WHERE last_seen>?
            """, (self.clock() - OFFLINE_TTL,)).fetchone()
        return dict(row)
