"""Automatically admit public verification Workers without issuing trusted node identities."""
from collections import deque
from contextlib import closing
import hashlib
import hmac
import re
import secrets
from threading import Lock

from public_pool.worker import MATRIX_SIZE, matrix_parameters


CHALLENGE_TTL = 60
HEARTBEAT_SECONDS = 20
OFFLINE_TTL = 90
MAX_WORKERS = 64
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
    def __init__(self, store, *, signing_secret):
        self.store = store
        self.key = hmac.digest(signing_secret.encode(), b"opencompute/public-admission/v1", "sha256")
        self.rate_lock = Lock()
        self.rate_windows = {action: deque() for action in ("challenge", "join", "heartbeat")}
        with store._tx() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS public_workers (
                    id TEXT PRIMARY KEY, token_hash TEXT NOT NULL, resource TEXT NOT NULL,
                    cores INTEGER NOT NULL, ram_mb INTEGER NOT NULL, vram_mb INTEGER NOT NULL,
                    seed TEXT NOT NULL, previous_seed TEXT, last_seen REAL NOT NULL,
                    checks INTEGER NOT NULL
                )
            """)

    def _rate(self, action, maximum, now):
        with self.rate_lock:
            events = self.rate_windows[action]
            while events and events[0] <= now - 60:
                events.popleft()
            if len(events) >= maximum:
                raise AdmissionError(429, "Public pool is busy. Retry shortly.")
            events.append(now)

    def _signature(self, seed, resource, expires):
        return hmac.new(self.key, f"{seed}:{resource}:{expires}".encode(), hashlib.sha256).hexdigest()

    def challenge(self, resource):
        now = self.store.clock()
        self._rate("challenge", 60, now)
        if resource not in ("cpu", "cuda"):
            raise AdmissionError(422, "Choose CPU or CUDA")
        seed = secrets.token_hex(16)
        expires = int(now) + CHALLENGE_TTL
        return {"seed": seed, "resource": resource, "expires": expires,
                "signature": self._signature(seed, resource, expires)}

    def join(self, challenge, resources, checksum):
        now = self.store.clock()
        self._rate("join", 60, now)
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
        with self.store._tx() as conn:
            conn.execute("DELETE FROM public_workers WHERE last_seen<=?", (now - OFFLINE_TTL,))
            if conn.execute("SELECT 1 FROM public_workers WHERE id=?", (seed,)).fetchone():
                raise AdmissionError(409, "Verification challenge has already been used")
            if conn.execute("SELECT COUNT(*) FROM public_workers").fetchone()[0] >= MAX_WORKERS:
                raise AdmissionError(429, "Public pool is at capacity. Retry shortly.")
            conn.execute("""
                INSERT INTO public_workers(id,token_hash,resource,cores,ram_mb,vram_mb,seed,last_seen,checks)
                VALUES(?,?,?,?,?,?,?,?,1)
            """, (seed, hashlib.sha256(token.encode()).hexdigest(), resource, cores, ram,
                  vram, next_seed, now))
        return {"node_id": seed, "token": token, "seed": next_seed,
                "heartbeat_seconds": HEARTBEAT_SECONDS}

    def heartbeat(self, node_id, token, seed, checksum):
        now = self.store.clock()
        self._rate("heartbeat", 600, now)
        if not TOKEN.fullmatch(token):
            raise AdmissionError(401, "Invalid public Worker token")
        with self.store._tx() as conn:
            worker = conn.execute("SELECT * FROM public_workers WHERE id=?", (node_id,)).fetchone()
            if (worker is None or worker["last_seen"] <= now - OFFLINE_TTL
                    or not hmac.compare_digest(worker["token_hash"], hashlib.sha256(token.encode()).hexdigest())):
                raise AdmissionError(401, "Public Worker session expired or invalid")
            if seed not in (worker["seed"], worker["previous_seed"]):
                raise AdmissionError(409, "Verification challenge has changed")
            if checksum != expected_checksum(seed):
                raise AdmissionError(422, "Verification workload returned the wrong result")
            if seed == worker["previous_seed"]:
                return {"seed": worker["seed"]}
            if now - worker["last_seen"] < HEARTBEAT_SECONDS / 2:
                raise AdmissionError(429, "Verification was sent too early")
            next_seed = secrets.token_hex(16)
            conn.execute("""
                UPDATE public_workers
                SET seed=?,previous_seed=?,last_seen=?,checks=checks+1 WHERE id=?
            """, (next_seed, seed, now, node_id))
        return {"seed": next_seed}

    def status(self):
        with closing(self.store._db()) as conn:
            row = conn.execute("""
                SELECT COUNT(CASE WHEN resource='cpu' THEN 1 END) AS cpu_workers,
                       COUNT(CASE WHEN resource='cuda' THEN 1 END) AS gpu_workers,
                       COALESCE(SUM(cores),0) AS cpu_cores,
                       COALESCE(SUM(vram_mb),0) AS gpu_vram_mb,
                       COALESCE(SUM(checks),0) AS verified_checks
                FROM public_workers WHERE last_seen>?
            """, (self.store.clock() - OFFLINE_TTL,)).fetchone()
        return dict(row)
