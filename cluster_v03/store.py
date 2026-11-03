"""SQLite-backed authenticated worker registry and capacity-aware task leases.

v0.3 only schedules a bounded allowlist of demo jobs; it does not execute LLM
layers or accept executable Python from clients. Transactions serialize claims.
"""
from contextlib import contextmanager, closing
import hashlib
import hmac
import json
import math
from pathlib import Path
import secrets
import sqlite3
import time
import uuid


class LeaseConflict(Exception):
    pass


ALLOWED_KINDS = {"sum", "sha256", "matmul_checksum", "tp_gpt2"}


def validate_payload(kind, payload):
    if kind == "sum":
        nums = payload.get("numbers")
        if not isinstance(nums, list) or len(nums) > 10000 or any(
            isinstance(x, bool) or type(x) not in (int, float) or not math.isfinite(x) or abs(x) > 1e12
            for x in nums
        ):
            raise ValueError("sum expects <=10000 bounded real numbers")
    elif kind == "sha256":
        if not isinstance(payload.get("text"), str) or len(payload["text"]) > 100000:
            raise ValueError("sha256 expects text <=100000 characters")
    elif kind == "matmul_checksum":
        size = payload.get("size")
        if type(size) is not int or not 1 <= size <= 64:
            raise ValueError("matmul_checksum requires integer size 1..64")
    else:
        raise ValueError("unsupported task kind")
    if len(json.dumps(payload, ensure_ascii=False)) > 200000:
        raise ValueError("task payload too large")


class ClusterStore:
    def __init__(self, path, *, clock=None, heartbeat_ttl=30.0, lease_seconds=25.0):
        self.path = str(path)
        self.clock = clock or time.time
        self.heartbeat_ttl = heartbeat_ttl
        self.lease_seconds = lease_seconds
        if heartbeat_ttl <= 0 or lease_seconds <= 0:
            raise ValueError("timeouts must be positive")
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with closing(self._db()) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS nodes (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    token_hash TEXT NOT NULL,
                    platform TEXT NOT NULL,
                    cpu_cores INTEGER NOT NULL,
                    ram_mb INTEGER NOT NULL,
                    free_ram_mb INTEGER NOT NULL,
                    cpu_load_pct REAL NOT NULL,
                    compute_score REAL NOT NULL,
                    slots INTEGER NOT NULL,
                    capabilities TEXT NOT NULL,
                    last_seen REAL NOT NULL,
                    created REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    required_cpu INTEGER NOT NULL,
                    required_memory_mb INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    assigned_node TEXT,
                    lease_token TEXT,
                    lease_until REAL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 3,
                    result_json TEXT,
                    error TEXT,
                    created REAL NOT NULL,
                    updated REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS tasks_status_created ON tasks(status, created);
                CREATE INDEX IF NOT EXISTS tasks_assigned_status ON tasks(assigned_node, status);
            """)

    def _db(self):
        conn = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=10000")
        return conn

    @contextmanager
    def _tx(self):
        conn = self._db()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def register(self, *, name, platform, cpu_cores, ram_mb, compute_score,
                 slots, capabilities):
        if slots > cpu_cores:
            raise ValueError("slots cannot exceed cpu_cores")
        if not set(capabilities) <= ALLOWED_KINDS or not capabilities:
            raise ValueError("unknown or empty capabilities")
        node_id = uuid.uuid4().hex
        token = secrets.token_urlsafe(32)
        now = self.clock()
        with self._tx() as conn:
            conn.execute("""INSERT INTO nodes VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                         (node_id, name, hashlib.sha256(token.encode()).hexdigest(),
                          platform, cpu_cores, ram_mb, ram_mb, 0.0, compute_score,
                          slots, json.dumps(sorted(set(capabilities))), now, now))
        return {"id": node_id, "node_token": token, "heartbeat_seconds": 5,
                "lease_seconds": self.lease_seconds}

    @staticmethod
    def _require_node(conn, node_id, token):
        row = conn.execute("SELECT * FROM nodes WHERE id=?", (node_id,)).fetchone()
        if row is None or not token or not hmac.compare_digest(
                row["token_hash"], hashlib.sha256(token.encode()).hexdigest()):
            raise PermissionError("invalid node credentials")
        return row

    def heartbeat(self, node_id, token, *, free_ram_mb, cpu_load_pct):
        with self._tx() as conn:
            node = self._require_node(conn, node_id, token)
            if free_ram_mb > node["ram_mb"]:
                raise ValueError("free RAM cannot exceed total RAM")
            conn.execute("""UPDATE nodes SET last_seen=?,free_ram_mb=?,cpu_load_pct=?
                            WHERE id=?""",
                         (self.clock(), free_ram_mb, cpu_load_pct, node_id))
        return {"ok": True}

    def submit(self, kind, payload, *, required_cpu=1, required_memory_mb=64,
               max_attempts=3):
        validate_payload(kind, payload)
        now = self.clock()
        task_id = uuid.uuid4().hex
        with self._tx() as conn:
            conn.execute("""INSERT INTO tasks(id,kind,payload_json,required_cpu,
                            required_memory_mb,status,attempts,max_attempts,created,updated)
                            VALUES(?,?,?,?,?,'queued',0,?,?,?)""",
                         (task_id, kind, json.dumps(payload), required_cpu,
                          required_memory_mb, max_attempts, now, now))
        return {"id": task_id, "status": "queued"}

    def _reap(self, conn, now):
        # Expiry or offline nodes release their reservation. Attempts count
        # starts, not retries; exhausted tasks are terminally failed.
        conn.execute("""
            UPDATE tasks SET status=CASE WHEN attempts>=max_attempts THEN 'failed'
                              ELSE 'queued' END,
                assigned_node=NULL,lease_token=NULL,lease_until=NULL,
                error=CASE WHEN attempts>=max_attempts THEN 'lease expired or worker offline'
                           ELSE error END,updated=?
            WHERE status='leased' AND (
                lease_until < ? OR assigned_node IN (
                    SELECT id FROM nodes WHERE last_seen < ?))
        """, (now, now, now - self.heartbeat_ttl))

    @staticmethod
    def _active_reservations(conn, now):
        rows = conn.execute("""SELECT assigned_node,COUNT(*) AS running,
                                SUM(required_cpu) AS cpu,SUM(required_memory_mb) AS ram
                                FROM tasks WHERE status='leased' AND lease_until>=?
                                GROUP BY assigned_node""", (now,)).fetchall()
        return {r["assigned_node"]: {"slots": r["running"],
                "cpu": r["cpu"], "ram": r["ram"]} for r in rows}

    def _best_node_for(self, job, nodes, active):
        best = None
        for node in nodes:
            caps = json.loads(node["capabilities"])
            if job["kind"] not in caps:
                continue
            reserve = active.get(node["id"], {"slots": 0, "cpu": 0, "ram": 0})
            free_cores = node["cpu_cores"] - reserve["cpu"]
            free_memory = node["free_ram_mb"] - reserve["ram"]
            if (reserve["slots"] >= node["slots"] or
                    free_cores < job["required_cpu"] or
                    free_memory < job["required_memory_mb"]):
                continue
            # Advertised score is an untrusted scheduling hint, not a verified
            # benchmark. Busy nodes degrade in priority; a full slot excludes.
            score = node["compute_score"] * max(0.05, 1 - node["cpu_load_pct"] / 100)
            score *= max(0.1, free_cores / node["cpu_cores"])
            candidate = (score, free_memory, node["id"])
            if best is None or candidate > best[0]:
                best = (candidate, node)
        return best[1] if best is not None else None

    def claim(self, node_id, token):
        with self._tx() as conn:
            requester = self._require_node(conn, node_id, token)
            now = self.clock()
            self._reap(conn, now)
            if requester["last_seen"] < now - self.heartbeat_ttl:
                raise LeaseConflict("worker offline; heartbeat first")
            nodes = conn.execute("SELECT * FROM nodes WHERE last_seen>=?",
                                 (now - self.heartbeat_ttl,)).fetchall()
            active = self._active_reservations(conn, now)
            queued = conn.execute("SELECT * FROM tasks WHERE status='queued' "
                                  "ORDER BY created,id LIMIT 100").fetchall()
            for job in queued:
                winner = self._best_node_for(job, nodes, active)
                if winner is None or winner["id"] != node_id:
                    continue
                lease_token = secrets.token_urlsafe(24)
                deadline = now + self.lease_seconds
                conn.execute("""UPDATE tasks SET status='leased',assigned_node=?,
                                lease_token=?,lease_until=?,attempts=attempts+1,
                                updated=? WHERE id=? AND status='queued'""",
                             (node_id, lease_token, deadline, now, job["id"]))
                return {"task": {"id": job["id"], "kind": job["kind"],
                        "payload": json.loads(job["payload_json"]),
                        "lease_token": lease_token, "lease_until": deadline}}
        return {"task": None}

    def complete(self, node_id, token, task_id, lease_token, result, *, error=None):
        if len(json.dumps(result, ensure_ascii=False)) > 200000:
            raise ValueError("result too large")
        with self._tx() as conn:
            self._require_node(conn, node_id, token)
            now = self.clock()
            self._reap(conn, now)
            job = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
            if (job is None or job["status"] != "leased" or
                    job["assigned_node"] != node_id or not lease_token or
                    not hmac.compare_digest(job["lease_token"] or "", lease_token) or
                    job["lease_until"] < now):
                raise LeaseConflict("invalid, expired or already completed lease")
            if error is None:
                status = "completed"
            else:
                status = "failed" if job["attempts"] >= job["max_attempts"] else "queued"
            conn.execute("""UPDATE tasks SET status=?,result_json=?,error=?,
                            lease_token=NULL,lease_until=NULL,
                            assigned_node=CASE WHEN ?='completed' THEN assigned_node ELSE NULL END,
                            updated=? WHERE id=?""",
                         (status, json.dumps(result) if error is None else None,
                          error, status, now, task_id))
            return {"ok": True, "status": status}

    def status(self):
        with self._tx() as conn:
            now = self.clock()
            self._reap(conn, now)
            nodes = [dict(row) for row in conn.execute("""SELECT id,name,platform,
                      cpu_cores,ram_mb,free_ram_mb,cpu_load_pct,compute_score,
                      slots,capabilities,last_seen FROM nodes ORDER BY name,id""")]
            for node in nodes:
                node["online"] = node["last_seen"] >= now - self.heartbeat_ttl
                node["capabilities"] = json.loads(node["capabilities"])
            tasks = [dict(row) for row in conn.execute("""SELECT id,kind,status,
                     required_cpu,required_memory_mb,assigned_node,attempts,
                     max_attempts,result_json,error,created,updated
                     FROM tasks ORDER BY created DESC,id DESC LIMIT 100""")]
            for task in tasks:
                task["result"] = json.loads(task.pop("result_json")) if task["result_json"] else None
            counts = {r["status"]: r["n"] for r in conn.execute(
                "SELECT status,COUNT(*) AS n FROM tasks GROUP BY status")}
        return {"nodes": nodes, "tasks": tasks,
                "counts": {kind: counts.get(kind, 0) for kind in (
                    "queued", "leased", "completed", "failed")},
                "heartbeat_ttl_seconds": self.heartbeat_ttl,
                "lease_seconds": self.lease_seconds}
