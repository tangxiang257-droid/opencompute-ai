"""SQLite-backed authenticated worker registry and capacity-aware task leases.

v0.3 only schedules a bounded allowlist of demo jobs; it does not execute LLM
layers or accept executable Python from clients. Transactions serialize claims.
"""
from contextlib import contextmanager, closing
import hashlib
import hmac
import json
import math
import re
from pathlib import Path
import secrets
import sqlite3
import time
import uuid


class LeaseConflict(Exception):
    pass


ALLOWED_KINDS = {"sum", "sha256", "matmul_checksum", "tp_gpt2", "tp_qwen2"}


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
                    created REAL NOT NULL,
                    role TEXT NOT NULL DEFAULT 'trusted'
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
                CREATE TABLE IF NOT EXISTS gpu_devices (
                    node_id TEXT NOT NULL,
                    gpu_index INTEGER NOT NULL,
                    uuid TEXT NOT NULL,
                    name TEXT NOT NULL,
                    vendor TEXT NOT NULL,
                    backend TEXT NOT NULL,
                    total_memory_mb INTEGER NOT NULL,
                    free_memory_mb INTEGER NOT NULL,
                    utilization_pct REAL NOT NULL,
                    compute_capability TEXT,
                    supported_dtypes TEXT NOT NULL,
                    runtime TEXT NOT NULL DEFAULT 'unknown',
                    runtime_version TEXT,
                    qwen2_model_id TEXT,
                    qwen2_vram_mb INTEGER,
                    last_seen REAL NOT NULL,
                    PRIMARY KEY(node_id, gpu_index)
                );
                CREATE INDEX IF NOT EXISTS gpu_devices_node ON gpu_devices(node_id);
            """)
            node_columns = {row["name"] for row in conn.execute("PRAGMA table_info(nodes)")}
            if "role" not in node_columns:
                conn.execute("ALTER TABLE nodes ADD COLUMN role TEXT NOT NULL DEFAULT 'trusted'")
            gpu_columns = {row["name"] for row in conn.execute("PRAGMA table_info(gpu_devices)")}
            if "runtime" not in gpu_columns:
                conn.execute("ALTER TABLE gpu_devices ADD COLUMN runtime TEXT NOT NULL DEFAULT 'unknown'")
            if "runtime_version" not in gpu_columns:
                conn.execute("ALTER TABLE gpu_devices ADD COLUMN runtime_version TEXT")
            if "qwen2_model_id" not in gpu_columns:
                conn.execute("ALTER TABLE gpu_devices ADD COLUMN qwen2_model_id TEXT")
            if "qwen2_vram_mb" not in gpu_columns:
                conn.execute("ALTER TABLE gpu_devices ADD COLUMN qwen2_vram_mb INTEGER")

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
            conn.execute("""INSERT INTO nodes(
                id,name,token_hash,platform,cpu_cores,ram_mb,free_ram_mb,cpu_load_pct,
                compute_score,slots,capabilities,last_seen,created
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                         (node_id, name, hashlib.sha256(token.encode()).hexdigest(),
                          platform, cpu_cores, ram_mb, ram_mb, 0.0, compute_score,
                          slots, json.dumps(sorted(set(capabilities))), now, now))
        return {"id": node_id, "node_token": token, "heartbeat_seconds": 5,
                "lease_seconds": self.lease_seconds}

    @staticmethod
    def _valid_node_token(row, token):
        return bool(row is not None and token and hmac.compare_digest(
            row["token_hash"], hashlib.sha256(token.encode()).hexdigest()
        ))

    @classmethod
    def _require_node(cls, conn, node_id, token):
        row = conn.execute("SELECT * FROM nodes WHERE id=?", (node_id,)).fetchone()
        if not cls._valid_node_token(row, token) or row["role"] != "trusted":
            raise PermissionError("invalid node credentials")
        return row

    @classmethod
    def _require_gpu_node(cls, conn, node_id, token):
        row = conn.execute("SELECT * FROM nodes WHERE id=?", (node_id,)).fetchone()
        if not cls._valid_node_token(row, token) or row["role"] not in ("trusted", "public_gpu"):
            raise PermissionError("invalid GPU Worker credentials")
        return row

    def register_public_gpu(self, node_id, token, *, name, platform, cpu_cores, ram_mb):
        """Register a public GPU Worker with GPU-only credentials and no CPU-task rights."""
        if (not isinstance(node_id, str) or len(node_id) != 32
                or any(ch not in "0123456789abcdef" for ch in node_id)
                or not isinstance(token, str) or not token or len(token) > 128):
            raise ValueError("invalid public GPU Worker identity")
        if (type(cpu_cores) is not int or not 2 <= cpu_cores <= 1024
                or type(ram_mb) is not int or not 2048 <= ram_mb <= 4194304):
            raise ValueError("invalid public GPU Worker resources")
        now = self.clock()
        with self._tx() as conn:
            conn.execute("""INSERT INTO nodes(
                id,name,token_hash,platform,cpu_cores,ram_mb,free_ram_mb,cpu_load_pct,
                compute_score,slots,capabilities,last_seen,created,role
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,'public_gpu')""",
                (node_id, name[:128], hashlib.sha256(token.encode()).hexdigest(),
                 platform[:32], cpu_cores, ram_mb, ram_mb, 0.0, 1.0, 1, "[]", now, now))
        return {"id": node_id, "heartbeat_seconds": 20,
                "lease_seconds": self.lease_seconds}

    def heartbeat_public_gpu(self, node_id, token):
        """Refresh only the GPU scheduling liveness of a public GPU Worker."""
        with self._tx() as conn:
            node = self._require_gpu_node(conn, node_id, token)
            if node["role"] != "public_gpu":
                raise PermissionError("not a public GPU Worker")
            conn.execute("UPDATE nodes SET last_seen=? WHERE id=?", (self.clock(), node_id))
        return {"ok": True}

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
        """Account for BOTH ordinary task leases and managed TP process groups.

        Called inside BEGIN IMMEDIATE, so TP allocations and ordinary claims
        compete for the same capacity atomically.
        """
        rows = conn.execute("""SELECT assigned_node,COUNT(*) AS running,
                                SUM(required_cpu) AS cpu,SUM(required_memory_mb) AS ram
                                FROM tasks WHERE status='leased' AND lease_until>=?
                                GROUP BY assigned_node""", (now,)).fetchall()
        active = {r["assigned_node"]: {"slots": r["running"],
                  "cpu": r["cpu"], "ram": r["ram"]} for r in rows}
        schema = {row["name"] for row in conn.execute(
            "PRAGMA table_info(tp_assignments)")}
        if {"reserved_cpu", "reserved_memory_mb"} <= schema:
            sessions = conn.execute("""SELECT a.node_id,COUNT(*) AS running,
                     SUM(a.reserved_cpu) AS cpu,SUM(a.reserved_memory_mb) AS ram
                     FROM tp_assignments a JOIN tp_sessions s ON s.id=a.session_id
                     WHERE (s.state IN ('preparing','running') AND s.deadline>=?
                            AND a.state IN ('assigned','ready','done'))
                        OR (s.state='failed' AND s.updated+180>=?
                            AND a.state IN ('assigned','ready'))
                     GROUP BY a.node_id""", (now, now)).fetchall()
            for row in sessions:
                reserve = active.setdefault(row["node_id"],
                                             {"slots": 0, "cpu": 0, "ram": 0})
                reserve["slots"] += row["running"]
                reserve["cpu"] += row["cpu"]
                reserve["ram"] += row["ram"]
        return active

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

    @staticmethod
    def _task_detail(conn, task_id):
        row = conn.execute("""SELECT id,kind,payload_json,status,required_cpu,
            required_memory_mb,assigned_node,attempts,max_attempts,
            result_json,error,created,updated FROM tasks WHERE id=?""",
            (task_id,)).fetchone()
        if row is None:
            raise KeyError(task_id)
        task = dict(row)
        task["payload"] = json.loads(task.pop("payload_json"))
        result = task.pop("result_json")
        task["result"] = json.loads(result) if result is not None else None
        return task

    def task_detail(self, task_id):
        with self._tx() as conn:
            self._reap(conn, self.clock())
            return self._task_detail(conn, task_id)

    def cancel(self, task_id):
        """Invalidate a lease; the worker may finish CPU work but cannot commit it."""
        with self._tx() as conn:
            now = self.clock()
            self._reap(conn, now)
            task = self._task_detail(conn, task_id)
            if task["status"] not in ("queued", "leased"):
                raise LeaseConflict("only queued or leased tasks can be cancelled")
            conn.execute("""UPDATE tasks SET status='cancelled',assigned_node=NULL,
                lease_token=NULL,lease_until=NULL,result_json=NULL,
                error='cancelled by administrator',updated=? WHERE id=?""", (now, task_id))
            return self._task_detail(conn, task_id)

    def retry(self, task_id):
        """Reset only failed/cancelled work, serialized against claims/completions."""
        with self._tx() as conn:
            self._reap(conn, self.clock())
            task = self._task_detail(conn, task_id)
            if task["status"] not in ("failed", "cancelled"):
                raise LeaseConflict("only failed or cancelled tasks can be retried")
            conn.execute("""UPDATE tasks SET status='queued',assigned_node=NULL,
                lease_token=NULL,lease_until=NULL,result_json=NULL,error=NULL,
                attempts=0,updated=? WHERE id=?""", (self.clock(), task_id))
            return self._task_detail(conn, task_id)

    def report_gpu_inventory(self, node_id, token, devices):
        """Replace a node's reported GPU inventory after authenticating its node token.

        GPU facts are device-reported telemetry, not cryptographic hardware attestation.
        """
        if not isinstance(devices, list) or len(devices) > 16:
            raise ValueError("GPU inventory must contain at most 16 devices")
        seen_indices, seen_uuids = set(), set()
        for gpu in devices:
            index = gpu.get("index")
            if type(index) is not int or not 0 <= index < 16 or index in seen_indices:
                raise ValueError("GPU indices must be unique integers in range 0..15")
            seen_indices.add(index)
            identifier = gpu.get("uuid")
            if not isinstance(identifier, str) or not identifier or len(identifier) > 128:
                raise ValueError("GPU UUID must be a non-empty string of at most 128 characters")
            if identifier in seen_uuids:
                raise ValueError("GPU UUIDs must be unique within a node")
            seen_uuids.add(identifier)
            if not isinstance(gpu.get("name"), str) or not 1 <= len(gpu["name"]) <= 128:
                raise ValueError("GPU name must contain 1..128 characters")
            if gpu.get("vendor") not in ("nvidia", "amd", "intel", "unknown"):
                raise ValueError("unsupported GPU vendor")
            if gpu.get("backend") not in ("cuda", "rocm", "xpu", "unknown"):
                raise ValueError("unsupported GPU backend")
            total, free = gpu.get("total_memory_mb"), gpu.get("free_memory_mb")
            if type(total) is not int or not 1 <= total <= 1048576:
                raise ValueError("invalid total GPU memory")
            if type(free) is not int or not 0 <= free <= total:
                raise ValueError("invalid free GPU memory")
            utilization = gpu.get("utilization_pct")
            if (isinstance(utilization, bool) or not isinstance(utilization, (int, float))
                    or not math.isfinite(utilization) or not 0 <= utilization <= 100):
                raise ValueError("invalid GPU utilization")
            capability = gpu.get("compute_capability")
            if capability is not None and (not isinstance(capability, str) or len(capability) > 16):
                raise ValueError("invalid GPU compute capability")
            dtypes = gpu.get("supported_dtypes", [])
            if (not isinstance(dtypes, list) or len(dtypes) > 6
                    or any(item not in ("fp32", "fp16", "bf16", "tf32", "int8", "int4")
                           for item in dtypes)):
                raise ValueError("invalid GPU precision capabilities")
            runtime = gpu.get("runtime", "unknown")
            if runtime not in ("unknown", "pytorch_cuda"):
                raise ValueError("unsupported GPU runtime")
            runtime_version = gpu.get("runtime_version")
            if runtime_version is not None and (not isinstance(runtime_version, str) or len(runtime_version) > 32):
                raise ValueError("invalid GPU runtime version")
            model_id, model_vram = gpu.get("qwen2_model_id"), gpu.get("qwen2_vram_mb")
            if model_id is not None or model_vram is not None:
                if (not isinstance(model_id, str) or not re.fullmatch(r"[0-9a-f]{64}", model_id)
                        or type(model_vram) is not int or not 64 <= model_vram <= total
                        or (gpu["vendor"], gpu["backend"], runtime)
                        != ("nvidia", "cuda", "pytorch_cuda")):
                    raise ValueError("invalid Qwen2 GPU model capability")

        now = self.clock()
        with self._tx() as conn:
            self._require_gpu_node(conn, node_id, token)
            conn.execute("DELETE FROM gpu_devices WHERE node_id=?", (node_id,))
            for gpu in devices:
                conn.execute(
                    """INSERT INTO gpu_devices(
                        node_id,gpu_index,uuid,name,vendor,backend,total_memory_mb,
                        free_memory_mb,utilization_pct,compute_capability,
                        supported_dtypes,runtime,runtime_version,qwen2_model_id,qwen2_vram_mb,last_seen
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (node_id, gpu["index"], gpu["uuid"], gpu["name"], gpu["vendor"],
                     gpu["backend"], gpu["total_memory_mb"], gpu["free_memory_mb"],
                     float(gpu["utilization_pct"]), gpu.get("compute_capability"),
                     json.dumps(sorted(set(gpu.get("supported_dtypes", [])))),
                     gpu.get("runtime", "unknown"), gpu.get("runtime_version"),
                     gpu.get("qwen2_model_id"), gpu.get("qwen2_vram_mb"), now),
                )
        return {"ok": True, "reported": len(devices), "reported_at": now}

    def gpu_status(self):
        """Return GPU inventory and aggregate online capacity without exposing node tokens."""
        with self._tx() as conn:
            now = self.clock()
            rows = conn.execute(
                """SELECT g.node_id,g.gpu_index,g.uuid,g.name,g.vendor,g.backend,
                          g.total_memory_mb,g.free_memory_mb,g.utilization_pct,
                          g.compute_capability,g.supported_dtypes,g.runtime,
                          g.runtime_version,g.qwen2_model_id,g.qwen2_vram_mb,g.last_seen,
                          n.name AS node_name,n.role AS node_role,n.last_seen AS node_last_seen
                   FROM gpu_devices g JOIN nodes n ON n.id=g.node_id
                   ORDER BY n.name,g.gpu_index"""
            ).fetchall()
            devices = []
            for row in rows:
                item = dict(row)
                item["supported_dtypes"] = json.loads(item["supported_dtypes"])
                item["online"] = (
                    item["node_last_seen"] >= now - self.heartbeat_ttl
                    and item["last_seen"] >= now - max(90.0, self.heartbeat_ttl * 3)
                )
                item["telemetry_age_seconds"] = max(0, int(now - item["last_seen"]))
                item["verification"] = "self_reported"
                item.pop("node_last_seen", None)
                devices.append(item)
            online = [item for item in devices if item["online"]]
            summary = {
                "registered": len(devices),
                "online": len(online),
                "offline": len(devices) - len(online),
                "total_vram_mb": sum(item["total_memory_mb"] for item in online),
                "free_vram_mb": sum(item["free_memory_mb"] for item in online),
                "vendors": {
                    vendor: sum(1 for item in online if item["vendor"] == vendor)
                    for vendor in ("nvidia", "amd", "intel", "unknown")
                },
            }
        return {"devices": devices, "summary": summary,
                "heartbeat_ttl_seconds": self.heartbeat_ttl}

    def status(self):
        with self._tx() as conn:
            now = self.clock()
            self._reap(conn, now)
            nodes = [dict(row) for row in conn.execute("""SELECT id,name,platform,role,
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
                    "queued", "leased", "completed", "failed", "cancelled")},
                "heartbeat_ttl_seconds": self.heartbeat_ttl,
                "lease_seconds": self.lease_seconds}
