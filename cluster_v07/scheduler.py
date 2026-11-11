"""GPU task scheduling with per-device VRAM reservations and expiring leases.

This scheduler handles a deliberately small allowlist of GPU workloads. It does
not execute user-supplied code, load arbitrary model files, or claim that VRAM
reported by a Worker is trusted hardware attestation.
"""
import json
import math
import re
import secrets
import uuid

from cluster_v03.store import LeaseConflict


QWEN2_TASK = "qwen2_generate"
GPU_TASK_KINDS = {"matmul_checksum", QWEN2_TASK}
GPU_TASK_STATUSES = ("queued", "leased", "completed", "failed", "cancelled")


def validate_gpu_task(kind, payload, required_vram_mb):
    if kind not in GPU_TASK_KINDS:
        raise ValueError("unsupported GPU task kind")
    if type(required_vram_mb) is not int or not 64 <= required_vram_mb <= 1048576:
        raise ValueError("required_vram_mb must be an integer in range 64..1048576")
    if kind == QWEN2_TASK:
        if not isinstance(payload, dict) or set(payload) != {"model_id", "prompt", "max_new_tokens"}:
            raise ValueError("Qwen2 payload requires model_id, prompt and max_new_tokens")
        model_id, prompt, steps = payload["model_id"], payload["prompt"], payload["max_new_tokens"]
        if not isinstance(model_id, str) or not re.fullmatch(r"[0-9a-f]{64}", model_id):
            raise ValueError("invalid Qwen2 model identifier")
        if not isinstance(prompt, str) or not prompt.strip() or len(prompt) > 4096:
            raise ValueError("Qwen2 prompt must contain 1..4096 characters")
        try:
            prompt.encode("utf-8")
        except UnicodeError as exc:
            raise ValueError("Qwen2 prompt must be valid UTF-8") from exc
        if type(steps) is not int or not 1 <= steps <= 64:
            raise ValueError("max_new_tokens must be an integer in range 1..64")
        return None
    if not isinstance(payload, dict) or set(payload) - {"size"}:
        raise ValueError("GPU matmul payload accepts only the size field")
    size = payload.get("size")
    if type(size) is not int or not 16 <= size <= 512:
        raise ValueError("GPU matmul size must be an integer in range 16..512")
    estimated = 64 + math.ceil(3 * size * size * 4 / (1024 * 1024))
    if required_vram_mb < estimated:
        raise ValueError(f"requested VRAM is below the conservative workload estimate ({estimated} MB)")
    if len(json.dumps(payload, ensure_ascii=False)) > 1024:
        raise ValueError("GPU task payload too large")
    return {"size": size, "estimated_vram_mb": estimated}


class GPUScheduler:
    def __init__(self, store, *, lease_seconds=120.0):
        self.store = store
        self.lease_seconds = float(lease_seconds)
        if not math.isfinite(self.lease_seconds) or self.lease_seconds < 15:
            raise ValueError("GPU lease_seconds must be finite and at least 15")
        with self.store._tx() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS gpu_tasks (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    required_vram_mb INTEGER NOT NULL,
                    required_vendor TEXT NOT NULL,
                    required_backend TEXT NOT NULL,
                    status TEXT NOT NULL,
                    assigned_node TEXT,
                    assigned_gpu_index INTEGER,
                    assigned_gpu_uuid TEXT,
                    assigned_at REAL,
                    lease_token TEXT,
                    lease_until REAL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 3,
                    result_json TEXT,
                    error TEXT,
                    created REAL NOT NULL,
                    updated REAL NOT NULL
                )
            """)
            columns = {column["name"] for column in conn.execute("PRAGMA table_info(gpu_tasks)")}
            if "assigned_gpu_uuid" not in columns:
                conn.execute("ALTER TABLE gpu_tasks ADD COLUMN assigned_gpu_uuid TEXT")
                conn.execute("""
                    UPDATE gpu_tasks SET assigned_gpu_uuid=(
                        SELECT uuid FROM gpu_devices
                        WHERE node_id=gpu_tasks.assigned_node AND gpu_index=gpu_tasks.assigned_gpu_index
                    ) WHERE status='leased'
                """)
            conn.execute("CREATE INDEX IF NOT EXISTS gpu_tasks_status_created ON gpu_tasks(status, created, id)")
            conn.execute("CREATE INDEX IF NOT EXISTS gpu_tasks_device_uuid ON gpu_tasks(assigned_gpu_uuid, status, lease_until)")

    def submit(self, kind, payload, *, required_vram_mb=256,
               required_vendor="nvidia", required_backend="cuda", max_attempts=3):
        validate_gpu_task(kind, payload, required_vram_mb)
        if required_vendor != "nvidia" or required_backend != "cuda":
            raise ValueError("v0.7.1 GPU execution currently supports NVIDIA CUDA only")
        if type(max_attempts) is not int or not 1 <= max_attempts <= 5:
            raise ValueError("max_attempts must be an integer in range 1..5")
        task_id, now = uuid.uuid4().hex, self.store.clock()
        with self.store._tx() as conn:
            if kind == QWEN2_TASK:
                minimum = conn.execute("""
                    SELECT MIN(g.qwen2_vram_mb)
                    FROM gpu_devices g JOIN nodes n ON n.id=g.node_id
                    WHERE g.qwen2_model_id=? AND n.last_seen>=? AND g.last_seen>=?
                """, (payload["model_id"], now - self.store.heartbeat_ttl,
                      now - max(90.0, self.store.heartbeat_ttl * 3))).fetchone()[0]
                if minimum is None:
                    raise ValueError("Qwen2 model has no online GPU Worker")
                required_vram_mb = max(required_vram_mb, minimum)
            conn.execute("""
                INSERT INTO gpu_tasks(
                    id,kind,payload_json,required_vram_mb,required_vendor,
                    required_backend,status,attempts,max_attempts,created,updated
                ) VALUES(?,?,?,?,?,?,'queued',0,?,?,?)
            """, (task_id, kind, json.dumps(payload), required_vram_mb,
                  required_vendor, required_backend, max_attempts, now, now))
        return {"id": task_id, "status": "queued",
                "required_vram_mb": required_vram_mb}

    def _reap(self, conn, now):
        conn.execute("""
            UPDATE gpu_tasks
            SET status=CASE WHEN attempts>=max_attempts THEN 'failed' ELSE 'queued' END,
                assigned_node=NULL,assigned_gpu_index=NULL,assigned_gpu_uuid=NULL,assigned_at=NULL,
                lease_token=NULL,lease_until=NULL,
                error=CASE WHEN attempts>=max_attempts
                           THEN 'GPU lease expired or Worker went offline'
                           ELSE 'GPU lease expired; task returned to queue' END,
                updated=?
            WHERE status='leased' AND (
                lease_until<=? OR (kind<>? AND assigned_node IN (
                    SELECT id FROM nodes WHERE last_seen<?
                ))
            )
        """, (now, now, QWEN2_TASK, now - self.store.heartbeat_ttl))
        conn.execute("""
            UPDATE gpu_tasks
            SET assigned_node=NULL,assigned_gpu_index=NULL,assigned_gpu_uuid=NULL,
                assigned_at=NULL,lease_token=NULL,lease_until=NULL,updated=?
            WHERE status='cancelled' AND lease_until<=?
        """, (now, now))

    @staticmethod
    def _detail(conn, task_id):
        row = conn.execute("""
            SELECT id,kind,payload_json,required_vram_mb,required_vendor,
                   required_backend,status,assigned_node,assigned_gpu_index,
                   lease_until,attempts,max_attempts,result_json,error,created,updated
            FROM gpu_tasks WHERE id=?
        """, (task_id,)).fetchone()
        if row is None:
            raise KeyError(task_id)
        task = dict(row)
        task["payload"] = json.loads(task.pop("payload_json"))
        result = task.pop("result_json")
        task["result"] = json.loads(result) if result is not None else None
        return task

    def _available_devices(self, conn, now):
        rows = conn.execute("""
            SELECT g.node_id,g.gpu_index,g.uuid,g.name,g.vendor,g.backend,
                   g.total_memory_mb,g.free_memory_mb,g.utilization_pct,g.qwen2_model_id,g.qwen2_vram_mb,
                   COALESCE(leases.reserved, 0) AS reserved
            FROM gpu_devices g JOIN nodes n ON n.id=g.node_id
            LEFT JOIN (
                SELECT assigned_gpu_uuid,SUM(required_vram_mb) AS reserved
                FROM gpu_tasks WHERE status IN ('leased','cancelled') AND lease_until>?
                GROUP BY assigned_gpu_uuid
            ) leases ON leases.assigned_gpu_uuid=g.uuid
            WHERE n.last_seen>=? AND g.last_seen>=? AND g.runtime='pytorch_cuda'
              AND NOT EXISTS (
                  SELECT 1 FROM gpu_tasks busy WHERE busy.assigned_node=g.node_id
                    AND busy.kind=? AND busy.status IN ('leased','cancelled') AND busy.lease_until>?
              )
        """, (now, now - self.store.heartbeat_ttl,
              now - max(90.0, self.store.heartbeat_ttl * 3), QWEN2_TASK, now))
        devices = []
        for row in rows:
            # Telemetry cannot establish how much of a reservation is allocated.
            available = max(0, row["free_memory_mb"] - row["reserved"])
            if available < 64:
                continue
            utilization = row["utilization_pct"]
            score = (available / row["total_memory_mb"]) * (1 - utilization / 100)
            devices.append((score, available, -utilization, row["node_id"], -row["gpu_index"], row))
        return sorted(devices, key=lambda device: device[:5], reverse=True)

    def claim(self, node_id, token):
        with self.store._tx() as conn:
            requester = self.store._require_gpu_node(conn, node_id, token)
            now = self.store.clock()
            self._reap(conn, now)
            if requester["last_seen"] < now - self.store.heartbeat_ttl:
                raise ValueError("Worker offline; heartbeat first")
            devices = self._available_devices(conn, now)
            capacity = max((entry[1] for entry in devices if entry[3] == node_id), default=0)
            queued = conn.execute("""
                SELECT * FROM gpu_tasks WHERE status='queued' AND required_vram_mb<=?
                ORDER BY created,id
            """, (capacity,))
            for job in queued:
                payload = json.loads(job["payload_json"])
                device = next((row for _, available, _, _, _, row in devices
                               if available >= job["required_vram_mb"]
                               and row["vendor"] == job["required_vendor"]
                               and row["backend"] == job["required_backend"]
                               and (job["kind"] != QWEN2_TASK
                                    or (row["qwen2_model_id"] == payload["model_id"]
                                        and row["qwen2_vram_mb"] <= job["required_vram_mb"]))), None)
                if device is None or device["node_id"] != node_id:
                    continue
                lease_token = secrets.token_urlsafe(24)
                deadline = now + self.lease_seconds
                conn.execute("""
                    UPDATE gpu_tasks SET status='leased',assigned_node=?,
                        assigned_gpu_index=?,assigned_gpu_uuid=?,assigned_at=?,lease_token=?,lease_until=?,
                        attempts=attempts+1,updated=?
                    WHERE id=? AND status='queued'
                """, (node_id, device["gpu_index"], device["uuid"], now, lease_token,
                      deadline, now, job["id"]))
                return {"task": {
                    "id": job["id"], "kind": job["kind"],
                    "payload": payload,
                    "required_vram_mb": job["required_vram_mb"],
                    "gpu_index": device["gpu_index"],
                    "gpu_name": device["name"],
                    "lease_token": lease_token,
                    "lease_until": deadline,
                    "lease_seconds": self.lease_seconds,
                }}
        return {"task": None}

    @staticmethod
    def _leased_job(conn, node_id, task_id, gpu_index, lease_token, now):
        job = conn.execute("SELECT * FROM gpu_tasks WHERE id=?", (task_id,)).fetchone()
        if (job is None or job["status"] not in ("leased", "cancelled")
                or job["assigned_node"] != node_id
                or job["assigned_gpu_index"] != gpu_index
                or not isinstance(lease_token, str) or not lease_token.isascii()
                or not secrets.compare_digest(job["lease_token"] or "", lease_token)
                or job["lease_until"] is None or job["lease_until"] <= now):
            raise LeaseConflict("invalid, expired or already completed GPU lease")
        return job

    def lease_active(self, node_id, token, task_id, gpu_index, lease_token):
        with self.store._tx() as conn:
            self.store._require_gpu_node(conn, node_id, token)
            now = self.store.clock()
            self._reap(conn, now)
            job = self._leased_job(conn, node_id, task_id, gpu_index, lease_token, now)
            return job["status"] == "leased"

    def complete(self, node_id, token, task_id, gpu_index, lease_token,
                 result, *, error=None):
        if not isinstance(result, dict):
            raise ValueError("GPU task result must be an object")
        try:
            encoded_result = json.dumps(result, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ValueError("GPU task result must contain valid JSON values") from exc
        if len(encoded_result) > 200000:
            raise ValueError("GPU task result must be no larger than 200 KB")
        if error is not None and (not isinstance(error, str) or len(error) > 500):
            raise ValueError("GPU task error must be at most 500 characters")
        with self.store._tx() as conn:
            self.store._require_gpu_node(conn, node_id, token)
            now = self.store.clock()
            self._reap(conn, now)
            job = self._leased_job(conn, node_id, task_id, gpu_index, lease_token, now)
            if job["status"] == "cancelled":
                status, error = "cancelled", job["error"]
            else:
                status = "completed" if error is None else (
                    "failed" if job["attempts"] >= job["max_attempts"] else "queued"
                )
            conn.execute("""
                UPDATE gpu_tasks SET status=?,result_json=?,error=?,
                    assigned_node=CASE WHEN ?='completed' THEN assigned_node ELSE NULL END,
                    assigned_gpu_index=CASE WHEN ?='completed' THEN assigned_gpu_index ELSE NULL END,
                    assigned_gpu_uuid=CASE WHEN ?='completed' THEN assigned_gpu_uuid ELSE NULL END,
                    assigned_at=CASE WHEN ?='completed' THEN assigned_at ELSE NULL END,
                    lease_token=NULL,lease_until=NULL,updated=? WHERE id=?
            """, (status, encoded_result if status == "completed" else None,
                  error, status, status, status, status, now, task_id))
            return {"ok": True, "status": status}

    def detail(self, task_id):
        with self.store._tx() as conn:
            self._reap(conn, self.store.clock())
            return self._detail(conn, task_id)

    def cancel(self, task_id):
        with self.store._tx() as conn:
            now = self.store.clock()
            self._reap(conn, now)
            task = self._detail(conn, task_id)
            if task["status"] not in ("queued", "leased"):
                raise LeaseConflict("only queued or leased GPU tasks can be cancelled")
            if task["kind"] == QWEN2_TASK and task["status"] == "leased":
                conn.execute("""
                    UPDATE gpu_tasks SET status='cancelled',error='cancelled by administrator',
                        updated=? WHERE id=?
                """, (now, task_id))
            else:
                conn.execute("""
                    UPDATE gpu_tasks SET status='cancelled',assigned_node=NULL,
                        assigned_gpu_index=NULL,assigned_gpu_uuid=NULL,assigned_at=NULL,
                        lease_token=NULL,lease_until=NULL,result_json=NULL,
                        error='cancelled by administrator',updated=? WHERE id=?
                """, (now, task_id))
            return self._detail(conn, task_id)

    def retry(self, task_id):
        with self.store._tx() as conn:
            now = self.store.clock()
            self._reap(conn, now)
            task = self._detail(conn, task_id)
            if task["status"] not in ("failed", "cancelled"):
                raise LeaseConflict("only failed or cancelled GPU tasks can be retried")
            if task["lease_until"] is not None and task["lease_until"] > now:
                raise LeaseConflict("GPU inference is still shutting down; retry after lease release")
            conn.execute("""
                UPDATE gpu_tasks SET status='queued',assigned_node=NULL,
                    assigned_gpu_index=NULL,assigned_gpu_uuid=NULL,assigned_at=NULL,lease_token=NULL,
                    lease_until=NULL,result_json=NULL,error=NULL,attempts=0,updated=?
                WHERE id=?
            """, (now, task_id))
            return self._detail(conn, task_id)

    def status(self):
        with self.store._tx() as conn:
            now = self.store.clock()
            self._reap(conn, now)
            rows = conn.execute("""
                SELECT id,kind,payload_json,required_vram_mb,required_vendor,
                       required_backend,status,assigned_node,assigned_gpu_index,
                       lease_until,attempts,max_attempts,result_json,error,created,updated
                FROM gpu_tasks ORDER BY created DESC,id DESC LIMIT 100
            """).fetchall()
            tasks = []
            for row in rows:
                task = dict(row)
                task["payload"] = json.loads(task.pop("payload_json"))
                result = task.pop("result_json")
                task["result"] = json.loads(result) if result is not None else None
                tasks.append(task)
            counts = {row["status"]: row["n"] for row in conn.execute(
                "SELECT status,COUNT(*) AS n FROM gpu_tasks GROUP BY status"
            )}
            reserved = conn.execute("""
                SELECT COALESCE(SUM(required_vram_mb),0) AS total
                FROM gpu_tasks WHERE status IN ('leased','cancelled') AND lease_until>?
            """, (now,)).fetchone()["total"]
        return {
            "tasks": tasks,
            "counts": {status: counts.get(status, 0) for status in GPU_TASK_STATUSES},
            "reserved_vram_mb": int(reserved or 0),
            "lease_seconds": self.lease_seconds,
        }
