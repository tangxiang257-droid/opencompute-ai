"""Single-host research coordinator linking registered v0.3 nodes to GPT-2 TP ranks.

The trusted API host exports model shards. Each authenticated node may read
only its own rank files. Not suitable for internet-facing untrusted peers.
"""
from contextlib import closing
import hashlib
import json
import math
import re
from pathlib import Path
import shutil
import socket
import time
import uuid

from compute.gpt2_shards import export_state, synthetic_state
from compute.gpt2_generate import read_manifest
from cluster_v03.store import LeaseConflict


class SessionCoordinator:
    def __init__(self, cluster, root, *, checkpoint_root=None, clock=None,
                 prepare_ttl=120, run_ttl=180):
        self.cluster = cluster
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.checkpoint_root = (Path(checkpoint_root).resolve()
                                if checkpoint_root else self.root.parent / "checkpoints")
        self._file_index = {}
        self.clock = clock or time.time
        self.prepare_ttl, self.run_ttl = prepare_ttl, run_ttl
        with closing(self.cluster._db()) as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS tp_sessions (
                  id TEXT PRIMARY KEY, state TEXT NOT NULL, created REAL NOT NULL,
                  updated REAL NOT NULL, deadline REAL NOT NULL,
                  workers INTEGER NOT NULL, prompt_json TEXT NOT NULL,
                  new_tokens INTEGER NOT NULL, master_host TEXT NOT NULL,
                  master_port INTEGER NOT NULL, config_json TEXT NOT NULL,
                  error TEXT
                );
                CREATE TABLE IF NOT EXISTS tp_assignments (
                  session_id TEXT NOT NULL, rank INTEGER NOT NULL,
                  node_id TEXT NOT NULL, state TEXT NOT NULL,
                  result_json TEXT, error TEXT,
                  reserved_cpu INTEGER NOT NULL DEFAULT 1,
                  reserved_memory_mb INTEGER NOT NULL DEFAULT 512,
                  PRIMARY KEY(session_id, rank),
                  UNIQUE(session_id,node_id)
                );
                CREATE INDEX IF NOT EXISTS tp_node_jobs ON tp_assignments(node_id,state);
            """)
            # Migrate existing v0.3.1 SQLite deployments in place.
            columns = {item["name"] for item in db.execute(
                "PRAGMA table_info(tp_assignments)")}
            if "reserved_cpu" not in columns:
                db.execute("ALTER TABLE tp_assignments ADD COLUMN reserved_cpu INTEGER NOT NULL DEFAULT 1")
            if "reserved_memory_mb" not in columns:
                db.execute("ALTER TABLE tp_assignments ADD COLUMN reserved_memory_mb INTEGER NOT NULL DEFAULT 512")

    @staticmethod
    def _reservation_mb(weight_file_bytes):
        # Conservative runtime reserve; actual RSS must still be measured.
        return 512 + math.ceil(weight_file_bytes * 2 / (1024 * 1024))

    @staticmethod
    def _allocate_port():
        # Laboratory only; bind-release is not a reservation against concurrent apps.
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            return listener.getsockname()[1]

    def _expire(self, db, now):
        rows = db.execute("""SELECT s.id FROM tp_sessions s
            WHERE s.state IN ('preparing','running') AND (
              s.deadline<? OR EXISTS (
                SELECT 1 FROM tp_assignments a JOIN nodes n ON n.id=a.node_id
                WHERE a.session_id=s.id AND a.state IN ('assigned','ready') AND n.last_seen<?
              ))""", (now, now - self.cluster.heartbeat_ttl)).fetchall()
        for row in rows:
            db.execute("""UPDATE tp_sessions SET state='failed',
                       error='timeout or Worker heartbeat missing',updated=? WHERE id=?""",
                       (now, row["id"]))

    def _select_nodes(self, db, workers, file_sizes, now):
        nodes = db.execute("SELECT * FROM nodes WHERE last_seen>=?",
                           (now - self.cluster.heartbeat_ttl,)).fetchall()
        active = self.cluster._active_reservations(db, now)
        idle = []
        for node in nodes:
            caps = json.loads(node["capabilities"])
            reservation = active.get(node["id"], {"slots": 0, "ram": 0, "cpu": 0})
            if ("tp_gpt2" not in caps or reservation["slots"] >= node["slots"]
                    or node["cpu_cores"] - reservation["cpu"] < 1):
                continue
            free_ram = node["free_ram_mb"] - reservation["ram"]
            if free_ram < 512:
                continue
            score = node["compute_score"] * max(0.05, 1 - node["cpu_load_pct"] / 100)
            idle.append((score, free_ram, node["id"]))
        idle.sort(reverse=True)
        chosen = []
        for rank in range(workers):
            need = self._reservation_mb(file_sizes[rank])
            match = next((node for node in idle
                          if node[1] >= need and node[2] not in chosen), None)
            if match is None:
                raise LeaseConflict("not enough online idle tp_gpt2 nodes with RAM headroom")
            chosen.append(match[2])
        return [(node_id, self._reservation_mb(file_sizes[rank]))
                for rank, node_id in enumerate(chosen)]

    def create_synthetic(self, *, workers, hidden, heads, layers, prompt_ids, new_tokens):
        if (not 1 <= workers <= 4 or not 8 <= hidden <= 128
                or not 1 <= heads <= 8 or hidden % heads or heads % workers
                or not 1 <= layers <= 4 or not 1 <= new_tokens <= 16
                or not 1 <= len(prompt_ids) <= 32
                or any(type(v) is not int or not 0 <= v < 64 for v in prompt_ids)
                or len(prompt_ids) + new_tokens - 1 > 64):
            raise ValueError("unsupported synthetic model / prompt configuration")
        session_id = uuid.uuid4().hex
        folder = self.root / session_id
        try:
            state, config = synthetic_state(hidden=hidden, heads=heads, layers=layers,
                                            vocab=64, positions=64)
            manifest = export_state(state, config, folder, workers,
                                    "synthetic-tp-v0.3.1")
            return self._register_session(session_id, folder, manifest,
                                          workers, prompt_ids, new_tokens)
        except BaseException:
            shutil.rmtree(folder, ignore_errors=True)
            raise

    def create_checkpoint(self, *, checkpoint, prompt_ids, new_tokens):
        """Use locally trusted, already exported GPT-2 shards; no model downloads."""
        if (not isinstance(checkpoint, str)
                or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", checkpoint)
                or checkpoint in (".", "..")):
            raise ValueError("checkpoint name must be a safe directory name")
        source = (self.checkpoint_root / checkpoint).resolve()
        if not source.is_relative_to(self.checkpoint_root.resolve()):
            raise ValueError("checkpoint directory traversal rejected")
        if not source.is_dir():
            raise ValueError("checkpoint directory not found")
        try:
            manifest = read_manifest(source)
        except (FileNotFoundError, KeyError, json.JSONDecodeError) as exc:
            raise ValueError("checkpoint manifest missing or invalid") from exc
        world = manifest["workers"]
        if (not 1 <= world <= 4 or not 1 <= new_tokens <= 16
                or not 1 <= len(prompt_ids) <= 32
                or any(type(v) is not int or not 0 <= v < manifest["vocab"]
                       for v in prompt_ids)
                or len(prompt_ids) + new_tokens - 1 > manifest["positions"]):
            raise ValueError("unsupported checkpoint or context dimensions")
        session_id = uuid.uuid4().hex
        folder = self.root / session_id
        folder.mkdir(parents=True, exist_ok=False)
        try:
            names = (["manifest.json", "rank0_io.pt"]
                     + [f"rank_{rank}.pt" for rank in range(world)])
            for name in names:
                src = (source / name).resolve()
                if (not src.is_relative_to(source) or not src.is_file()
                        or src.stat().st_size > 512_000_000):
                    raise ValueError("checkpoint file missing, unsafe or too large")
                shutil.copyfile(src, folder / name)
            return self._register_session(session_id, folder, manifest,
                                          world, prompt_ids, new_tokens)
        except BaseException:
            shutil.rmtree(folder, ignore_errors=True)
            raise

    def _register_session(self, session_id, folder, manifest,
                          workers, prompt_ids, new_tokens):
        sizes = [(folder / f"rank_{rank}.pt").stat().st_size
                 + ((folder / "rank0_io.pt").stat().st_size if rank == 0 else 0)
                 for rank in range(workers)]
        now = self.clock()
        with self.cluster._tx() as db:
            self._expire(db, now)
            exists = db.execute("""SELECT 1 FROM tp_sessions
                                 WHERE state IN ('preparing','running') LIMIT 1""").fetchone()
            if exists:
                raise LeaseConflict("only one active TP session is supported")
            nodes = self._select_nodes(db, workers, sizes, now)
            port = self._allocate_port()
            db.execute("INSERT INTO tp_sessions VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                       (session_id, "preparing", now, now, now + self.prepare_ttl,
                        workers, json.dumps(prompt_ids), new_tokens, "127.0.0.1",
                        port, json.dumps(manifest), None))
            for rank, (node_id, reserved_mb) in enumerate(nodes):
                db.execute("""INSERT INTO tp_assignments(
                                session_id,rank,node_id,state,reserved_cpu,reserved_memory_mb)
                              VALUES(?,?,?,'assigned',1,?)""",
                           (session_id, rank, node_id, reserved_mb))
        return self.status(session_id)

    def _indexed_files(self, session_id):
        if session_id not in self._file_index:
            folder = self.root / session_id
            entries = {}
            for item in folder.iterdir():
                if not item.is_file():
                    continue
                checksum = hashlib.sha256()
                with item.open("rb") as reader:
                    while True:
                        block = reader.read(1024 * 1024)
                        if not block:
                            break
                        checksum.update(block)
                entries[item.name] = {
                    "name": item.name,
                    "size": item.stat().st_size,
                    "sha256": checksum.hexdigest(),
                }
            self._file_index[session_id] = entries
        return self._file_index[session_id]

    def status(self, session_id):
        with self.cluster._tx() as db:
            now = self.clock()
            self._expire(db, now)
            row = db.execute("SELECT * FROM tp_sessions WHERE id=?",
                             (session_id,)).fetchone()
            if row is None:
                raise KeyError("session not found")
            assigned = [dict(entry) for entry in db.execute(
                """SELECT rank,node_id,state,result_json,error FROM tp_assignments
                   WHERE session_id=? ORDER BY rank""", (session_id,))]
        for entry in assigned:
            raw = entry.pop("result_json")
            entry["result"] = json.loads(raw) if raw else None
        return {
            "session_id": session_id, "status": row["state"],
            "workers": row["workers"], "assigned": assigned,
            "prompt_ids": json.loads(row["prompt_json"]),
            "max_new_tokens": row["new_tokens"],
            "model": json.loads(row["config_json"]),
            "error": row["error"],
        }

    def _assignment_for(self, db, node_id, token, session_id=None):
        self.cluster._require_node(db, node_id, token)
        query = """SELECT s.*,a.rank,a.state AS assignment_state
                   FROM tp_assignments a JOIN tp_sessions s ON s.id=a.session_id
                   WHERE a.node_id=?"""
        params = [node_id]
        if session_id is not None:
            query += " AND s.id=?"
            params.append(session_id)
        query += " ORDER BY s.created DESC LIMIT 1"
        row = db.execute(query, tuple(params)).fetchone()
        if row is None:
            raise KeyError("assignment not found")
        return row

    def poll(self, node_id, token):
        with self.cluster._tx() as db:
            self._expire(db, self.clock())
            self.cluster._require_node(db, node_id, token)
            row = db.execute("""SELECT s.*,a.rank,a.state AS assignment_state
                FROM tp_assignments a JOIN tp_sessions s ON a.session_id=s.id
                WHERE a.node_id=? AND a.state IN ('assigned','ready')
                  AND s.state IN ('preparing','running')
                ORDER BY s.created LIMIT 1""", (node_id,)).fetchone()
        if row is None:
            return {"assignment": None}
        session_id = row["id"]
        rank = row["rank"]
        names = ["manifest.json", f"rank_{rank}.pt"]
        if rank == 0:
            names.append("rank0_io.pt")
        index = self._indexed_files(session_id)
        files = [index[name] for name in names]
        return {"assignment": {
            "session_id": session_id, "rank": rank,
            "workers": row["workers"],
            "phase": row["assignment_state"],
            "run": row["state"] == "running",
            "master_host": row["master_host"],
            "master_port": row["master_port"],
            "prompt_ids": json.loads(row["prompt_json"]),
            "max_new_tokens": row["new_tokens"],
            "files": files,
        }}

    def authorize_file(self, node_id, token, session_id, filename):
        with self.cluster._tx() as db:
            self._expire(db, self.clock())
            row = self._assignment_for(db, node_id, token, session_id)
            allowed = {"manifest.json", f"rank_{row['rank']}.pt"}
            if row["rank"] == 0:
                allowed.add("rank0_io.pt")
            if row["state"] not in ("preparing", "running") or filename not in allowed:
                raise PermissionError("file not assigned to this node")
        return self.root / session_id / filename

    def ready(self, node_id, token, session_id):
        with self.cluster._tx() as db:
            self._expire(db, self.clock())
            row = self._assignment_for(db, node_id, token, session_id)
            if row["state"] != "preparing" or row["assignment_state"] not in ("assigned", "ready"):
                raise LeaseConflict("session not preparing")
            db.execute("""UPDATE tp_assignments SET state='ready'
                          WHERE session_id=? AND node_id=?""",
                       (session_id, node_id))
            remaining = db.execute("""SELECT COUNT(*) AS n FROM tp_assignments
                                      WHERE session_id=? AND state!='ready'""",
                                   (session_id,)).fetchone()["n"]
            if remaining == 0:
                now = self.clock()
                db.execute("""UPDATE tp_sessions SET state='running',updated=?,deadline=?
                              WHERE id=?""",
                           (now, now + self.run_ttl, session_id))
        return {"ready": True, "all_ready": remaining == 0}

    def complete(self, node_id, token, session_id, result=None, error=None):
        with self.cluster._tx() as db:
            now = self.clock()
            self._expire(db, now)
            row = self._assignment_for(db, node_id, token, session_id)
            if error is not None and (not isinstance(error, str) or not error.strip()):
                raise ValueError("error must contain nonblank text if supplied")
            if error:
                allowed = (row["state"] in ("preparing", "running")
                           and row["assignment_state"] in ("assigned", "ready"))
            else:
                allowed = row["state"] == "running" and row["assignment_state"] == "ready"
            if not allowed:
                raise LeaseConflict("session failed, expired or already completed")
            if error and len(error) > 500:
                raise ValueError("error too long")
            if result is not None and len(json.dumps(result)) > 65536:
                raise ValueError("result too large")
            if error is None:
                # This checks protocol structure, not trustworthiness of a
                # computation performed by an untrusted outside device.
                if not isinstance(result, dict):
                    raise ValueError("rank completion requires a result object")
                if row["rank"] == 0:
                    ids = result.get("generated_ids")
                    model = json.loads(row["config_json"])
                    if (not isinstance(ids, list)
                            or len(ids) != row["new_tokens"]
                            or any(type(v) is not int or not 0 <= v < model["vocab"]
                                   for v in ids)):
                        raise ValueError("rank zero returned invalid generated token IDs")
                elif (type(result.get("rank")) is not int
                      or result["rank"] != row["rank"]
                      or result.get("completed") is not True):
                    raise ValueError("peer rank returned invalid completion result")
            assignment_state = "failed" if error else "done"
            db.execute("""UPDATE tp_assignments SET state=?,result_json=?,error=?
                          WHERE session_id=? AND node_id=?""",
                       (assignment_state,
                        json.dumps(result) if result is not None else None,
                        error, session_id, node_id))
            if error:
                db.execute("""UPDATE tp_sessions SET state='failed',error=?,updated=?
                              WHERE id=?""",
                           (f"rank {row['rank']}: {error}", now, session_id))
            else:
                outstanding = db.execute("""SELECT COUNT(*) AS n FROM tp_assignments
                    WHERE session_id=? AND state!='done'""",
                    (session_id,)).fetchone()["n"]
                if outstanding == 0:
                    db.execute("""UPDATE tp_sessions SET state='completed',updated=?
                                  WHERE id=?""", (now, session_id))
        return {"ok": True, "state": assignment_state}

    def cancel(self, session_id):
        """Idempotent administrative cancellation; workers poll for revocation."""
        with self.cluster._tx() as db:
            self._expire(db, self.clock())
            row = db.execute("SELECT state FROM tp_sessions WHERE id=?",
                             (session_id,)).fetchone()
            if row is None:
                raise KeyError("session not found")
            if row["state"] in ("preparing", "running"):
                db.execute("""UPDATE tp_sessions SET state='failed',
                              error='cancelled by administrator',updated=?
                              WHERE id=?""", (self.clock(), session_id))
                return {"status": "failed", "cancelled": True}
            return {"status": row["state"], "cancelled": False}

    def stopped(self, node_id, token, session_id):
        """Acknowledge process-group teardown before releasing a failed TP slot."""
        with self.cluster._tx() as db:
            self._expire(db, self.clock())
            row = self._assignment_for(db, node_id, token, session_id)
            if row["state"] != "failed":
                raise LeaseConflict("stop acknowledgement only applies to failed sessions")
            if row["assignment_state"] in ("assigned", "ready"):
                db.execute("""UPDATE tp_assignments SET state='stopped'
                              WHERE session_id=? AND node_id=?""",
                           (session_id, node_id))
        return {"ok": True}

    def prune(self, older_than_seconds=86400):
        """Remove *terminal* session files after a grace period.

        A running session can never be pruned. SQLite deletion and the file
        cleanup are separate; an interrupted cleanup is safe to retry.
        """
        if older_than_seconds < 60:
            raise ValueError("retention must be at least 60 seconds")
        with self.cluster._tx() as db:
            now = self.clock()
            self._expire(db, now)
            rows = db.execute("""SELECT s.id FROM tp_sessions s
                    WHERE s.state IN ('completed','failed') AND s.updated<=?
                      AND NOT EXISTS (SELECT 1 FROM tp_assignments a
                         WHERE a.session_id=s.id AND s.state='failed'
                           AND a.state IN ('assigned','ready')
                           AND s.updated+180>=?)""",
                    (now - older_than_seconds, now)).fetchall()
            ids = [row["id"] for row in rows]
            for session_id in ids:
                db.execute("DELETE FROM tp_assignments WHERE session_id=?", (session_id,))
                db.execute("DELETE FROM tp_sessions WHERE id=?", (session_id,))
        for session_id in ids:
            self._file_index.pop(session_id, None)
            shutil.rmtree(self.root / session_id, ignore_errors=True)
        return {"removed": len(ids)}
