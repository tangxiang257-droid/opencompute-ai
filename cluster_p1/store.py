"""SQLite metadata journal. This module never restores weights, activations or KV."""

from dataclasses import dataclass, field
from contextlib import contextmanager
from collections.abc import Sequence
import json
from pathlib import Path
import sqlite3
from .contracts import (
    Task,
    Session,
    Stage,
    Tile,
    State,
    TERMINAL,
    TRANSITIONS,
    Identifier,
)
from pydantic import TypeAdapter


class Conflict(ValueError):
    """Event identity, binding, epoch, sequence, or state conflict."""


@dataclass(frozen=True, slots=True)
class StageProgress:
    stage_id: int
    committed_tokens: int
    next_tile_id: int
    remaining_tokens: int
    inflight_tile: Tile | None
    inflight_state: str | None
    upstream_tile: Tile | None


@dataclass(frozen=True, slots=True)
class PipelineProgress:
    session_id: str
    epoch: int
    state: State
    expected_tokens: int
    stages: tuple[StageProgress, ...]
    execution_allowed: bool = field(default=False, init=False)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


class P1Store:
    """File-backed independent connections; BEGIN IMMEDIATE serializes each mutation.

    Event IDs are global to this store, immutable and bounded identifiers. Exact
    replays return the original result without applying mutations again.
    """

    def __init__(self, path):
        self.path = str(path)
        if self.path == ":memory:":
            raise ValueError("persistent file required")
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with self._tx() as c:
            c.execute(
                "CREATE TABLE IF NOT EXISTS p1_tasks(id TEXT PRIMARY KEY, body TEXT NOT NULL)"
            )
            c.execute(
                "CREATE TABLE IF NOT EXISTS p1_sessions(id TEXT PRIMARY KEY, task_id TEXT UNIQUE NOT NULL REFERENCES p1_tasks(id), body TEXT NOT NULL)"
            )
            c.execute(
                "CREATE TABLE IF NOT EXISTS p1_stages(session_id TEXT NOT NULL REFERENCES p1_sessions(id), stage_id INTEGER NOT NULL, body TEXT NOT NULL, PRIMARY KEY(session_id,stage_id))"
            )
            c.execute(
                "CREATE TABLE IF NOT EXISTS p1_tiles(session_id TEXT NOT NULL, epoch INTEGER NOT NULL, stage_id INTEGER NOT NULL, tile_id INTEGER NOT NULL, body TEXT NOT NULL, PRIMARY KEY(session_id,epoch,stage_id,tile_id), FOREIGN KEY(session_id,stage_id) REFERENCES p1_stages(session_id,stage_id))"
            )
            c.execute(
                "CREATE TABLE IF NOT EXISTS p1_tile_states(session_id TEXT NOT NULL, epoch INTEGER NOT NULL, stage_id INTEGER NOT NULL, tile_id INTEGER NOT NULL, body TEXT NOT NULL, state TEXT NOT NULL, PRIMARY KEY(session_id,epoch,stage_id,tile_id))"
            )
            c.execute(
                "CREATE TABLE IF NOT EXISTS p1_stage_states(session_id TEXT NOT NULL, epoch INTEGER NOT NULL, stage_id INTEGER NOT NULL, state TEXT NOT NULL, PRIMARY KEY(session_id,epoch,stage_id))"
            )
            c.execute(
                "CREATE TABLE IF NOT EXISTS p1_events(id TEXT PRIMARY KEY, payload TEXT NOT NULL, result TEXT NOT NULL)"
            )

    @contextmanager
    def _tx(self):
        c = sqlite3.connect(self.path, timeout=15, isolation_level=None)
        try:
            c.execute("PRAGMA foreign_keys=ON")
            c.execute("BEGIN IMMEDIATE")
            yield c
            c.commit()
        except BaseException:
            c.rollback()
            raise
        finally:
            c.close()

    def _event(self, event_id, payload, operation):
        TypeAdapter(Identifier).validate_python(event_id, strict=True)
        encoded = canonical(payload)
        with self._tx() as c:
            old = c.execute(
                "SELECT payload,result FROM p1_events WHERE id=?", (event_id,)
            ).fetchone()
            if old:
                if old[0] != encoded:
                    raise Conflict("event ID reused with different payload")
                return json.loads(old[1])
            result = operation(c)
            c.execute(
                "INSERT INTO p1_events VALUES(?,?,?)",
                (event_id, encoded, canonical(result)),
            )
            return result

    @staticmethod
    def _load(c, table, key, model):
        row = c.execute(f"SELECT body FROM {table} WHERE id=?", (key,)).fetchone()
        if not row:
            raise Conflict("unknown object")
        return model.model_validate_json(row[0])

    @staticmethod
    def _save(c, table, key, obj):
        c.execute(f"UPDATE {table} SET body=? WHERE id=?", (obj.model_dump_json(), key))

    def create_task(self, task: Task, *, event_id: str) -> None:
        task = Task.model_validate(task)

        def op(c):
            if task.state != State.QUEUED:
                raise Conflict("new task must be queued")
            if c.execute(
                "SELECT 1 FROM p1_tasks WHERE id=?", (task.task_id,)
            ).fetchone():
                raise Conflict("task exists")
            c.execute(
                "INSERT INTO p1_tasks VALUES(?,?)",
                (task.task_id, task.model_dump_json()),
            )

        return self._event(event_id, ["create", task.model_dump(mode="json")], op)

    def get_task(self, task_id: str, *, owner_id: str) -> Task:
        with self._tx() as c:
            task = self._load(c, "p1_tasks", task_id, Task)
        if task.owner_id != owner_id:
            raise PermissionError("task owner mismatch")
        return task

    def cancel_task(self, task_id: str, *, owner_id: str, event_id: str):
        """Owner-authorized cancellation atomically terminates any bound session."""

        def op(c):
            task = self._load(c, "p1_tasks", task_id, Task)
            if task.owner_id != owner_id:
                raise PermissionError("task owner mismatch")
            if task.state in TERMINAL:
                raise Conflict("task is terminal")
            row = c.execute(
                "SELECT body FROM p1_sessions WHERE task_id=?", (task_id,)
            ).fetchone()
            if row:
                self._states(c, Session.model_validate_json(row[0]), State.CANCELLED)
            else:
                self._save(
                    c,
                    "p1_tasks",
                    task_id,
                    task.model_copy(update={"state": State.CANCELLED}),
                )

        return self._event(event_id, ["cancel_task", task_id, owner_id], op)

    def get_session(self, session_id: str) -> Session:
        """Trusted control-plane read; callers must authenticate before exposing it."""
        with self._tx() as c:
            return self._load(c, "p1_sessions", session_id, Session)

    def bind_session(
        self, session: Session, stages: Sequence[Stage], *, event_id: str
    ) -> None:
        session = Session.model_validate(session)
        stages = tuple(Stage.model_validate(s) for s in stages)

        def op(c):
            task = self._load(c, "p1_tasks", session.task_id, Task)
            if (
                task.state != State.QUEUED
                or session.state != State.PREPARING
                or session.epoch != 1
            ):
                raise Conflict("invalid initial binding")
            if (
                session.owner_id != task.owner_id
                or session.model_hash != task.model_hash
            ):
                raise Conflict("owner/model mismatch")
            if c.execute(
                "SELECT 1 FROM p1_sessions WHERE id=? OR task_id=?",
                (session.session_id, session.task_id),
            ).fetchone():
                raise Conflict("session/task already bound")
            end = 0
            for i, s in enumerate(stages):
                if (
                    s.session_id != session.session_id
                    or s.stage_id != i
                    or s.layer_start != end
                    or s.model_hash != session.model_hash
                    or s.layout_hash != session.layout_hash
                    or s.assigned_node not in session.selected_nodes
                ):
                    raise Conflict("invalid stage coverage/binding/fingerprint")
                end = s.layer_end
            if (
                end != task.total_layers
                or not stages
                or {s.assigned_node for s in stages} != set(session.selected_nodes)
            ):
                raise Conflict("incomplete stage/node coverage")
            c.execute(
                "INSERT INTO p1_sessions VALUES(?,?,?)",
                (session.session_id, session.task_id, session.model_dump_json()),
            )
            for s in stages:
                c.execute(
                    "INSERT INTO p1_stages VALUES(?,?,?)",
                    (s.session_id, s.stage_id, s.model_dump_json()),
                )
            self._save(
                c,
                "p1_tasks",
                task.task_id,
                task.model_copy(update={"state": State.PREPARING}),
            )

        return self._event(
            event_id,
            [
                "bind",
                session.model_dump(mode="json"),
                [s.model_dump(mode="json") for s in stages],
            ],
            op,
        )

    def _active(self, c, sid, epoch):
        session = self._load(c, "p1_sessions", sid, Session)
        if type(epoch) is not int or epoch != session.epoch:
            raise Conflict("stale/invalid epoch")
        if session.state in TERMINAL:
            raise Conflict("terminal session")
        return session

    def _states(self, c, session, state):
        self._save(
            c,
            "p1_sessions",
            session.session_id,
            session.model_copy(update={"state": state}),
        )
        task = self._load(c, "p1_tasks", session.task_id, Task)
        self._save(
            c, "p1_tasks", task.task_id, task.model_copy(update={"state": state})
        )

    def transition(self, session_id: str, epoch: int, state: State, *, event_id: str):
        if not isinstance(state, State):
            raise ValueError("State enum required")

        def op(c):
            s = self._active(c, session_id, epoch)
            if state not in TRANSITIONS[s.state]:
                raise Conflict("illegal transition")
            if state == State.RUNNING:
                states = c.execute(
                    "SELECT state FROM p1_stage_states WHERE session_id=? AND epoch=?",
                    (session_id, epoch),
                ).fetchall()
                count = c.execute(
                    "SELECT count(*) FROM p1_stages WHERE session_id=?", (session_id,)
                ).fetchone()[0]
                if len(states) != count or any(row[0] != "ready" for row in states):
                    raise Conflict("all stages must be ready")
            if state in (State.VERIFYING, State.COMPLETED):
                stages = c.execute(
                    "SELECT stage_id FROM p1_stages WHERE session_id=?", (session_id,)
                ).fetchall()
                for (stage,) in stages:
                    row = c.execute(
                        "SELECT body FROM p1_tiles WHERE session_id=? AND epoch=? AND stage_id=? ORDER BY tile_id DESC LIMIT 1",
                        (session_id, epoch, stage),
                    ).fetchone()
                    t = Tile.model_validate_json(row[0]) if row else None
                    if t is None or t.token_start + t.token_count != s.expected_tokens:
                        raise Conflict("incomplete verified prefix")
            self._states(c, s, state)

        return self._event(event_id, ["transition", session_id, epoch, state.value], op)

    def _commit_tile(self, c, tile, *, write=True):
        s = self._active(c, tile.session_id, tile.epoch)
        if s.state != State.RUNNING:
            raise Conflict("session not running")
        if not c.execute(
            "SELECT 1 FROM p1_stages WHERE session_id=? AND stage_id=?",
            (tile.session_id, tile.stage_id),
        ).fetchone():
            raise Conflict("unknown stage")
        rows = c.execute(
            "SELECT body FROM p1_tiles WHERE session_id=? AND epoch=? AND stage_id=? ORDER BY tile_id",
            (tile.session_id, tile.epoch, tile.stage_id),
        ).fetchall()
        last = Tile.model_validate_json(rows[-1][0]) if rows else None
        if tile.tile_id != len(rows) or tile.token_start != (
            last.token_start + last.token_count if last else 0
        ):
            raise Conflict("tile sequence/prefix mismatch")
        if tile.stage_id:
            row = c.execute(
                "SELECT body FROM p1_tiles WHERE session_id=? AND epoch=? AND stage_id=? AND tile_id=?",
                (tile.session_id, tile.epoch, tile.stage_id - 1, tile.tile_id),
            ).fetchone()
            if not row:
                raise Conflict("upstream dependency uncommitted")
            up = Tile.model_validate_json(row[0])
            if (up.token_start, up.token_count, up.dtype, up.shape) != (
                tile.token_start,
                tile.token_count,
                tile.dtype,
                tile.shape,
            ):
                raise Conflict("upstream tensor mismatch")
        if not write:
            return
        c.execute(
            "INSERT INTO p1_tiles VALUES(?,?,?,?,?)",
            (
                tile.session_id,
                tile.epoch,
                tile.stage_id,
                tile.tile_id,
                tile.model_dump_json(),
            ),
        )

    def tile_transition(self, tile: Tile, state: str, *, event_id: str):
        """Record trusted executor/verifier attestations; no computation proof implied."""
        tile = Tile.model_validate(tile)
        sequence = {
            "received": "computed",
            "computed": "verified",
            "verified": "committed",
        }

        def op(c):
            s = self._active(c, tile.session_id, tile.epoch)
            if s.state != State.RUNNING:
                raise Conflict("session not running")
            if tile.model_hash != s.model_hash or tile.layout_hash != s.layout_hash:
                raise Conflict("tile fingerprint mismatch")
            if tile.token_start + tile.token_count > s.expected_tokens:
                raise Conflict("token range exceeds plan")
            if not c.execute(
                "SELECT 1 FROM p1_stages WHERE session_id=? AND stage_id=?",
                (tile.session_id, tile.stage_id),
            ).fetchone():
                raise Conflict("unknown stage")
            key = (tile.session_id, tile.epoch, tile.stage_id, tile.tile_id)
            row = c.execute(
                "SELECT body,state FROM p1_tile_states WHERE session_id=? AND epoch=? AND stage_id=? AND tile_id=?",
                key,
            ).fetchone()
            if row is None:
                if state != "received":
                    raise Conflict("tile must be received first")
                self._commit_tile(c, tile, write=False)
                c.execute(
                    "INSERT INTO p1_tile_states VALUES(?,?,?,?,?,?)",
                    (*key, tile.model_dump_json(), state),
                )
            else:
                if row[0] != tile.model_dump_json() or sequence.get(row[1]) != state:
                    raise Conflict("tile payload/transition conflict")
                if state == "committed":
                    self._commit_tile(c, tile)
                c.execute(
                    "UPDATE p1_tile_states SET state=? WHERE session_id=? AND epoch=? AND stage_id=? AND tile_id=?",
                    (state, *key),
                )

        return self._event(
            event_id, ["tile_transition", tile.model_dump(mode="json"), state], op
        )

    def commit_tile(self, tile: Tile, *, event_id: str):
        """Commit only a previously verified tile; never infers verification."""
        return self.tile_transition(tile, "committed", event_id=event_id)

    def stage_state(self, session_id: str, epoch: int, stage_id: int) -> str:
        """Stage lifecycle follows its owning session; stage success is all-or-none."""
        with self._tx() as c:
            s = self._load(c, "p1_sessions", session_id, Session)
            if type(epoch) is not int or s.epoch != epoch:
                raise Conflict("stale epoch")
            if not c.execute(
                "SELECT 1 FROM p1_stages WHERE session_id=? AND stage_id=?",
                (session_id, stage_id),
            ).fetchone():
                raise Conflict("unknown stage")
            if s.state == State.PREPARING:
                row = c.execute(
                    "SELECT state FROM p1_stage_states WHERE session_id=? AND epoch=? AND stage_id=?",
                    (session_id, epoch, stage_id),
                ).fetchone()
                return row[0] if row else "preparing"
            return s.state.value

    def stage_transition(
        self, session_id: str, epoch: int, stage_id: int, state: str, *, event_id: str
    ):
        """Individually attest readiness, or fail/cancel the entire bound session."""

        def op(c):
            s = self._active(c, session_id, epoch)
            if (
                type(stage_id) is not int
                or not c.execute(
                    "SELECT 1 FROM p1_stages WHERE session_id=? AND stage_id=?",
                    (session_id, stage_id),
                ).fetchone()
            ):
                raise Conflict("unknown stage")
            if state in ("failed", "cancelled"):
                self._states(c, s, State(state))
                return
            if state != "ready" or s.state != State.PREPARING:
                raise Conflict("illegal stage transition")
            if c.execute(
                "SELECT 1 FROM p1_stage_states WHERE session_id=? AND epoch=? AND stage_id=?",
                (session_id, epoch, stage_id),
            ).fetchone():
                raise Conflict("stage already ready")
            c.execute(
                "INSERT INTO p1_stage_states VALUES(?,?,?,?)",
                (session_id, epoch, stage_id, state),
            )

        return self._event(event_id, ["stage", session_id, epoch, stage_id, state], op)

    def committed_prefix(self, session_id: str, epoch: int) -> int:
        """Minimum metadata commit prefix, never proof of durable/recoverable KV."""
        with self._tx() as c:
            self._active(c, session_id, epoch)
            prefixes = []
            for (stage,) in c.execute(
                "SELECT stage_id FROM p1_stages WHERE session_id=?", (session_id,)
            ).fetchall():
                row = c.execute(
                    "SELECT body FROM p1_tiles WHERE session_id=? AND epoch=? AND stage_id=? ORDER BY tile_id DESC LIMIT 1",
                    (session_id, epoch, stage),
                ).fetchone()
                t = Tile.model_validate_json(row[0]) if row else None
                prefixes.append(t.token_start + t.token_count if t else 0)
            return min(prefixes, default=0)

    def pipeline_progress(self, session_id: str, epoch: int, *, owner_id: str):
        """Owner-scoped, single read-transaction metadata observation only.

        No claim/reservation, executor readiness, KV validity or resume permission
        is issued. Results can be stale immediately after return. Existing mutating
        APIs must still recheck epoch/state/dependencies in their own transaction.
        A downstream upstream_tile is the exact next committed predecessor tile,
        not a fabricated range inferred from the minimum whole-session prefix.
        Terminal/interrupted sessions remain observable in their current epoch.
        """
        uri = Path(self.path).resolve().as_uri() + "?mode=ro"
        c = sqlite3.connect(uri, uri=True, timeout=15, isolation_level=None)
        try:
            c.execute("PRAGMA query_only=ON")
            c.execute("BEGIN")
            session = self._load(c, "p1_sessions", session_id, Session)
            if session.owner_id != owner_id:
                raise PermissionError("session owner mismatch")
            if type(epoch) is not int or epoch != session.epoch:
                raise Conflict("stale/invalid epoch")
            stages = []
            for (stage_id,) in c.execute(
                "SELECT stage_id FROM p1_stages WHERE session_id=? ORDER BY stage_id",
                (session_id,),
            ).fetchall():
                last = c.execute(
                    "SELECT body FROM p1_tiles WHERE session_id=? AND epoch=? "
                    "AND stage_id=? ORDER BY tile_id DESC LIMIT 1",
                    (session_id, epoch, stage_id),
                ).fetchone()
                tile = Tile.model_validate_json(last[0]) if last else None
                prefix = tile.token_start + tile.token_count if tile else 0
                next_id = tile.tile_id + 1 if tile else 0
                rows = c.execute(
                    "SELECT body,state FROM p1_tile_states WHERE session_id=? "
                    "AND epoch=? AND stage_id=? AND state!='committed'",
                    (session_id, epoch, stage_id),
                ).fetchall()
                if len(rows) > 1:
                    raise Conflict("multiple in-flight stage tiles")
                pending = Tile.model_validate_json(rows[0][0]) if rows else None
                pending_state = rows[0][1] if rows else None
                if pending is not None and (
                    pending.tile_id != next_id or pending.token_start != prefix
                    or pending_state not in ("received", "computed", "verified")
                ):
                    raise Conflict("inconsistent stage progress")
                upstream = None
                if stage_id and prefix < session.expected_tokens:
                    row = c.execute(
                        "SELECT body FROM p1_tiles WHERE session_id=? AND epoch=? "
                        "AND stage_id=? AND tile_id=?",
                        (session_id, epoch, stage_id - 1, next_id),
                    ).fetchone()
                    upstream = Tile.model_validate_json(row[0]) if row else None
                    if upstream is not None and upstream.token_start != prefix:
                        raise Conflict("inconsistent upstream prefix")
                stages.append(StageProgress(
                    stage_id, prefix, next_id, session.expected_tokens - prefix,
                    pending, pending_state, upstream,
                ))
            return PipelineProgress(session_id, epoch, session.state,
                                    session.expected_tokens, tuple(stages))
        finally:
            # No DML, event creation or schema initialization in this read path.
            # Closing rolls back the read transaction and releases its snapshot.
            c.close()

    def identify_interrupted(self, *, event_id: str) -> list[str]:
        """Explicit startup barrier: mark active metadata interrupted; no KV recovery.

        Invoke only while old producers are stopped. Opening a store is harmless.
        """

        def op(c):
            interrupted = []
            for (body,) in c.execute(
                "SELECT body FROM p1_sessions ORDER BY id"
            ).fetchall():
                s = Session.model_validate_json(body)
                if s.state in (State.RUNNING, State.PREPARING, State.VERIFYING):
                    self._states(c, s, State.INTERRUPTED)
                    interrupted.append(s.session_id)
            return interrupted

        return self._event(event_id, ["identify_interrupted"], op)

    def advance_epoch(self, session_id: str, expected_epoch: int, *, event_id: str):
        """Fence old results, start empty metadata prefix; no automatic resume/KV claim."""

        def op(c):
            s = self._active(c, session_id, expected_epoch)
            if s.state != State.INTERRUPTED or s.epoch >= 2**31 - 1:
                raise Conflict("epoch advance requires interruption")
            s = s.model_copy(update={"epoch": s.epoch + 1})
            self._states(c, s, State.PREPARING)
            return s.epoch

        return self._event(event_id, ["epoch", session_id, expected_epoch], op)
