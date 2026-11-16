"""Fresh isolated v3 control records. Never grants execution or releases devices.

Not a migration or an adapter for existing cluster/terminal databases. Device
strings are unverified claims; uniqueness is local bookkeeping, not availability.
Uses the dedicated connection/path prerequisites of InputTaskLedger.
"""
from dataclasses import dataclass

from snapshot_task_ledger import InputTaskLedger, LedgerRejected, _id, _need

_SCHEMA = tuple(
    sql.replace("state IN ('STORED','CANCELLED')", "state IN ('STORED','CANCELLED','STOP_PENDING')")
       .replace("OLD.state='STORED' AND NEW.state='CANCELLED'",
                "OLD.state='STORED' AND NEW.state IN ('CANCELLED','STOP_PENDING')")
    for sql in InputTaskLedger.SCHEMA
) + (
    "CREATE TABLE inert_attempts(task_id TEXT PRIMARY KEY REFERENCES stored_tasks(task_id), session_id TEXT NOT NULL UNIQUE, epoch INTEGER NOT NULL CHECK(epoch=1), plan_claim TEXT NOT NULL, snapshot_ref TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('PENDING','REVOKED','ISOLATED')))",
    "CREATE TABLE inert_holds(task_id TEXT NOT NULL REFERENCES inert_attempts(task_id), stage_id INTEGER NOT NULL CHECK(stage_id IN (0,1)), node_id TEXT NOT NULL, device_claim TEXT NOT NULL UNIQUE, instance_claim TEXT NOT NULL UNIQUE, PRIMARY KEY(task_id,stage_id), UNIQUE(task_id,node_id))",
    "CREATE TRIGGER inert_attempt_identity BEFORE UPDATE OF task_id,session_id,epoch,plan_claim,snapshot_ref ON inert_attempts BEGIN SELECT RAISE(ABORT,'immutable'); END",
    "CREATE TRIGGER inert_attempt_transition BEFORE UPDATE OF state ON inert_attempts WHEN NOT ((OLD.state='PENDING' AND NEW.state IN ('REVOKED','ISOLATED')) OR (OLD.state='REVOKED' AND NEW.state='ISOLATED')) BEGIN SELECT RAISE(ABORT,'immutable'); END",
    "CREATE TRIGGER inert_attempt_insert BEFORE INSERT ON inert_attempts WHEN NOT EXISTS(SELECT 1 FROM stored_tasks t JOIN task_inputs i ON t.task_id=i.task_id WHERE t.task_id=NEW.task_id AND t.state='STORED' AND i.snapshot_digest=NEW.snapshot_ref) BEGIN SELECT RAISE(ABORT,'binding'); END",
    "CREATE TRIGGER inert_cancel_guard BEFORE UPDATE OF state ON stored_tasks WHEN EXISTS(SELECT 1 FROM inert_attempts a WHERE a.task_id=NEW.task_id AND ((NEW.state='CANCELLED' AND a.state NOT IN ('REVOKED','ISOLATED')) OR (NEW.state='STOP_PENDING' AND a.state<>'ISOLATED'))) BEGIN SELECT RAISE(ABORT,'binding'); END",
)
for _table in ('inert_attempts', 'inert_holds'):
    _SCHEMA += (f"CREATE TRIGGER {_table}_no_delete BEFORE DELETE ON {_table} BEGIN SELECT RAISE(ABORT,'immutable'); END",)
_SCHEMA += ("CREATE TRIGGER inert_holds_no_update BEFORE UPDATE ON inert_holds BEGIN SELECT RAISE(ABORT,'immutable'); END",)
del _table


@dataclass(frozen=True, slots=True)
class InertRecord:
    task_id: str
    task_state: str
    attempt_state: str | None
    epoch: int | None
    authorization: str = 'unavailable'
    physical_stop: str = 'unknown'
    resource_release_allowed: bool = False


def _hash(value):
    _need(type(value) is str and len(value) == 64 and all(c in '0123456789abcdef' for c in value))


class InertAttemptLedger(InputTaskLedger):
    """Pending/revoked/isolated records only; epoch 2 and execution unsupported.

    Fresh schema only. No result commit API, verified flag, grant, start, release,
    migration or injected runtime adapters. No use alongside another task authority.
    """
    VERSION = 3
    SCHEMA = _SCHEMA
    STATES = frozenset(('STORED', 'CANCELLED', 'STOP_PENDING'))

    def _shared(self, conn):
        # Helpers participate in the caller's transaction; never begin or commit.
        _need(conn is self._db and conn.in_transaction)

    def _record(self, conn, owner, task_id):
        self._shared(conn)
        task, _ = self._metadata(owner, task_id)
        row = conn.execute('SELECT state,epoch FROM inert_attempts WHERE task_id=?', (task_id,)).fetchone()
        if row is not None:
            _need(conn.execute('SELECT count(*) FROM inert_holds WHERE task_id=?', (task_id,)).fetchone()[0] == 2)
        return InertRecord(task_id, task.state, row[0] if row else None, row[1] if row else None)

    def _pending(self, owner, task_id, session_id, epoch, plan_claim, assignments):
        _id(session_id)
        _need(type(epoch) is int and epoch == 1)
        _hash(plan_claim)
        # Tuple of (node, device claim, process instance claim), ordered by stage.
        _need(type(assignments) is tuple and len(assignments) == 2)
        for assignment in assignments:
            _need(type(assignment) is tuple and len(assignment) == 3)
            _id(assignment[0]); _id(assignment[1]); _hash(assignment[2])
        _need(all(assignments[0][i] != assignments[1][i] for i in range(3)))
        with self._transaction():
            conn = self._db
            self._shared(conn)
            task, _ = self._metadata(owner, task_id)
            _need(task.state == 'STORED')
            expected = (session_id, epoch, plan_claim, task.snapshot.digest, 'PENDING')
            old = conn.execute('SELECT session_id,epoch,plan_claim,snapshot_ref,state FROM inert_attempts WHERE task_id=?', (task_id,)).fetchone()
            if old is None:
                conn.execute('INSERT INTO inert_attempts VALUES(?,?,?,?,?,?)', (task_id, *expected))
                for stage, (node, device, instance) in enumerate(assignments):
                    conn.execute('INSERT INTO inert_holds VALUES(?,?,?,?,?)', (task_id, stage, node, device, instance))
            else:
                _need(old == expected)
                saved = conn.execute('SELECT node_id,device_claim,instance_claim FROM inert_holds WHERE task_id=? ORDER BY stage_id', (task_id,)).fetchall()
                _need(tuple(saved) == assignments)
            result = self._record(conn, owner, task_id)
        return result

    def record_pending(self, authenticated_owner, task_id, session_id, epoch, plan_claim, assignments):
        """Persist unverified claims only. A receipt is never a capability."""
        try:
            return self._pending(authenticated_owner, task_id, session_id, epoch, plan_claim, assignments)
        except Exception:
            pass
        del authenticated_owner, task_id, session_id, epoch, plan_claim, assignments
        raise LedgerRejected()

    def _fence(self, owner, task_id, isolate):
        with self._transaction():
            conn = self._db
            before = self._record(conn, owner, task_id)
            if before.attempt_state is not None:
                target = 'ISOLATED' if isolate else 'REVOKED'
                if before.attempt_state == 'PENDING' or (isolate and before.attempt_state == 'REVOKED'):
                    changed = conn.execute('UPDATE inert_attempts SET state=? WHERE task_id=? AND state=?', (target, task_id, before.attempt_state))
                    _need(changed.rowcount == 1)
            if before.task_state == 'STORED':
                target = 'STOP_PENDING' if isolate else 'CANCELLED'
                changed = conn.execute("UPDATE stored_tasks SET state=? WHERE task_id=? AND state='STORED'", (target, task_id))
                _need(changed.rowcount == 1)
            # Terminal cancellation stays terminal; isolation can only strengthen
            # attempt/hold interpretation. No hold is removed or made available.
            result = self._record(conn, owner, task_id)
        return result

    def cancel(self, authenticated_owner, task_id):
        try:
            return self._fence(authenticated_owner, task_id, False)
        except Exception:
            pass
        del authenticated_owner, task_id
        raise LedgerRejected()

    def quarantine(self, authenticated_owner, task_id):
        try:
            return self._fence(authenticated_owner, task_id, True)
        except Exception:
            pass
        del authenticated_owner, task_id
        raise LedgerRejected()
