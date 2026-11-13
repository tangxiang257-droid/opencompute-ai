"""Isolated storage ledger, not scheduler admission or an authentication service.

Use a dedicated caller-owned file SQLite connection (autocommit, no sharing or
concurrent outside use), and the private snapshot directory FD. Caller secures
DB path/sidecars and authenticates owner independently of client payloads.
No task from this module is runnable. No file/DB cross-media atomicity claimed.
"""
from contextlib import contextmanager
from dataclasses import dataclass, asdict
import hashlib
import json
import re
import secrets
import sqlite3

from token_input_snapshot import InputBinding, SnapshotRef, publish_input, read_input

_ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z')
_HASH = re.compile(r'[0-9a-f]{64}\Z')


_SCHEMA = (
    "CREATE TABLE stored_tasks(task_id TEXT PRIMARY KEY, owner_id TEXT NOT NULL, request_id TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('STORED','CANCELLED')), UNIQUE(owner_id,request_id))",
    'CREATE TABLE task_inputs(task_id TEXT PRIMARY KEY REFERENCES stored_tasks(task_id), binding TEXT NOT NULL, snapshot_digest TEXT NOT NULL)',
    'CREATE TABLE input_requests(owner_id TEXT NOT NULL, idem_key TEXT NOT NULL, fingerprint TEXT NOT NULL, task_id TEXT NOT NULL UNIQUE REFERENCES stored_tasks(task_id), PRIMARY KEY(owner_id,idem_key))',
    "CREATE TRIGGER task_identity_immutable BEFORE UPDATE OF task_id,owner_id,request_id ON stored_tasks BEGIN SELECT RAISE(ABORT,'immutable'); END",
    "CREATE TRIGGER task_state_monotone BEFORE UPDATE OF state ON stored_tasks WHEN NOT (OLD.state='STORED' AND NEW.state='CANCELLED') BEGIN SELECT RAISE(ABORT,'immutable'); END",
)
for _table in ('stored_tasks', 'task_inputs', 'input_requests'):
    _SCHEMA += (f"CREATE TRIGGER {_table}_no_delete BEFORE DELETE ON {_table} BEGIN SELECT RAISE(ABORT,'immutable'); END",)
for _table in ('task_inputs', 'input_requests'):
    _SCHEMA += (f"CREATE TRIGGER {_table}_no_update BEFORE UPDATE ON {_table} BEGIN SELECT RAISE(ABORT,'immutable'); END",)
del _table


class LedgerRejected(ValueError):
    def __init__(self):
        super().__init__('input ledger operation rejected')


@dataclass(frozen=True, slots=True)
class StoredTask:
    """Immutable storage metadata, never execution permission."""
    task_id: str
    owner_id: str
    request_id: str
    snapshot: SnapshotRef
    state: str


def _need(condition):
    if not condition:
        raise LedgerRejected()


def _id(value):
    _need(type(value) is str and len(value) <= 128 and _ID.fullmatch(value))


def _canonical(binding):
    return json.dumps(asdict(binding), sort_keys=True, separators=(',', ':'),
                      ensure_ascii=True, allow_nan=False)


def _fingerprint(reference):
    return hashlib.sha256(b'opencompute/stored-task-input/v1\0' +
                          reference.digest.encode('ascii')).hexdigest()


def _pairs(pairs):
    _need(len(pairs) == 6)
    value = {}
    for key, item in pairs:
        _need(key not in value)
        value[key] = item
    return value


class InputTaskLedger:
    """Dedicated connection and directory FD owned and kept open by caller.

    Connection is reserved exclusively for the ledger. No outside SQL, config,
    callbacks, temp objects or attachments during its lifetime, even sequentially.
    Authentication and secure DB/sidecar path provisioning are caller obligations.
    """
    VERSION = 1
    SCHEMA = _SCHEMA
    STATES = frozenset(('STORED', 'CANCELLED'))

    def __init__(self, connection, snapshot_directory_fd, *, initialize=False):
        try:
            self._open(connection, snapshot_directory_fd, initialize)
            return
        except Exception:
            pass
        self.__dict__.clear()
        del connection, snapshot_directory_fd, initialize
        raise LedgerRejected()

    def _open(self, connection, snapshot_directory_fd, initialize):
        _need(type(initialize) is bool)
        _need(type(connection) is sqlite3.Connection)
        _need(connection.isolation_level is None and not connection.in_transaction)
        _need(connection.row_factory is None and connection.text_factory is str)
        _need(type(snapshot_directory_fd) is int and snapshot_directory_fd >= 0)
        self._db = connection
        self._directory = snapshot_directory_fd
        files = connection.execute('PRAGMA database_list').fetchall()
        _need(len(files) == 1 and files[0][1] == 'main' and bool(files[0][2]))
        connection.execute('PRAGMA foreign_keys=ON')
        connection.execute('PRAGMA synchronous=EXTRA')
        connection.execute('PRAGMA busy_timeout=5000')
        _need(connection.execute('PRAGMA foreign_keys').fetchone()[0] == 1)
        _need(connection.execute('PRAGMA synchronous').fetchone()[0] == 3)
        # WAL is intentionally not enabled; require the simple dedicated DB mode.
        _need(connection.execute('PRAGMA journal_mode').fetchone()[0] == 'delete')
        with self._transaction(check_schema=False):
            version = connection.execute('PRAGMA user_version').fetchone()[0]
            if initialize:
                _need(version == 0)
                _need(not connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE name NOT GLOB 'sqlite_*'").fetchone())
                self._initialize()
            else:
                _need(version == self.VERSION)
            self._verify_schema()

    @contextmanager
    def _transaction(self, *, check_schema=True):
        _need(not self._db.in_transaction and self._db.isolation_level is None)
        _need(self._db.row_factory is None and self._db.text_factory is str)
        files = self._db.execute('PRAGMA database_list').fetchall()
        _need(len(files) == 1 and files[0][1] == 'main' and bool(files[0][2]))
        _need(self._db.execute('PRAGMA journal_mode').fetchone()[0] == 'delete')
        _need(self._db.execute('PRAGMA foreign_keys').fetchone()[0] == 1)
        _need(self._db.execute('PRAGMA synchronous').fetchone()[0] == 3)
        self._db.execute('BEGIN IMMEDIATE')
        try:
            if check_schema:
                self._verify_schema()
            yield
            self._db.execute('COMMIT')
        except BaseException:
            if self._db.in_transaction:
                self._db.execute('ROLLBACK')
            raise

    def _initialize(self):
        for sql in self.SCHEMA:
            self._db.execute(sql)
        self._db.execute('PRAGMA user_version=' + str(self.VERSION))

    def _verify_schema(self):
        _need(self._db.execute('PRAGMA user_version').fetchone()[0] == self.VERSION)
        rows = self._db.execute("SELECT sql FROM sqlite_master WHERE name NOT GLOB 'sqlite_*'").fetchmany(len(self.SCHEMA) + 1)
        _need(len(rows) == len(self.SCHEMA) and {row[0] for row in rows} == set(self.SCHEMA))

    def _metadata(self, owner, task_id):
        _id(owner)
        _id(task_id)
        row = self._db.execute('''SELECT t.task_id,t.owner_id,t.request_id,t.state,
            i.binding,i.snapshot_digest,r.owner_id,r.fingerprint FROM stored_tasks t
            JOIN task_inputs i ON i.task_id=t.task_id
            JOIN input_requests r ON r.task_id=t.task_id
            WHERE t.task_id=? AND t.owner_id=?''', (task_id, owner)).fetchone()
        _need(row is not None)
        task, stored_owner, request, state, encoded, digest, idem_owner, fingerprint = row
        _need(stored_owner == owner == idem_owner and state in self.STATES)
        _id(request)
        _need(type(encoded) is str and 0 < len(encoded) <= 2048)
        _need(type(digest) is str and len(digest) == 64 and _HASH.fullmatch(digest))
        # Exact canonical re-encoding also rejects unknown fields and alternate encodings.
        value = json.loads(encoded, object_pairs_hook=_pairs)
        binding = InputBinding(**value)
        _need(_canonical(binding) == encoded)
        _need(binding.owner_id == owner and binding.request_id == request)
        reference = SnapshotRef(digest)
        _need(fingerprint == _fingerprint(reference))
        return StoredTask(task, owner, request, reference, state), binding

    def _create(self, authenticated_owner, idempotency_key, binding, tokens):
        _id(authenticated_owner)
        _id(idempotency_key)
        _need(type(binding) is InputBinding and binding.owner_id == authenticated_owner)
        # No DB task exists if publication fails. DB failure may leave this object orphaned.
        reference = publish_input(self._directory, binding, tokens)
        with self._transaction():
            return self._commit_published_input(self._db, authenticated_owner,
                                                idempotency_key, binding, reference)

    def _commit_published_input(self, connection, authenticated_owner,
                                idempotency_key, binding, reference):
        """Participate in the caller's SAME transaction; never begin/commit here.

        A published reference is a locator, not authority. Read/verify its actual
        bytes and binding. Files remain outside the DB transaction; rollback does
        not remove them. Callers must retain normal uncertain-COMMIT handling.
        """
        _need(connection is self._db and self._db.in_transaction)
        _id(authenticated_owner); _id(idempotency_key)
        _need(type(binding) is InputBinding and binding.owner_id == authenticated_owner)
        _need(type(reference) is SnapshotRef)
        read_input(self._directory, reference, binding)
        fingerprint = _fingerprint(reference)
        prior = self._db.execute('SELECT fingerprint,task_id FROM input_requests WHERE owner_id=? AND idem_key=?',
                                 (authenticated_owner, idempotency_key)).fetchone()
        if prior is not None:
            _need(prior[0] == fingerprint)
            task, saved = self._metadata(authenticated_owner, prior[1])
            _need(saved == binding and task.snapshot == reference)
            read_input(self._directory, task.snapshot, saved)
        else:
            task_id = secrets.token_hex(16)
            self._db.execute('INSERT INTO stored_tasks VALUES(?,?,?,?)',
                             (task_id, authenticated_owner, binding.request_id, 'STORED'))
            self._db.execute('INSERT INTO task_inputs VALUES(?,?,?)',
                             (task_id, _canonical(binding), reference.digest))
            self._db.execute('INSERT INTO input_requests VALUES(?,?,?,?)',
                             (authenticated_owner, idempotency_key, fingerprint, task_id))
            task, saved = self._metadata(authenticated_owner, task_id)
            read_input(self._directory, task.snapshot, saved)
        return task

    def create(self, authenticated_owner, idempotency_key, binding, tokens):
        """Auth owner is a trusted caller argument, not a client-supplied claim."""
        try:
            return self._create(authenticated_owner, idempotency_key, binding, tokens)
        except Exception:
            pass
        del authenticated_owner, idempotency_key, binding, tokens
        raise LedgerRejected()

    def _read_task(self, authenticated_owner, task_id):
        with self._transaction():
            task, binding = self._metadata(authenticated_owner, task_id)
            _need(task.state == 'STORED')
            stored = read_input(self._directory, task.snapshot, binding)
        return task, stored

    def read(self, authenticated_owner, task_id):
        """Revalidate stored data; cancelled inputs are not returned for consumption."""
        try:
            return self._read_task(authenticated_owner, task_id)
        except Exception:
            pass
        del authenticated_owner, task_id
        raise LedgerRejected()

    def _cancel_task(self, authenticated_owner, task_id):
        with self._transaction():
            task, _ = self._metadata(authenticated_owner, task_id)
            if task.state == 'STORED':
                self._db.execute("UPDATE stored_tasks SET state='CANCELLED' WHERE task_id=? AND owner_id=?", (task_id, authenticated_owner))
            task, _ = self._metadata(authenticated_owner, task_id)
        return task

    def cancel(self, authenticated_owner, task_id):
        """Storage-state cancellation only: no execution, stop or release evidence."""
        try:
            return self._cancel_task(authenticated_owner, task_id)
        except Exception:
            pass
        del authenticated_owner, task_id
        raise LedgerRejected()
