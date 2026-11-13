"""Linux SQLite boundary anchored in an explicitly provisioned directory FD.

PathTrust is trusted deployment configuration, NEVER request input. Possession
of an arbitrary client FD is not authentication. No implicit trust in observed
UIDs, namespace-wide root ownership, or same-UID adversarial processes.
"""
from contextlib import contextmanager
from dataclasses import dataclass
import os
import sqlite3
import stat
import sys

from snapshot_task_ledger import InputTaskLedger, LedgerRejected

_NAME = 'inputs.sqlite'


@dataclass(frozen=True, slots=True)
class PathTrust:
    """Explicit trust anchor capability, relative components and configured UIDs.

    Caller securely provisions anchor_fd; ancestors ABOVE it are outside this
    resolver's checks, NOT outside deployment trust: SQLite canonicalization
    requires the entire resolved DB/journal namespace (including ancestors above
    the anchor) to remain trusted and stable. Defaults trust system/sticky UIDs 0.
    The effective service UID may own private or non-shared protected directories.
    """
    anchor_fd: int
    components: tuple[str, ...]
    system_uids: tuple[int, ...] = (0,)
    sticky_uids: tuple[int, ...] = (0,)


class DatabaseRejected(LedgerRejected):
    """Payload-free failure stage; no path, UID, SQL, token or internal cause."""
    def __init__(self, stage):
        super().__init__()
        self.stage = stage


def _need(condition, stage):
    if not condition:
        raise DatabaseRejected(stage)


@contextmanager
def _stage(name):
    try:
        yield
    except DatabaseRejected:
        raise
    except Exception:
        raise DatabaseRejected(name) from None


def _validate(trust):
    _need(sys.platform == 'linux' and type(trust) is PathTrust, 'config')
    _need(type(trust.anchor_fd) is int and trust.anchor_fd >= 0, 'config')
    _need(type(trust.components) is tuple and 1 <= len(trust.components) <= 32, 'config')
    for part in trust.components:
        _need(type(part) is str and 1 <= len(part) <= 128 and part.isascii()
              and part not in ('.', '..') and all(c.isalnum() or c in '._-' for c in part), 'config')
    for uids in (trust.system_uids, trust.sticky_uids):
        _need(type(uids) is tuple and len(uids) <= 16, 'config')
        _need(all(type(uid) is int and 0 <= uid < 2**32 - 1 for uid in uids), 'config')


def _ancestor(info, trust, stage):
    _need(stat.S_ISDIR(info.st_mode), stage)
    protected = not (info.st_mode & 0o022)
    system = info.st_uid in trust.system_uids and protected
    service = info.st_uid == os.geteuid() and protected
    shared = (info.st_uid in trust.sticky_uids and
              bool(info.st_mode & stat.S_ISVTX) and bool(info.st_mode & 0o022))
    _need(system or service or shared, stage)


def _directory(trust):
    _validate(trust)
    fd = None
    try:
        with _stage('anchor'):
            fd = os.dup(trust.anchor_fd)
            _ancestor(os.fstat(fd), trust, 'anchor')
        for index, part in enumerate(trust.components):
            with _stage('resolve'):
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                                dir_fd=fd)
                previous, fd = fd, child
                os.close(previous)
            info = os.fstat(fd)
            if index == len(trust.components) - 1:
                _need(info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o700,
                      'private_directory')
            else:
                _ancestor(info, trust, 'ancestor')
        result, fd = fd, None
        return result
    finally:
        if fd is not None:
            os.close(fd)


def _file(directory, name, required, stage):
    with _stage(stage):
        try:
            info = os.stat(name, dir_fd=directory, follow_symlinks=False)
        except FileNotFoundError:
            _need(not required, stage)
            return None
        _need(stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid()
              and stat.S_IMODE(info.st_mode) == 0o600 and info.st_nlink == 1, stage)
        return info


def _check(directory, trust, expected=None):
    fresh = _directory(trust)
    try:
        first, second = os.fstat(directory), os.fstat(fresh)
        _need((first.st_dev, first.st_ino) == (second.st_dev, second.st_ino), 'namespace')
    finally:
        os.close(fresh)
    with _stage('sidecar'):
        _need(set(os.listdir(directory)) <= {_NAME, _NAME + '-journal'}, 'sidecar')
    current = _file(directory, _NAME, True, 'file')
    _file(directory, _NAME + '-journal', False, 'sidecar')
    if expected is not None:
        _need((current.st_dev, current.st_ino) == expected, 'namespace')
    return current.st_dev, current.st_ino


class ManagedLedger:
    """Owns connection, directory FD and duplicated anchor; caller owns snapshot FD."""
    def __init__(self):
        raise DatabaseRejected('config')

    def _invoke(self, operation, args):
        _need(self._connection is not None, 'closed')
        _check(self._directory, self._trust, self._identity)
        return getattr(self._ledger, operation)(*args)

    def create(self, authenticated_owner, idempotency_key, binding, tokens):
        stage = 'ledger'
        try:
            return self._invoke('create', (authenticated_owner, idempotency_key, binding, tokens))
        except DatabaseRejected as error:
            stage = error.stage
        except Exception:
            pass
        del authenticated_owner, idempotency_key, binding, tokens
        raise DatabaseRejected(stage)

    def read(self, authenticated_owner, task_id):
        stage = 'ledger'
        try:
            return self._invoke('read', (authenticated_owner, task_id))
        except DatabaseRejected as error:
            stage = error.stage
        except Exception:
            pass
        del authenticated_owner, task_id
        raise DatabaseRejected(stage)

    def cancel(self, authenticated_owner, task_id):
        stage = 'ledger'
        try:
            return self._invoke('cancel', (authenticated_owner, task_id))
        except DatabaseRejected as error:
            stage = error.stage
        except Exception:
            pass
        del authenticated_owner, task_id
        raise DatabaseRejected(stage)

    def _close(self):
        connection, directory, trust = self._connection, self._directory, self._trust
        self._connection = self._directory = self._ledger = self._trust = None
        try:
            if connection is not None:
                connection.close()
        finally:
            try:
                if directory is not None:
                    os.close(directory)
            finally:
                if trust is not None:
                    os.close(trust.anchor_fd)

    def close(self):
        try:
            self._close()
            return
        except Exception:
            pass
        raise DatabaseRejected('cleanup')

    def __enter__(self):
        _need(self._connection is not None, 'closed')
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()
        return False


def _open(config, snapshot_fd, initialize, *, _factory=InputTaskLedger):
    directory = connection = trust = None
    try:
        _validate(config)
        with _stage('anchor'):
            trust = PathTrust(os.dup(config.anchor_fd), config.components, config.system_uids, config.sticky_uids)
        directory = _directory(trust)
        if initialize:
            with _stage('initialize'):
                _need(not os.listdir(directory), 'initialize')
                fd = os.open(_NAME, os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                             os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=directory)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
                os.fsync(directory)
        identity = _check(directory, trust)
        # Pinned descriptor bounds our traversal, not all SQLite/VFS traversal.
        # Canonicalization can use absolute DB/journal paths above this anchor;
        # deployment MUST trust that entire stable namespace and /proc for the
        # connection lifetime. Anchor checks cannot prove that external premise.
        with _stage('database_open'):
            connection = sqlite3.connect(f'file:/proc/self/fd/{directory}/{_NAME}?mode=rw',
                                         uri=True, isolation_level=None, timeout=5)
            resolved = connection.execute('PRAGMA database_list').fetchone()[2]
            opened = os.stat(resolved, follow_symlinks=False)
            _need((opened.st_dev, opened.st_ino) == identity, 'namespace')
        _check(directory, trust, identity)
        with _stage('schema'):
            ledger = _factory(connection, snapshot_fd, initialize=initialize)
        _check(directory, trust, identity)
        with _stage('sync'):
            os.fsync(directory)
        handle = object.__new__(ManagedLedger)
        handle._connection, handle._directory = connection, directory
        handle._trust, handle._identity, handle._ledger = trust, identity, ledger
        connection = directory = trust = None
        return handle
    finally:
        try:
            if connection is not None:
                connection.close()
        finally:
            try:
                if directory is not None:
                    os.close(directory)
            finally:
                if trust is not None:
                    os.close(trust.anchor_fd)


def initialize_new(trusted_location, snapshot_directory_fd):
    """Only explicit trusted configuration; failure retains artifacts, no cleanup."""
    stage = 'storage'
    try:
        return _open(trusted_location, snapshot_directory_fd, True)
    except DatabaseRejected as error:
        stage = error.stage
    except Exception:
        pass
    del trusted_location, snapshot_directory_fd
    raise DatabaseRejected(stage)


def open_existing(trusted_location, snapshot_directory_fd):
    """Require existing exact schema. Never initialize, migrate or overwrite."""
    stage = 'storage'
    try:
        return _open(trusted_location, snapshot_directory_fd, False)
    except DatabaseRejected as error:
        stage = error.stage
    except Exception:
        pass
    del trusted_location, snapshot_directory_fd
    raise DatabaseRejected(stage)
