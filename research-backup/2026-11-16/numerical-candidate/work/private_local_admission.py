"""Isolated local admission skeleton; NEVER production authentication.

No configured adapter means deny. IdentityAdapter is a trusted integration
boundary, not proof of a real IdP. No adapter or route is shipped. Local fences
order only locally recorded changes, never remote revocations. Fresh-only writes;
reopen is history-only. Arbitrary same-process mutation is outside this boundary.
"""
from abc import ABC, abstractmethod
import asyncio
from contextlib import contextmanager
from dataclasses import dataclass
import fcntl
import hashlib
import json
import math
import os
import sqlite3
import stat
import threading
import time
import uuid
from pathlib import Path

from private_source_bridge import LocalUserFence
from task_input_api import Principal


class AdmissionRejected(ValueError):
    def __init__(self):
        super().__init__('local admission unavailable')


@dataclass(frozen=True)
class Observation:
    issuer: str
    subject: str
    session: str
    audience: str
    expires_at: float


class IdentityAdapter(ABC):
    """Trusted bootstrap only: validate issuer/audience/signature/expiry/session.

    Implementations must define freshness and remote revocation/gap policies.
    Returning Observation is not itself authentication. No production adapter
    exists here; synthetic adapters belong exclusively in validation fixtures.
    """
    @abstractmethod
    async def observe(self, credential):
        raise NotImplementedError


def _need(value):
    if not value:
        raise AdmissionRejected()


def _text(value):
    _need(type(value) is str and 0 < len(value) <= 256
          and all(32 <= ord(c) != 127 for c in value))
    return value


def _fingerprint(value):
    _need(type(value) is str and 0 < len(value) <= 8192)
    return hashlib.sha256(value.encode()).hexdigest()


class LocalAdmission(LocalUserFence):
    """F in-memory fence; W serial writer. Never wait W while holding F.

    A -> F -> C is the unchanged bridge order. Writers: W -> short F ->
    release F -> own SQLite connection/transaction/close -> F publication.
    close: invalidate under F, release F, drain W, release file flock.
    """
    def __init__(self):
        self._f = threading.RLock()
        self._w = threading.Lock()
        self._state = 'CLOSED'
        self._pid = os.getpid()
        self._generation = 0
        self._revision = 0
        self._policy = 0
        self._subjects = {}
        self._sessions = {}
        self._cache = {}
        self._fd = None
        self._adapter = None
        self._history_only = True
        self._boot = uuid.uuid4().hex

    @classmethod
    def create(cls, path, *, issuer, audience, adapter=None, freshness=30.0):
        _text(issuer); _text(audience)
        _need(adapter is None or isinstance(adapter, IdentityAdapter))
        _need(type(freshness) in (int,float) and math.isfinite(freshness) and 0 < freshness <= 300)
        self = cls()
        self._path = os.fspath(path)
        self._issuer, self._audience = issuer, audience
        self._freshness, self._adapter = freshness, adapter
        try:
            self._fd = os.open(self._path, os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_NOFOLLOW, 0o600)
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._file_identity = self._inode(os.fstat(self._fd))
            c = self._connect('rw')
            try:
                c.execute('PRAGMA synchronous=FULL')
                c.execute('CREATE TABLE config (id INTEGER PRIMARY KEY CHECK(id=1), value TEXT NOT NULL)')
                c.execute('CREATE TABLE events (seq INTEGER PRIMARY KEY, op TEXT UNIQUE NOT NULL, kind TEXT NOT NULL, body TEXT NOT NULL)')
                c.execute('INSERT INTO config VALUES(1,?)', (json.dumps({'issuer':issuer,'audience':audience,'freshness':freshness,'boot':self._boot,'format':1}),))
                c.commit()
            finally:
                c.close()
            self._check_file()
            self._history_only = False
            self._state = 'READY'
            return self
        except BaseException:
            if self._fd is not None:
                os.close(self._fd); self._fd = None
            raise AdmissionRejected() from None

    @classmethod
    def open_existing(cls, path):
        self = cls(); self._path = os.fspath(path)
        try:
            self._fd = os.open(self._path, os.O_RDONLY | os.O_NOFOLLOW)
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._file_identity = self._inode(os.fstat(self._fd))
            # History observation only: no adapter, no restored cache, no writes.
            self._state = 'HISTORY'
            self.history()
            return self
        except BaseException:
            if self._fd is not None:
                os.close(self._fd); self._fd = None
            raise AdmissionRejected() from None

    @property
    def state(self):
        self._check_pid()
        with self._f:
            return self._state

    def _check_pid(self):
        # Must precede every lock: fork may inherit a permanently held mutex.
        _need(os.getpid() == self._pid)

    @staticmethod
    def _inode(info):
        _need(stat.S_ISREG(info.st_mode))
        return info.st_dev, info.st_ino

    def _check_file(self):
        self._check_pid()
        _need(self._fd is not None)
        _need(self._inode(os.lstat(self._path)) == self._file_identity
              == self._inode(os.fstat(self._fd)))

    def _connect(self, mode):
        # Trusted private parent directory required. These checks detect stable
        # replacement; they are NOT atomic protection against hostile path races.
        self._check_file()
        c=sqlite3.connect(Path(self._path).absolute().as_uri()+'?mode='+mode,uri=True,timeout=1)
        try:
            self._check_file()
            return c
        except BaseException:
            c.close()
            raise

    def _live(self):
        _need(os.getpid() == self._pid and not self._history_only and self._state == 'READY')

    def _stamp(self):
        return (self._boot,self._generation,self._revision,self._policy)

    def _poison(self):
        self._check_pid()
        with self._f:
            if self._state not in ('CLOSED','CLOSING','CLOSE_FAILED','HISTORY'):
                self._state = 'POISONED'
            self._generation += 1
            self._cache.clear()

    def verify(self, credential):
        if os.getpid() != self._pid:
            return None
        with self._f:
            try:
                self._live()
                _need(self._adapter is not None)
                # Cached admission must stay bound to the locked file too.
                # Only lstat/fstat: never acquire W, open SQLite, or call history
                # under F. Stable replacement/missing-file errors retire this
                # instance. Trusted directory remains required; not a TOCTOU
                # proof or a preemptible filesystem operation.
                try:
                    self._check_file()
                except (AdmissionRejected, OSError):
                    self._poison()
                    raise AdmissionRejected() from None
                fingerprint = _fingerprint(credential)
                entry = self._cache.get(fingerprint)
                _need(entry is not None)
                obs, deadline, stamp = entry
                _need(stamp == self._stamp())
                if time.time() >= obs.expires_at or time.monotonic() >= deadline:
                    # Observed expiry is terminal for this cached observation.
                    # Wall-clock rollback cannot restore it. A new trusted
                    # adapter observation is required for any later admission.
                    self._cache.pop(fingerprint, None)
                    raise AdmissionRejected()
                _need(not self._subjects.get(obs.subject,(0,False))[1])
                _need(not self._sessions.get((obs.subject,obs.session),(0,False))[1])
                return Principal(obs.issuer,obs.subject)
            except AdmissionRejected:
                return None

    @contextmanager
    def hold(self, credential):
        self._check_pid()
        with self._f:
            _need(self.verify(credential) is not None)
            yield
            _need(self.verify(credential) is not None)

    async def prepare_user(self, credential):
        self._check_pid()
        fingerprint = _fingerprint(credential)
        with self._f:
            self._live()
            adapter = self._adapter
            _need(adapter is not None)
            stamp = self._stamp()
            subjects, sessions = dict(self._subjects), dict(self._sessions)
            started = time.monotonic()
        obs = await adapter.observe(credential)
        self._check_pid()
        _need(type(obs) is Observation)
        _text(obs.subject); _text(obs.session)
        _need(obs.issuer == self._issuer and obs.audience == self._audience)
        _need(type(obs.expires_at) in (int,float) and math.isfinite(obs.expires_at)
              and obs.expires_at > time.time())
        deadline = started + self._freshness
        _need(time.monotonic() < deadline)
        with self._f:
            self._live()
            _need(adapter is self._adapter and stamp == self._stamp())
            _need(subjects.get(obs.subject,(0,False)) == self._subjects.get(obs.subject,(0,False)))
            _need(sessions.get((obs.subject,obs.session),(0,False)) == self._sessions.get((obs.subject,obs.session),(0,False)))
            _need(not self._subjects.get(obs.subject,(0,False))[1]
                  and not self._sessions.get((obs.subject,obs.session),(0,False))[1])
        await self._run_write(stamp, 'observe', {'subject':obs.subject,'session':obs.session,
                     'expires_at':obs.expires_at}, (fingerprint,obs,deadline))
        principal = self.verify(credential)
        _need(principal is not None)
        return principal

    async def revoke_subject(self, subject):
        await self._change('revoke_subject', {'subject':_text(subject)})

    async def revoke_session(self, subject, session):
        await self._change('revoke_session', {'subject':_text(subject),'session':_text(session)})

    async def advance_policy(self):
        await self._change('policy', {})

    async def _change(self, kind, body):
        self._check_pid()
        with self._f:
            self._live(); stamp = self._stamp()
        await self._run_write(stamp,kind,body,None)

    @staticmethod
    def _commit(c):
        c.commit()

    def _write(self, stamp, kind, body, admission):
        self._check_pid()
        with self._w:
            # A stale/pre-entry refusal has no uncertain transaction to poison.
            with self._f:
                self._live(); _need(stamp == self._stamp())
                self._state = 'BUSY'
                self._cache.clear()
            try:
                c = self._connect('rw')
                try:
                    c.execute('PRAGMA synchronous=FULL')
                    c.execute('BEGIN IMMEDIATE')
                    c.execute('INSERT INTO events VALUES(?,?,?,?)',
                              (self._revision+1,uuid.uuid4().hex,kind,json.dumps(body,sort_keys=True)))
                    self._check_file()
                    self._commit(c)
                finally:
                    c.close()
                self._check_file()
                with self._f:
                    _need(self._state == 'BUSY' and stamp == self._stamp() and os.getpid() == self._pid)
                    if admission is not None:
                        _, obs, deadline = admission
                        _need(time.time() < obs.expires_at and time.monotonic() < deadline)
                    if kind == 'revoke_subject':
                        key=body['subject']; v=self._subjects.get(key,(0,False))[0]
                        self._subjects[key]=(v+1,True)
                    elif kind == 'revoke_session':
                        key=(body['subject'],body['session']); v=self._sessions.get(key,(0,False))[0]
                        self._sessions[key]=(v+1,True)
                    elif kind == 'policy':
                        self._policy += 1
                    self._revision += 1
                    if admission is not None:
                        fp,obs,deadline=admission
                        self._cache[fp]=(obs,deadline,self._stamp())
                    self._state = 'READY'
            except BaseException:
                self._poison()
                raise AdmissionRejected() from None

    async def _settle(self, job, *, poison_on_cancel):
        cancelled = False
        while not job.done():
            try:
                await asyncio.shield(job)
            except asyncio.CancelledError:
                cancelled = True
                if poison_on_cancel:
                    self._poison()
            except BaseException:
                break
        if cancelled:
            # Retrieve failure too: no detached task/worker or unobserved error.
            if not job.cancelled():
                job.exception()
            raise asyncio.CancelledError()
        return job.result()

    async def _run_write(self, *args):
        job=asyncio.create_task(asyncio.to_thread(self._write,*args))
        return await self._settle(job,poison_on_cancel=True)

    async def close(self):
        self._check_pid()
        with self._f:
            _need(os.getpid() == self._pid)
            if self._state == 'CLOSED':
                return
            _need(self._state != 'CLOSE_FAILED')
            self._state = 'CLOSING'
            self._generation += 1
            self._cache.clear()
        job=asyncio.create_task(asyncio.to_thread(self._finish_close))
        await self._settle(job,poison_on_cancel=False)

    def _finish_close(self):
        self._check_pid()
        with self._w:
            with self._f:
                _need(self._state != 'CLOSE_FAILED')
                if self._fd is not None:
                    fd = self._fd
                    # Consume close ownership before entering an uncertain OS
                    # boundary. Never retry an integer that may be reused.
                    self._fd = None
                    try:
                        os.close(fd)
                    except BaseException:
                        self._state = 'CLOSE_FAILED'
                        raise AdmissionRejected() from None
                self._state = 'CLOSED'

    def history(self):
        # Private administration only; must never be called from a held fence.
        self._check_pid()
        with self._w:
            with self._f:
                _need(self._state == 'HISTORY' and os.getpid() == self._pid)
            c=self._connect('ro')
            try:
                _need(json.loads(c.execute('SELECT value FROM config WHERE id=1').fetchone()[0])['format'] == 1)
                return tuple(c.execute('SELECT seq,op,kind,body FROM events ORDER BY seq'))
            finally:
                c.close()
