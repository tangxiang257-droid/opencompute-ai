"""Fresh-only experimental plan journal; never executes or migrates legacy data.

Trusted stable local parent/path and non-adversarial same-UID access required.
The injected synchronous context manager must authenticate and hold its own
revocation fence until exit. SQLite cannot supply that external guarantee.
"""
from contextlib import contextmanager
import hashlib
import os
from pathlib import Path
import stat
import threading
from pydantic import TypeAdapter
from .contracts import Task, Session, Stage, State, Identifier
from .store import P1Store, Conflict, canonical
from .tile_contract_v2 import TileV2, validate_prefill_plan


class V2Journal:
    @classmethod
    def create(cls, path, task, session, stages, *, authorize):
        if not callable(authorize):
            raise TypeError('synchronous authorization context factory required')
        task=Task.model_validate(task); session=Session.model_validate(session)
        stages=tuple(Stage.model_validate(s) for s in stages)
        path=Path(path)
        obj=cls.__new__(cls)
        obj._authorize=authorize; obj._lock=threading.RLock(); obj._closed=False
        with authorize() as owner:
            TypeAdapter(Identifier).validate_python(owner,strict=True)
            if owner != task.owner_id or owner != session.owner_id:
                raise PermissionError('owner mismatch')
            # No parent creation, reopening, migration, or replacement.
            fd=os.open(path,os.O_CREAT|os.O_EXCL|os.O_RDWR|os.O_NOFOLLOW,0o600)
            try:
                s=os.fstat(fd); obj._identity=(s.st_dev,s.st_ino)
            finally:os.close(fd)
            obj._path=path
            try:
                obj._store=P1Store(path)
                obj._store.create_task(task,event_id='v2:create')
                obj._store.bind_session(session,stages,event_id='v2:bind')
                with obj._store._tx() as c:
                    c.execute('CREATE TABLE p1_v2_protocol(version INTEGER PRIMARY KEY CHECK(version=2))')
                    c.execute('INSERT INTO p1_v2_protocol VALUES(2)')
                    c.execute('CREATE TABLE p1_v2_sessions(session_id TEXT PRIMARY KEY REFERENCES p1_sessions(id))')
                    c.execute('INSERT INTO p1_v2_sessions VALUES(?)',(session.session_id,))
                    c.execute('CREATE TABLE p1_v2_plans(session_id TEXT NOT NULL REFERENCES p1_v2_sessions(session_id), epoch INTEGER NOT NULL, body TEXT NOT NULL, digest TEXT NOT NULL, PRIMARY KEY(session_id,epoch))')
                    c.execute('CREATE TABLE p1_v2_edges(plan_digest TEXT PRIMARY KEY, body TEXT NOT NULL)')
                    c.execute('CREATE TABLE p1_v2_receipts(plan_digest TEXT NOT NULL REFERENCES p1_v2_edges(plan_digest), tile_id INTEGER NOT NULL, digest TEXT NOT NULL, PRIMARY KEY(plan_digest,tile_id))')
                    for table in ('p1_tiles','p1_tile_states'):
                        for action in ('INSERT','UPDATE'):
                            c.execute(f'''CREATE TRIGGER v2_block_{table}_{action} BEFORE {action} ON {table}
                            WHEN EXISTS(SELECT 1 FROM p1_v2_sessions WHERE session_id=NEW.session_id)
                            BEGIN SELECT RAISE(ABORT,'V2 session rejects legacy tile writes'); END''')
                obj._session_id=session.session_id;obj._task_id=task.task_id
                obj._check_file()
            except BaseException as exc:
                obj._closed=True
                # Diagnostics are best-effort and cannot replace the original failure.
                try:
                    BaseException.add_note(exc, 'Fresh initialization failed; partial files may remain. No automatic cleanup or reopen is supported.')
                except BaseException:
                    pass
                raise
        return obj

    def _check_file(self):
        if self._closed:raise Conflict('journal closed')
        try:s=self._path.lstat()
        except OSError:raise Conflict('journal file unavailable') from None
        if not stat.S_ISREG(s.st_mode) or (s.st_dev,s.st_ino)!=self._identity:
            raise Conflict('journal file identity changed')

    @contextmanager
    def _authorized(self):
        with self._lock:
            self._check_file()
            with self._authorize() as owner:
                TypeAdapter(Identifier).validate_python(owner,strict=True)
                yield owner

    @staticmethod
    def _record_event(c,event_id,payload,result):
        c.execute('INSERT INTO p1_events VALUES(?,?,?)',(event_id,payload,canonical(result)))

    def register(self,tiles,*,hidden_size,vocab_size,event_id):
        TypeAdapter(Identifier).validate_python(event_id,strict=True)
        if type(tiles) is not tuple or not 2<=len(tiles)<=128:
            raise Conflict('bounded complete plan required')
        parsed=tuple(TileV2.model_validate(t) for t in tiles)
        body={'protocol':2,'session_id':self._session_id,'hidden_size':hidden_size,
              'vocab_size':vocab_size,'tiles':[t.model_dump(mode='json') for t in parsed]}
        payload=canonical(['register_v2',body])
        digest=hashlib.sha256(payload.encode()).hexdigest()
        with self._authorized() as owner, self._store._tx() as c:
            session=self._store._load(c,'p1_sessions',self._session_id,Session)
            task=self._store._load(c,'p1_tasks',self._task_id,Task)
            if owner!=session.owner_id or owner!=task.owner_id:
                raise PermissionError('owner mismatch')
            old=c.execute('SELECT payload,result FROM p1_events WHERE id=?',(event_id,)).fetchone()
            if old:
                if old[0]!=payload:raise Conflict('event identity conflict')
                # Receipt is historical, not a statement of current executability.
                return {'plan_digest':digest,'historical_receipt':True,'execution_allowed':False}
            if session.epoch!=parsed[0].epoch or session.state!=State.PREPARING or task.state!=State.PREPARING:
                raise Conflict('stale epoch or non-preparing task/session')
            stages=tuple(Stage.model_validate_json(row[0]) for row in c.execute(
                'SELECT body FROM p1_stages WHERE session_id=? ORDER BY stage_id',(session.session_id,)))
            validate_prefill_plan(task,session,stages,parsed,hidden_size=hidden_size,vocab_size=vocab_size)
            if c.execute('SELECT 1 FROM p1_v2_plans WHERE session_id=? AND epoch=?',(session.session_id,session.epoch)).fetchone():
                raise Conflict('plan already registered')
            c.execute('INSERT INTO p1_v2_plans VALUES(?,?,?,?)',(session.session_id,session.epoch,canonical(body),digest))
            result={'plan_digest':digest,'historical_receipt':False,'execution_allowed':False}
            self._record_event(c,event_id,payload,result)
            return result

    def cancel(self,*,event_id):
        # Existing task-wide semantics intentionally retained, not cancel-epoch.
        with self._authorized() as owner:
            return self._store.cancel_task(self._task_id,owner_id=owner,event_id=event_id)

    def close(self):
        with self._lock:self._closed=True
