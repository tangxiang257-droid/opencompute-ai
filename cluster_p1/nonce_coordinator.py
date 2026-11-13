"""Private single-Linux-coordinator provisioning. No remote credentials or recovery.

One stable trusted database, parent directory and same-UID Python assembly.
Deleting/rolling back this database defeats its history; no global replay claim.
"""
from contextlib import contextmanager
from dataclasses import dataclass
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import threading
from .contracts import Task,Session,Stage,Digest,Identifier
from pydantic import TypeAdapter
from .tile_contract_v2 import TileV2,validate_prefill_plan
from .store import Conflict,canonical
from .local_attempt import LocalAttemptJournal,_non_suppressing
from .handoff import EdgeBinding
from .bound_connection import BoundReceiptConnection

@dataclass(frozen=True,slots=True,eq=False)
class _Reservation:
    body: str

class _CoordinatedSender(LocalAttemptJournal):
    def _plan(self,c,owner,*,active,stage_id=0):
        d=json.loads(self._coordinator_body)
        task=self._store._load(c,'p1_tasks',self._task_id,Task)
        session=self._store._load(c,'p1_sessions',self._session_id,Session)
        stages=[Stage.model_validate_json(x[0]) for x in c.execute(
            'SELECT body FROM p1_stages WHERE session_id=? ORDER BY stage_id',(self._session_id,))]
        expected_task=dict(d['task'],state='preparing') # P1Store.bind_session's fixed initial transition
        if (canonical(task.model_dump(mode='json'))!=canonical(expected_task) or
            canonical(session.model_dump(mode='json'))!=canonical(d['session']) or
            canonical([x.model_dump(mode='json') for x in stages])!=canonical(d['stages'])):
            raise Conflict('coordinator-fixed sender declarations changed')
        result=super()._plan(c,owner,active=active,stage_id=stage_id)
        if result[1]!=d['plan_digest']:raise Conflict('coordinator-fixed sender plan changed')
        return result

class NonceCoordinator:
    @classmethod
    def initialize(cls,path,*,authorize):
        return cls._open(path,authorize,True)

    @classmethod
    def open_existing(cls,path,*,authorize):
        return cls._open(path,authorize,False)

    @classmethod
    def _open(cls,path,authorize,fresh):
        if not callable(authorize):raise TypeError('owner fence required')
        obj=cls.__new__(cls);obj._path=Path(path).absolute();obj._authorize=authorize
        obj._lock=threading.RLock();obj._closed=False;obj._retired=False;obj._reservations={}
        flags=os.O_RDWR|os.O_NOFOLLOW|(os.O_CREAT|os.O_EXCL if fresh else 0)
        try:
            with _non_suppressing(authorize()) as owner:
                TypeAdapter(Identifier).validate_python(owner,strict=True)
                obj._owner=owner;fd=os.open(obj._path,flags,0o600);obj._fd=fd
                try:
                    s=os.fstat(fd)
                    if not stat.S_ISREG(s.st_mode):raise Conflict('regular registry file required')
                    obj._identity=(s.st_dev,s.st_ino)
                    fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
                    with obj._connection() as c:
                        if fresh:
                            c.execute('BEGIN IMMEDIATE')
                            c.execute('CREATE TABLE protocol(version INTEGER PRIMARY KEY CHECK(version=1),owner TEXT NOT NULL)')
                            c.execute('INSERT INTO protocol VALUES(1,?)',(owner,))
                            c.execute('CREATE TABLE sessions(nonce TEXT PRIMARY KEY,body TEXT NOT NULL)')
                            c.execute('CREATE TABLE slots(nonce TEXT NOT NULL REFERENCES sessions(nonce),stage INTEGER NOT NULL CHECK(stage IN (0,1)),binding TEXT NOT NULL,PRIMARY KEY(nonce,stage))')
                            obj._commit(c)
                        if c.execute('SELECT version,owner FROM protocol').fetchall()!=[(1,owner)]:
                            raise Conflict('registry protocol/owner mismatch')
                        if c.execute('PRAGMA quick_check').fetchall()!=[('ok',)]:raise Conflict('registry integrity')
                        c.execute('SELECT nonce,body FROM sessions LIMIT 0');c.execute('SELECT nonce,stage,binding FROM slots LIMIT 0')
                    obj._check()
                except BaseException:
                    obj._closed=True;os.close(fd);raise
        except BaseException:
            if hasattr(obj,'_fd') and not obj._closed:
                obj._closed=True;os.close(obj._fd)
            raise
        return obj

    def _check(self):
        if self._closed or self._retired:raise Conflict('coordinator unavailable; no continuation')
        try:s=self._path.lstat()
        except OSError:raise Conflict('registry missing') from None
        if not stat.S_ISREG(s.st_mode) or (s.st_dev,s.st_ino)!=self._identity:
            self._retired=True;raise Conflict('registry identity changed')

    @contextmanager
    def _connection(self):
        # Existing mode only: never recreate a disappeared registry implicitly.
        c=sqlite3.connect(self._path.as_uri()+'?mode=rw',uri=True,timeout=1,isolation_level=None)
        try:
            c.execute('PRAGMA foreign_keys=ON');c.execute('PRAGMA synchronous=FULL')
            yield c
        finally:c.close()

    def _commit(self,c):c.commit()

    @contextmanager
    def _access(self):
        with _non_suppressing(self._authorize()) as owner:
            if owner!=self._owner:raise PermissionError('coordinator owner mismatch')
            with self._lock:
                self._check();yield

    def _write(self,operation):
        committed=False
        try:
            with self._access(),self._connection() as c:
                c.execute('BEGIN IMMEDIATE')
                try:
                    result=operation(c)
                    try:self._commit(c)
                    except BaseException:
                        self._retired=True;raise
                    committed=True
                    return result
                except BaseException:
                    c.rollback();raise
        except BaseException:
            if committed:self._retired=True # authorization exit failure after commit
            raise

    def register(self,task,session,stages,tiles,*,hidden_size,vocab_size,config_sha256):
        task=Task.model_validate(task);session=Session.model_validate(session)
        stages=tuple(Stage.model_validate(x) for x in stages);tiles=tuple(TileV2.model_validate(x) for x in tiles)
        if not re.fullmatch('[0-9a-f]{32}',session.session_id):raise ValueError('trusted fresh nonce required')
        TypeAdapter(Digest).validate_python(config_sha256,strict=True)
        if task.owner_id!=self._owner or session.owner_id!=self._owner:raise PermissionError('fixed owner required')
        validate_prefill_plan(task,session,stages,tiles,hidden_size=hidden_size,vocab_size=vocab_size)
        plan={'protocol':2,'session_id':session.session_id,'hidden_size':hidden_size,'vocab_size':vocab_size,
              'tiles':[x.model_dump(mode='json') for x in tiles]}
        digest=hashlib.sha256(canonical(['register_v2',plan]).encode()).hexdigest()
        body=canonical({'task':task.model_dump(mode='json'),'session':session.model_dump(mode='json'),
                        'stages':[x.model_dump(mode='json') for x in stages],'plan':plan,
                        'plan_digest':digest,'config_sha256':config_sha256})
        def insert(c):
            if c.execute('SELECT 1 FROM sessions WHERE nonce=?',(session.session_id,)).fetchone():
                raise Conflict('nonce already burned; history is not permission')
            c.execute('INSERT INTO sessions VALUES(?,?)',(session.session_id,body))
        self._write(insert)
        r=_Reservation(body)
        # No await: a failed return cannot be converted into a reissued capability.
        with self._lock:
            self._check();self._reservations[id(r)]=r
        return r

    def _reservation(self,r):
        self._check()
        if type(r)is not _Reservation or self._reservations.get(id(r)) is not r:
            raise Conflict('live local reservation required; no reopen continuation')
        return json.loads(r.body)

    def _claim(self,r,stage,path,peer=None):
        binding=canonical({'path':str(Path(path).absolute()),'peer':peer})
        def claim(c):
            d=self._reservation(r);sid=d['session']['session_id']
            if c.execute('SELECT body FROM sessions WHERE nonce=?',(sid,)).fetchone()!=(r.body,):
                raise Conflict('registry declaration changed')
            if c.execute('SELECT 1 FROM slots WHERE nonce=? AND stage=?',(sid,stage)).fetchone():
                raise Conflict('stage slot burned; no rebinding')
            c.execute('INSERT INTO slots VALUES(?,?,?)',(sid,stage,binding))
            return d
        d=self._write(claim)
        @contextmanager
        def fence():
            with self._access():
                self._reservation(r)
                with self._connection() as c:
                    if c.execute('SELECT body FROM sessions WHERE nonce=?',(d['session']['session_id'],)).fetchone()!=(r.body,):
                        raise Conflict('registry declaration changed')
                    if c.execute('SELECT binding FROM slots WHERE nonce=? AND stage=?',(d['session']['session_id'],stage)).fetchone()!=(binding,):
                        raise Conflict('registry slot changed')
                yield self._owner
        return d,fence

    @staticmethod
    def _records(d):
        return (Task.model_validate_json(canonical(d['task'])),Session.model_validate_json(canonical(d['session'])),
                tuple(Stage.model_validate_json(canonical(x)) for x in d['stages']))

    @staticmethod
    def _register_journal(j,d):
        try:
            p=d['plan'];result=j.register(tuple(TileV2.model_validate_json(canonical(x)) for x in p['tiles']),hidden_size=p['hidden_size'],vocab_size=p['vocab_size'],event_id='coordinator-plan')
            if result['plan_digest']!=d['plan_digest']:raise Conflict('journal plan mismatch')
            return j
        except BaseException:j.close();raise

    def create_sender(self,r,path,*,execution_authorize):
        d,fence=self._claim(r,0,path)
        j=_CoordinatedSender.create(path,*self._records(d),authorize=fence,execution_authorize=execution_authorize)
        j._coordinator_body=r.body
        return self._register_journal(j,d)

    def create_receiver(self,r,bound,path,*,execution_authorize):
        if type(bound)is not BoundReceiptConnection:raise TypeError('bound Channel adapter required')
        with self._access():
            d=self._reservation(r)
            if bound.expected_peer.node_id!=d['stages'][0]['assigned_node']:raise Conflict('wrong upstream identity')
        peer={'node':bound.expected_peer.node_id,'certificate':bound.expected_peer.certificate_sha256,
              'generation':bound.expected_peer.connection_generation}
        d,fence=self._claim(r,1,path,peer)
        t,s,st=self._records(d)
        # Existing input encoding seam requires explicit wire layout >=2.
        edge=EdgeBinding(t.task_id,s.session_id,s.epoch,d['plan_digest'],s.layout_hash,2,
                         st[0].assigned_node,st[1].assigned_node)
        j=bound.create_journal(path,t,s,st,authorize=fence,execution_authorize=execution_authorize,
             receipt_binding=edge,local_node=st[1].assigned_node,expected_config_sha256=d['config_sha256'])
        return self._register_journal(j,d)

    def query(self,nonce):
        with self._access(),self._connection() as c:
            row=c.execute('SELECT body FROM sessions WHERE nonce=?',(nonce,)).fetchone()
            return {'exists':row is not None,'execution_allowed':False,
                    'claimed_stages':tuple(x[0] for x in c.execute('SELECT stage FROM slots WHERE nonce=? ORDER BY stage',(nonce,)))}

    def close(self):
        with self._lock:
            if not self._closed:
                self._closed=True;self._reservations.clear();os.close(self._fd)
