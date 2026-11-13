"""Private single-Linux-coordinator provisioning. No remote credentials or recovery.

One stable trusted database, parent directory and same-UID Python assembly.
Deleting/rolling back this database defeats its history; no global replay claim.
"""
from contextlib import contextmanager
from dataclasses import dataclass,asdict
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

# Version 2 is declaration-only. It never issues legacy live reservations.
_SOURCE_SCHEMA=(
 'CREATE TABLE protocol(version INTEGER PRIMARY KEY CHECK(version=2),owner TEXT NOT NULL,source_namespace TEXT NOT NULL)',
 'CREATE TABLE sessions(nonce TEXT PRIMARY KEY,body TEXT NOT NULL)',
 'CREATE TABLE slots(nonce TEXT NOT NULL REFERENCES sessions(nonce),stage INTEGER NOT NULL CHECK(stage IN (0,1)),binding TEXT NOT NULL,PRIMARY KEY(nonce,stage))',
 'CREATE TABLE source_claims(namespace TEXT NOT NULL,owner TEXT NOT NULL,task TEXT NOT NULL,nonce TEXT NOT NULL UNIQUE REFERENCES sessions(nonce),body TEXT NOT NULL,digest TEXT NOT NULL,PRIMARY KEY(namespace,owner,task))',
)
for _table in ('protocol','sessions','source_claims'):
    for _verb in ('UPDATE','DELETE'):
        _SOURCE_SCHEMA+=(f"CREATE TRIGGER {_table}_no_{_verb.lower()} BEFORE {_verb} ON {_table} BEGIN SELECT RAISE(ABORT,'immutable'); END",)
del _table,_verb

class _SourceAlreadyClaimed(Conflict):
    pass

@dataclass(frozen=True,slots=True)
class SourceRegistration:
    """Constructible data receipt; no proof of source validity or permission."""
    source_namespace: str
    task_id: str
    nonce: str
    declaration_sha256: str
    execution_allowed: bool = False


def _source_declaration_body(value):
    # Lazy dependency: legacy v1 users do not import the source-storage facade.
    from input_declaration import InputDeclaration,DeclaredModel
    from pair_plan_contract import _digest
    if type(value) is not InputDeclaration or type(value.model) is not DeclaredModel:
        raise TypeError('exact declaration data required')
    for name in ('owner_id','request_id','draft_id','preprocessing_version'):
        item=getattr(value,name)
        if type(item) is not str or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:-]{0,127}',item):
            raise ValueError('declaration identifier')
    if type(value.task_id) is not str or not re.fullmatch('[0-9a-f]{32}',value.task_id):
        raise ValueError('source task identifier')
    for name in ('text_sha256','render_sha256','snapshot_digest','token_blob_sha256','contract_sha256','model_record_digest'):
        item=getattr(value,name)
        if type(item) is not str or not re.fullmatch('[0-9a-f]{64}',item):raise ValueError('declaration digest')
    for name,high in (('draft_version',2147483647),('token_count',131072),('vocab_size',262144)):
        item=getattr(value,name)
        if type(item) is not int or not 1<=item<=high:raise ValueError('declaration range')
    fixed={'token_encoding':'int64-le','position_mode':'contiguous-zero','mask_mode':'causal-unpadded',
           'tokenizer_status':'internal_metadata_only','model_status':'unverified_declared'}
    if any(type(getattr(value,k)) is not str or getattr(value,k)!=v for k,v in fixed.items()):
        raise ValueError('unsupported declaration semantics')
    if type(value.batch_size) is not int or value.batch_size!=1 or value.execution_allowed is not False or value.computation_verified is not False:
        raise ValueError('inert declaration required')
    model={}
    for name in ('config_sha256','weights_sha256','semantics','weight_format',
                 'compute_dtype','kv_dtype','activation_dtype','logits_dtype'):
        item=getattr(value.model,name)
        if type(item) is not str:raise ValueError('model declaration type')
        model[name]=item
    # Only after exact scalar validation may asdict traverse the declaration.
    # Otherwise dataclass fields could invoke an arbitrary __deepcopy__ callback.
    for key in ('config_sha256','weights_sha256'):
        if not re.fullmatch('[0-9a-f]{64}',model[key]):raise ValueError('model digest')
    if model['semantics']!='stock-qwen2-full-causal-v1' or model['weight_format']!='safetensors-single' or any(model[k]!='float32' for k in ('compute_dtype','kv_dtype','activation_dtype','logits_dtype')):
        raise ValueError('model semantics')
    if _digest(dict(kind='ModelIdentity',version=1,**model))!=value.model_record_digest:
        raise ValueError('model record digest mismatch')
    body=canonical(asdict(value))
    if len(body.encode('utf-8'))>4096:raise ValueError('declaration size')
    return body


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
    def initialize(cls,path,*,authorize,source_namespace=None):
        return cls._open(path,authorize,True,source_namespace)

    @classmethod
    def open_existing(cls,path,*,authorize,source_namespace=None):
        return cls._open(path,authorize,False,source_namespace)

    @classmethod
    def _open(cls,path,authorize,fresh,source_namespace=None):
        if source_namespace is not None and (type(source_namespace) is not str or not re.fullmatch('[0-9a-f]{64}',source_namespace)):
            raise ValueError('trusted stable source namespace required')
        if not callable(authorize):raise TypeError('owner fence required')
        obj=cls.__new__(cls);obj._path=Path(path).absolute();obj._authorize=authorize
        obj._lock=threading.RLock();obj._closed=False;obj._retired=False;obj._reservations={}
        obj._source_namespace=source_namespace;obj._source_writable=fresh and source_namespace is not None
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
                            if source_namespace is None:
                                c.execute('CREATE TABLE protocol(version INTEGER PRIMARY KEY CHECK(version=1),owner TEXT NOT NULL)')
                                c.execute('INSERT INTO protocol VALUES(1,?)',(owner,))
                                c.execute('CREATE TABLE sessions(nonce TEXT PRIMARY KEY,body TEXT NOT NULL)')
                                c.execute('CREATE TABLE slots(nonce TEXT NOT NULL REFERENCES sessions(nonce),stage INTEGER NOT NULL CHECK(stage IN (0,1)),binding TEXT NOT NULL,PRIMARY KEY(nonce,stage))')
                            else:
                                for sql in _SOURCE_SCHEMA:c.execute(sql)
                                c.execute('INSERT INTO protocol VALUES(2,?,?)',(owner,source_namespace))
                            obj._commit(c)
                        if source_namespace is None:
                            if c.execute('SELECT version,owner FROM protocol').fetchall()!=[(1,owner)]:
                                raise Conflict('registry protocol/owner mismatch')
                        else:
                            # Check version before selecting v2-only columns.
                            if c.execute('SELECT version,owner FROM protocol').fetchall()!=[(2,owner)]:
                                raise Conflict('registry protocol/owner mismatch')
                            obj._verify_source_schema(c)
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
        if self._source_namespace is not None:raise Conflict('source-only registry never grants execution')
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
        if self._source_namespace is not None:raise Conflict('source-only registry never grants execution')
        self._check()
        if type(r)is not _Reservation or self._reservations.get(id(r)) is not r:
            raise Conflict('live local reservation required; no reopen continuation')
        return json.loads(r.body)

    def _claim(self,r,stage,path,peer=None):
        if self._source_namespace is not None:raise Conflict('source-only registry never grants execution')
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
        if self._source_namespace is not None:raise Conflict('source-only registry never grants execution')
        d,fence=self._claim(r,0,path)
        j=_CoordinatedSender.create(path,*self._records(d),authorize=fence,execution_authorize=execution_authorize)
        j._coordinator_body=r.body
        return self._register_journal(j,d)

    def create_receiver(self,r,bound,path,*,execution_authorize):
        if self._source_namespace is not None:raise Conflict('source-only registry never grants execution')
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

    def _verify_source_schema(self,c):
        if self._source_namespace is None:raise Conflict('source mode not enabled')
        if c.execute('SELECT version,owner,source_namespace FROM protocol').fetchall()!=[(2,self._owner,self._source_namespace)]:
            raise Conflict('source namespace/protocol mismatch')
        rows=c.execute("SELECT sql FROM sqlite_master WHERE name NOT GLOB 'sqlite_*'").fetchall()
        if len(rows)!=len(_SOURCE_SCHEMA) or {r[0] for r in rows}!=set(_SOURCE_SCHEMA):
            raise Conflict('source schema changed')

    def _insert_source_claim(self,c,task,nonce,body,digest):
        c.execute('INSERT INTO source_claims VALUES(?,?,?,?,?,?)',
                  (self._source_namespace,self._owner,task,nonce,body,digest))

    def _source_session_body(self,task,nonce,digest):
        return canonical({'kind':'source-declaration-only','version':1,
                          'namespace':self._source_namespace,'owner':self._owner,
                          'task':task,'nonce':nonce,'declaration_sha256':digest,
                          'execution_allowed':False})

    def bind_source_declaration(self,declaration,nonce):
        """Burn an inert source/nonce pair in ONE fresh v2 registry transaction.

        Trusted local assembly supplies the statement; this does not authenticate
        a source store, authorize execution, or create a cross-store transaction.
        Reopened registries are history-only, even if a prior COMMIT left no row.
        """
        if self._source_namespace is None or not self._source_writable:
            raise Conflict('fresh source-only registry required; no continuation')
        if type(nonce) is not str or not re.fullmatch('[0-9a-f]{32}',nonce):
            raise ValueError('trusted fresh nonce required')
        body=_source_declaration_body(declaration)
        if declaration.owner_id!=self._owner:raise PermissionError('fixed owner required')
        digest=hashlib.sha256(b'opencompute/source-declaration/v1\0'+body.encode()).hexdigest()
        task=declaration.task_id
        def insert(c):
            self._verify_source_schema(c)
            if (c.execute('SELECT 1 FROM source_claims WHERE namespace=? AND owner=? AND task=?',
                          (self._source_namespace,self._owner,task)).fetchone() or
                c.execute('SELECT 1 FROM sessions WHERE nonce=?',(nonce,)).fetchone()):
                raise _SourceAlreadyClaimed('source or nonce already burned; no retry')
            c.execute('INSERT INTO sessions VALUES(?,?)',(nonce,self._source_session_body(task,nonce,digest)))
            self._insert_source_claim(c,task,nonce,body,digest)
        try:self._write(insert)
        except _SourceAlreadyClaimed:raise
        except BaseException:
            self._retired=True
            raise
        return SourceRegistration(self._source_namespace,task,nonce,digest)

    def query_source(self,task_id):
        """Historical metadata only, never reconstruct a live reservation."""
        if type(task_id) is not str or not re.fullmatch('[0-9a-f]{32}',task_id):
            raise ValueError('source task identifier')
        with self._access(),self._connection() as c:
            self._verify_source_schema(c)
            row=c.execute('SELECT nonce,body,digest FROM source_claims WHERE namespace=? AND owner=? AND task=?',
                          (self._source_namespace,self._owner,task_id)).fetchone()
            if row is None:return {'exists':False,'execution_allowed':False}
            nonce,body,digest=row
            if (type(body) is not str or len(body.encode())>4096 or
                hashlib.sha256(b'opencompute/source-declaration/v1\0'+body.encode()).hexdigest()!=digest or
                c.execute('SELECT body FROM sessions WHERE nonce=?',(nonce,)).fetchone()!=
                (self._source_session_body(task_id,nonce,digest),)):
                raise Conflict('source history changed')
            return {'exists':True,'nonce':nonce,'declaration_sha256':digest,'execution_allowed':False}

    def query(self,nonce):
        with self._access(),self._connection() as c:
            row=c.execute('SELECT body FROM sessions WHERE nonce=?',(nonce,)).fetchone()
            return {'exists':row is not None,'execution_allowed':False,
                    'claimed_stages':tuple(x[0] for x in c.execute('SELECT stage FROM slots WHERE nonce=? ORDER BY stage',(nonce,)))}

    def close(self):
        with self._lock:
            if not self._closed:
                self._closed=True;self._reservations.clear();os.close(self._fd)
