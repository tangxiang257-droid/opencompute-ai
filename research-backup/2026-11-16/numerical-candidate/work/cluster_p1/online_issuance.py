"""Private online issuance storage/factories, no transport or execution grant.

One trusted central database and fresh role-local registries. History/JSON cannot
reconstruct live qualifications. Providers verify existing authority/resources;
all reopen paths are history-only. Cross-store commits are NOT atomic. Copies,
rollback and OS/process cloning need external protection. No model is loaded.
"""
from contextlib import contextmanager
from dataclasses import dataclass,asdict
import hashlib,json,os,re,sqlite3,threading,uuid
from pathlib import Path
from .contracts import Task,Session,Stage
from .tile_contract_v2 import TileV2,validate_prefill_plan
from .store import Conflict,canonical
from .local_attempt import _non_suppressing
from .projection_driver import nothing
from .channel import Channel,ChannelIdentity
from .async_role_bridge import _ssl_snapshot
from .bound_connection import BoundReceiptConnection
from .handoff import EdgeBinding

_DESCRIPTOR={'registration_id','source_namespace','source_id','owner','task','nonce','epoch','stage',
             'node','device','instance','resource_id','declaration','declaration_digest','issuer_incarnation','source_binding'}
_INTENT=_DESCRIPTOR|{'command_id','boot_id','store_id','challenge','issuer_peer'}
def _need(ok):
    if not ok:raise Conflict('online issuance rejected')
def _hash(x):return hashlib.sha256(canonical(x).encode()).hexdigest()
def _identifier(x):
    _need(type(x)is str and re.fullmatch('[A-Za-z0-9][A-Za-z0-9._:-]{0,127}',x) is not None)
def _digest(x):_need(type(x)is str and re.fullmatch('[a-f0-9]{64}',x) is not None)
def _copy(x):
    raw=canonical(x);_need(len(raw.encode())<=32768)
    return json.loads(raw)
def _make_declaration(task,session,stages,tiles,hidden_size,vocab_size,config_sha256):
    def validate(cls,value):
        return cls.model_validate_json(canonical(value)) if type(value)is dict else cls.model_validate(value)
    t=validate(Task,task);s=validate(Session,session)
    st=tuple(validate(Stage,x) for x in stages);ts=tuple(validate(TileV2,x) for x in tiles)
    _digest(config_sha256);_need(s.epoch==1 and re.fullmatch('[a-f0-9]{32}',s.session_id) is not None)
    validate_prefill_plan(t,s,st,ts,hidden_size=hidden_size,vocab_size=vocab_size)
    plan=dict(protocol=2,session_id=s.session_id,hidden_size=hidden_size,vocab_size=vocab_size,tiles=[x.model_dump(mode='json') for x in ts])
    d=dict(task=t.model_dump(mode='json'),session=s.model_dump(mode='json'),stages=[x.model_dump(mode='json') for x in st],
           plan=plan,plan_digest=_hash(['register_v2',plan]),config_sha256=config_sha256)
    _need(len(canonical(d).encode())<=16384);return d
def _source_binding(value):
    _need(type(value)is dict and set(value)=={'input_snapshot','token_digest','preprocessing'})
    for v in value.values():_digest(v)
    return _copy(value)
def _descriptor(value):
    _need(type(value)is dict and set(value)==_DESCRIPTOR)
    _source_binding(value['source_binding'])
    d=value['declaration'];_need(type(d)is dict)
    p=d['plan'];again=_make_declaration(d['task'],d['session'],d['stages'],p['tiles'],p['hidden_size'],p['vocab_size'],d['config_sha256'])
    _need(canonical(d)==canonical(again) and value['declaration_digest']==_hash(d))
    _need(type(value['stage'])is int and value['stage'] in (0,1) and type(value['epoch'])is int and value['epoch']==1)
    _need((value['owner'],value['task'],value['nonce'],value['node'])==(d['task']['owner_id'],d['task']['task_id'],d['session']['session_id'],d['stages'][value['stage']]['assigned_node']))
    for k in _DESCRIPTOR-{'declaration','stage','epoch','source_binding'}:_identifier(value[k])
    _digest(value['source_namespace']);_digest(value['declaration_digest']);_digest(value['registration_id'])
    return _copy(value)
def _intent(value):
    _need(type(value)is dict and set(value)==_INTENT)
    _descriptor({k:value[k] for k in _DESCRIPTOR})
    for k in ('command_id','boot_id','store_id'):_identifier(value[k])
    _digest(value['challenge'])
    peer=value['issuer_peer'];_need(type(peer)is dict and set(peer)=={'node_id','certificate_sha256','connection_generation'})
    _identifier(peer['node_id']);_digest(peer['certificate_sha256']);_identifier(peer['connection_generation'])
    return _copy(value)

class _Database:
    def _open(self,path):
        self.path=Path(path).absolute();self.lock=threading.RLock();self.pid=os.getpid();self.closed=False;self.retired=False
        try:fd=os.open(self.path,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600);os.close(fd);self.fresh=True
        except FileExistsError:self.fresh=False
        st=self.path.stat();self.file_identity=(st.st_dev,st.st_ino)
        self.db=sqlite3.connect(self.path.as_uri()+'?mode=rw',uri=True,isolation_level=None,check_same_thread=False,timeout=5)
        self.db.execute('PRAGMA synchronous=FULL')
    def _check(self,write=False):
        _need(not self.closed and self.pid==os.getpid())
        st=self.path.stat();_need((st.st_dev,st.st_ino)==self.file_identity)
        if write:_need(self.fresh and not self.retired)
    def _commit(self,c):c.commit()
    @contextmanager
    def _tx(self):
        with self.lock:
            self._check();self.db.execute('BEGIN IMMEDIATE')
            try:yield self.db;self._commit(self.db)
            except BaseException:self.db.rollback();raise
    def close(self):
        with self.lock:
            if not self.closed:self.closed=True;self.db.close()

@dataclass(frozen=True,slots=True,eq=False)
class _Registration:
    registration_id:str
    source_id:str
    body:str
    source_binding:str
    authorization:str|None=None

class IssuanceAuthority(_Database):
    def __init__(self,path,*,namespace,source_provider=None,authorize=None,resources=None,require_consent=False,enable_receiving=False):
        _need(type(require_consent)is bool and type(enable_receiving)is bool and (not enable_receiving or require_consent));self.require_consent=require_consent;self.enable_receiving=enable_receiving
        _digest(namespace);self.namespace=namespace;self.source_provider=source_provider;self.authorize=authorize;self.resources=resources
        self.registrations={};self.descriptors={};self._open(path)
        try:
            with self._tx() as c:
                if self.fresh:
                    c.execute('CREATE TABLE authority(version INTEGER,namespace TEXT,incarnation TEXT)')
                    c.execute('INSERT INTO authority VALUES(?,?,?)',(3 if enable_receiving else (2 if require_consent else 1),namespace,uuid.uuid4().hex))
                    if enable_receiving:
                        from .c_receiving import SCHEMA
                        c.execute(SCHEMA)
                    if require_consent:c.execute('CREATE TABLE authorizations(registration_id TEXT PRIMARY KEY,context TEXT NOT NULL)')
                    c.execute('CREATE TABLE registrations(id TEXT PRIMARY KEY,namespace TEXT,source_id TEXT,owner TEXT,task TEXT,nonce TEXT UNIQUE,body TEXT,source_binding TEXT NOT NULL,UNIQUE(namespace,owner,source_id),UNIQUE(owner,task))')
                    c.execute('CREATE TABLE slots(registration_id TEXT,stage INTEGER,command_id TEXT UNIQUE,intent TEXT,peer TEXT,PRIMARY KEY(registration_id,stage))')
                rows=c.execute('SELECT version,namespace,incarnation FROM authority').fetchall()
                _need(len(rows)==1 and rows[0][:2]==(3 if enable_receiving else (2 if require_consent else 1),namespace));self.incarnation=rows[0][2]
        except BaseException:self.close();raise
    def ready(self):
        self._check(True)
        _need(callable(getattr(self.source_provider,'check',None)) and callable(getattr(self.authorize,'hold',None)) and callable(getattr(self.resources,'hold',None)))
    def register(self,task,session,stages,tiles,*,hidden_size,vocab_size,config_sha256,source_id,source_binding,authorization_context=None):
        self.ready();_identifier(source_id);source_binding=_source_binding(source_binding)
        d=_make_declaration(task,session,stages,tiles,hidden_size,vocab_size,config_sha256)
        owner=d['task']['owner_id'];taskid=d['task']['task_id'];nonce=d['session']['session_id'];body=canonical(d)
        auth=None
        if self.require_consent:
            _need(type(authorization_context)is dict);auth=canonical(authorization_context);_need(len(auth.encode())<=32768)
        else:_need(authorization_context is None)
        request=dict(version=2,declaration=d,source_binding=source_binding,authorization=authorization_context) if self.require_consent else d
        rid=_hash([self.namespace,source_id,owner,taskid,nonce,d,source_binding]);committed=False
        try:
            with _non_suppressing(self.authorize.hold(json.loads(canonical(request)),'register')) as a:
                nothing(a);nothing(self.source_provider.check(self.namespace,source_id,owner,taskid,_hash(d),_copy(source_binding)))
                with self._tx() as c:
                    self._check(True)
                    if c.execute('SELECT 1 FROM registrations WHERE (namespace=? AND owner=? AND source_id=?) OR (owner=? AND task=?) OR nonce=?',(self.namespace,owner,source_id,owner,taskid,nonce)).fetchone():raise Conflict('source/task/nonce permanently occupied')
                    c.execute('INSERT INTO registrations VALUES(?,?,?,?,?,?,?,?)',(rid,self.namespace,source_id,owner,taskid,nonce,body,canonical(source_binding)))
                    if self.require_consent:c.execute('INSERT INTO authorizations VALUES(?,?)',(rid,auth))
                    committed=True
            r=_Registration(rid,source_id,body,canonical(source_binding),auth);self.registrations[id(r)]=r;return r
        except BaseException:
            if committed:self.retired=True
            raise
    def _registration(self,r):
        self.ready();_need(type(r)is _Registration and self.registrations.get(id(r)) is r)
        _need(self.db.execute('SELECT body,source_binding FROM registrations WHERE id=?',(r.registration_id,)).fetchone()==(r.body,r.source_binding))
        if self.require_consent:_need(self.db.execute('SELECT context FROM authorizations WHERE registration_id=?',(r.registration_id,)).fetchone()==(r.authorization,))
        return json.loads(r.body)
    def describe(self,r,stage,*,node,device,instance,resource_id):
        with self.lock:
            d=self._registration(r)
            value=dict(registration_id=r.registration_id,source_namespace=self.namespace,source_id=r.source_id,
                owner=d['task']['owner_id'],task=d['task']['task_id'],nonce=d['session']['session_id'],epoch=1,stage=stage,
                node=node,device=device,instance=instance,resource_id=resource_id,declaration=d,source_binding=json.loads(r.source_binding),declaration_digest=_hash(d),issuer_incarnation=self.incarnation)
            value=_descriptor(value);key=(r.registration_id,stage);raw=canonical(value)
            if key in self.descriptors:_need(self.descriptors[key]==raw)
            else:self.descriptors[key]=raw
            return value
    def claim_stage(self,r,intent,peer):
        value=_intent(intent);_need(type(peer)is ChannelIdentity)
        with self.lock:
            self._registration(r)
            descriptor={k:value[k] for k in _DESCRIPTOR};key=(r.registration_id,value['stage'])
            _need(self.descriptors.get(key)==canonical(descriptor) and value['issuer_incarnation']==self.incarnation
                  and value['registration_id']==r.registration_id and peer.node_id==value['node'])
        consumed=False
        try:
            with _non_suppressing(self.authorize.hold(dict(version=2,intent=_copy(value),authorization=json.loads(r.authorization)), 'claim') if self.require_consent else self.authorize.hold(_copy(value),'claim')) as a,_non_suppressing(self.resources.hold(value['resource_id'],_copy(value))) as resources:
                nothing(a);nothing(resources)
                with self._tx() as c:
                    self._registration(r)
                    if c.execute('SELECT 1 FROM slots WHERE registration_id=? AND stage=?',key).fetchone():raise Conflict('stage permanently occupied; query only')
                    c.execute('INSERT INTO slots VALUES(?,?,?,?,?)',(*key,value['command_id'],canonical(value),canonical(asdict(peer))))
                    consumed=True
            return dict(version=1,intent=value,slot_digest=_hash(value),issuer_incarnation=self.incarnation,execution_allowed=False)
        except BaseException:
            if consumed:self.retired=True
            raise
    def query(self,command_id):
        _identifier(command_id)
        with self.lock:
            self._check();row=self.db.execute('SELECT intent FROM slots WHERE command_id=?',(command_id,)).fetchone()
            return dict(exists=row is not None,intent=json.loads(row[0]) if row else None,execution_allowed=False)

@dataclass(frozen=True,slots=True,eq=False)
class _Pending:
    raw:str
    channel:object
    identity:object
    ssl:object
    @property
    def request(self):return json.loads(self.raw)
@dataclass(frozen=True,slots=True,eq=False)
class _InstallHandle:
    pending:_Pending
    @property
    def body(self):return canonical(self.pending.request['declaration'])

class RoleInstallStore(_Database):
    def __init__(self,path,*,role_node,identity=None,authorize=None,resources=None,device=None):
        _identifier(role_node);self.role_node=role_node;self.identity=identity;self.authorize=authorize;self.resources=resources;self.device=device
        self.pending_handles={};self.install_handles={};self.issued={};self.clients={};self.creating=None;self._open(path)
        try:
            with self._tx() as c:
                if self.fresh:
                    c.execute('CREATE TABLE role(version INTEGER,node TEXT,boot TEXT,store TEXT)')
                    c.execute('INSERT INTO role VALUES(1,?,?,?)',(role_node,uuid.uuid4().hex,uuid.uuid4().hex))
                    c.execute('CREATE TABLE install_control(command TEXT PRIMARY KEY,reservation TEXT NOT NULL,binding TEXT NOT NULL,charge INTEGER NOT NULL)')
                    c.execute('CREATE TABLE installs(command TEXT PRIMARY KEY,intent TEXT NOT NULL,response TEXT,state TEXT NOT NULL,retired INTEGER NOT NULL,path TEXT,UNIQUE(intent))')
                rows=c.execute('SELECT version,node,boot,store FROM role').fetchall();_need(len(rows)==1 and rows[0][:2]==(1,role_node))
                self.boot_id,self.store_id=rows[0][2:]
        except BaseException:self.close();raise
    def ready(self):
        self._check(True)
        _need(all(callable(getattr(p,'hold',None)) for p in (self.identity,self.authorize,self.resources,self.device)))
    @contextmanager
    def _fence(self,p,action):
        self.ready();_need(type(p)is _Pending and self.pending_handles.get(id(p)) is p and type(p.channel)is Channel)
        value=p.request;p.channel._identity_loop_check()
        with (p.channel.identity_fence(p.identity),_non_suppressing(self.identity.hold(_copy(value),p.identity)) as identity,
             _non_suppressing(self.authorize.hold(_copy(value),action)) as auth,
             _non_suppressing(self.resources.hold(value['resource_id'],_copy(value))) as resources,
             _non_suppressing(self.device.hold(_copy(value),'install')) as device):
            _need(_ssl_snapshot(p.channel.w)==p.ssl);nothing(identity);nothing(auth);nothing(resources);nothing(device)
            client=self.clients.get(id(p))
            if client is not None:nothing(client.live_guard())
            mapping=self.db.execute('SELECT reservation,binding,charge FROM install_control WHERE command=?',(value['command_id'],)).fetchone()
            if mapping is None:yield
            else:
                _need(callable(getattr(self.resources,'hold_control',None)))
                with _non_suppressing(self.resources.hold_control(value['resource_id'],mapping[0],json.loads(mapping[1]),mapping[2])) as held:
                    nothing(held);yield
    def pending(self,descriptor,channel,*,challenge):
        self.ready();d=_descriptor(descriptor);_digest(challenge);_need(d['node']==self.role_node and type(channel)is Channel)
        identity=channel.authenticated_identity
        value=dict(d,command_id=uuid.uuid4().hex,boot_id=self.boot_id,store_id=self.store_id,challenge=challenge,issuer_peer=asdict(identity))
        value=_intent(value);p=_Pending(canonical(value),channel,identity,_ssl_snapshot(channel.w));self.pending_handles[id(p)]=p
        try:
            with self._fence(p,'pending'),self._tx() as c:
                _need(c.execute('SELECT 1 FROM installs').fetchone() is None)
                c.execute("INSERT INTO installs VALUES(?, ?,NULL,'INSTALL_PENDING',0,NULL)",(value['command_id'],p.raw))
            return p
        except BaseException:self.retired=True;raise
    def _row(self,p):
        row=self.db.execute('SELECT intent,response,state,retired,path FROM installs WHERE command=?',(p.request['command_id'],)).fetchone()
        _need(row is not None and row[0]==p.raw);return row
    def accept(self,p,response,channel,client):
        from .online_issuance_transport import IssuanceClient
        _need(type(p)is _Pending and channel is p.channel and type(client)is IssuanceClient
              and client._store is self and client._pending is p and client.channel is channel)
        verified=getattr(client,'_verified_delivery',None)
        _need(type(verified)is tuple and len(verified)==2 and verified[0] is p and verified[1]==canonical(response).encode())
        value=_copy(response);_need(type(value)is dict and set(value)=={'version','intent','slot_digest','issuer_incarnation','execution_allowed'})
        _need(type(value['version'])is int and value['version']==1 and value['execution_allowed'] is False
              and canonical(value['intent'])==p.raw and value['slot_digest']==_hash(p.request)
              and value['issuer_incarnation']==p.request['issuer_incarnation'])
        try:
            with self._fence(p,'accept'):
                nothing(client.live_guard())
                with self._tx() as c:
                    row=self._row(p);_need(row[2]=='INSTALL_PENDING' and row[3]==0)
                    c.execute("UPDATE installs SET response=?,state='ACCEPTED' WHERE command=?",(canonical(value),p.request['command_id']))
            self.clients[id(p)]=client
            h=_InstallHandle(p);self.install_handles[id(h)]=h;return h
        except BaseException:self.retired=True;raise
    def _handle(self,h):
        self.ready();_need(type(h)is _InstallHandle and self.install_handles.get(id(h)) is h)
        return h.pending
    def _create(self,h,path,stage,bound,execution_authorize):
        from .nonce_coordinator import NonceCoordinator,_CoordinatedSender
        p=self._handle(h);d=p.request;_need(d['stage']==stage and callable(execution_authorize))
        path=str(Path(path).absolute());j=None
        try:
            with self._fence(p,'install'),self._tx() as c:
                row=self._row(p);_need(row[2]=='ACCEPTED' and not row[3])
                c.execute("UPDATE installs SET state='INSTALL_CONSUMED',path=? WHERE command=?",(path,d['command_id']))
            self.creating=h
            @contextmanager
            def fence():
                with self._fence(p,'journal'):
                    row=self._row(p)
                    _need(not row[3] and (row[2]=='INSTALLED' or (row[2]=='INSTALL_CONSUMED' and self.creating is h)))
                    yield d['owner']
            records=NonceCoordinator._records(d['declaration'])
            if stage==0:
                j=_CoordinatedSender.create(path,*records,authorize=fence,execution_authorize=execution_authorize)
                j._coordinator_body=h.body
            else:
                _need(type(bound)is BoundReceiptConnection and bound.expected_peer.node_id==d['declaration']['stages'][0]['assigned_node'])
                t,s,st=records
                edge=EdgeBinding(t.task_id,s.session_id,s.epoch,d['declaration']['plan_digest'],s.layout_hash,2,st[0].assigned_node,st[1].assigned_node)
                j=bound.create_journal(path,*records,authorize=fence,execution_authorize=execution_authorize,receipt_binding=edge,local_node=d['node'],expected_config_sha256=d['declaration']['config_sha256'])
            NonceCoordinator._register_journal(j,d['declaration'])
            with self._fence(p,'installed'),self._tx() as c:
                row=self._row(p);_need(row[2]=='INSTALL_CONSUMED' and not row[3])
                c.execute("UPDATE installs SET state='INSTALLED' WHERE command=?",(d['command_id'],))
            j._online_issuer=self
            self.issued[id(j)]=(j,h,stage,path,j._authorize,j._execution,bound)
            return j
        except BaseException:
            self.retired=True
            if j is not None:j.close()
            raise
        finally:self.creating=None
    def create_sender(self,h,path,*,execution_authorize):return self._create(h,path,0,None,execution_authorize)
    def create_receiver(self,h,bound,path,*,execution_authorize):return self._create(h,path,1,bound,execution_authorize)
    def require_issued(self,j,stage):
        from .nonce_coordinator import _CoordinatedSender
        from .receipt_attempt import ReceiptAttemptJournal
        _need(type(j)is (_CoordinatedSender if stage==0 else ReceiptAttemptJournal))
        row=self.issued.get(id(j));_need(row is not None and row[0] is j)
        _,h,actual,path,authorize,execution,bound=row;p=self._handle(h)
        _need(j._task_id==p.request['task'] and j._session_id==p.request['nonce'])
        _need(actual==stage and j._authorize is authorize and j._execution is execution and str(Path(j._path).absolute())==path and str(Path(j._store.path).absolute())==path)
        with j._authorized(),self._fence(p,'validate'):
            stored=self._row(p);_need(stored[2]=='INSTALLED' and not stored[3] and stored[4]==path)
            if stage==0:_need(j._coordinator_body==h.body)
            else:
                bound._journal(j)
                with j._receipt_fence():pass
        return self,h
    def bind_control(self,j,stage,descriptor):
        from .role_control import RoleDescriptor
        _need(type(descriptor)is RoleDescriptor)
        _,h=self.require_issued(j,stage);p=h.pending;d=p.request;b=descriptor.binding
        _need(descriptor.role_node==d['node'])
        expected=dict(owner=d['owner'],task=d['task'],session=d['nonce'],epoch=d['epoch'],stage=d['stage'],
            device=d['device'],instance=d['instance'],declaration_digest=d['declaration_digest'],
            model=d['declaration']['session']['model_hash'],layout=d['declaration']['session']['layout_hash'],plan=d['declaration']['plan_digest'])
        expected.update(d['source_binding'])
        if stage==0:expected['input_digest']=d['source_binding']['token_digest']
        _need(all(b.get(k)==v for k,v in expected.items()))
        try:
            # Burn binding before provider verification; a failed/uncertain mapping never rebinding.
            with self._fence(p,'bind_control'),self._tx() as c:
                _need(c.execute('SELECT 1 FROM install_control WHERE command=?',(d['command_id'],)).fetchone() is None)
                c.execute('INSERT INTO install_control VALUES(?,?,?,?)',(d['command_id'],descriptor.reservation_id,canonical(b),descriptor.charge))
            with self._fence(p,'control_bound'):pass
        except BaseException:
            self.retired=True
            raise
    def query(self,command_id):
        _identifier(command_id)
        with self.lock:
            self._check();row=self.db.execute('SELECT intent,response,state,retired,path FROM installs WHERE command=?',(command_id,)).fetchone()
            return dict(exists=row is not None,state=row[2] if row else None,retired=bool(row[3]) if row else False,execution_allowed=False)
    def retire(self,command_id):
        with self._tx() as c:
            self._check(True)
            c.execute('UPDATE installs SET retired=1 WHERE command=?',(command_id,))
