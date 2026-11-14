"""Actual local A/B operations; no foreign role database or result ledger.

Real model adapters are lazy and never supplied by default. Bootstrap must
provide real local authority/resource/device services. Synthetic tests establish
only byte/storage/control ordering, not model numerics or physical completion.
"""
from contextlib import contextmanager
import hashlib,json,re
from .projection_driver import Spec,Policy,Journal,Driver,need,nothing,synchronous
from .private_one_token import Loaded,CompletionBridge,_PushSpool,_BoundAuthority,_factories
from .local_attempt import _non_suppressing
from .nonce_coordinator import _require_coordinated_sender,_require_issued_journal
from .receipt_attempt import ReceiptAttemptJournal
from .byte_spool import Spool
from .control_journal import ControlJournal
from .control_exchange import result_digest
from .role_control import RoleDescriptor
from .store import canonical

class _Completion:
    def __init__(self,op):self.op=op
    def check(self,binding):self.op._check_binding(binding);self.op.ready()
    def bind_control(self,journal,descriptor):self.op.bind_control(journal,descriptor)
    def begin(self,binding,scope):
        self.check(binding);self.op.begin(scope)
    def finish(self,binding,scope):
        self.op._check_binding(binding);self.op.finish(scope)
    def quarantine(self,binding,scope):
        self.op._check_binding(binding);need(scope==self.op.scope);self.op.quarantine()

class _DriverCompletion:
    """Use the already registered role scope, never open a second device scope."""
    def __init__(self,op):self.op=op
    def open_scope(self,spec,token,scope):
        need(spec==self.op.spec and token==0 and scope==self.op.scope)
        s=self.op.bridge.scopes[scope];need(not s['finished'] and not self.op.retired)
    def materialize(self,*args):return self.op.bridge.materialize(*args)
    def finish_scope(self,*args):return self.op.bridge.finish_scope(*args)
    def quarantine(self,*args):return self.op.bridge.quarantine(*args)

class _LocalResources:
    def __init__(self,op):self.op=op
    def reserve_request(self,spec,policy):self.op.ready();return self.op.ticket
    def verify_token(self,spec,ticket,token):
        need(spec==self.op.spec and ticket is self.op.ticket and token==0);self.op.ready()
    def quarantine(self,spec,ticket,scope):nothing(self.op.resources.quarantine(spec,ticket,scope))

class _Operation:
    def _init(self,spec,policy,*,instance,layer_range,authority,resources,ticket,device,loader,factories):
        need(type(spec)is Spec and len(spec.inputs)==1 and type(policy)is Policy)
        policy.requirements(spec)
        need(type(instance)is str and bool(instance) and type(layer_range)is tuple and len(layer_range)==2)
        self.spec=spec;self.policy=policy;self.instance=instance;self.layer_range=layer_range
        self.authority=authority;self.resources=resources;self.ticket=ticket;self.device=device;self.loader=loader
        self.factory=factories or _factories;self.bridge=CompletionBridge(device);self.completion=_Completion(self)
        self.scope=None;self.retired=False;self.called=False;self.loaded=False;self.backend=None;self.metadata=None
        self.control=None;self.descriptor=None
        self.ready()
    def ready(self):
        need(not self.retired and self.ticket is not None and type(self.ticket)is not bool)
        for provider,names in ((self.resources,('verify_stage','quarantine')),
                (self.device,('hold_load','open_scope','require_open','materialize','finish_scope','require_finished','quarantine')),
                (self.loader,('load','quarantine')),
                (self.authority,('hold_load',)+(('hold_stage0',) if self.stage==0 else ('hold_call','finalize')))):
            need(provider is not None and all(callable(getattr(provider,n,None)) for n in names))
        nothing(self.resources.verify_stage(self.spec,self.ticket,self.stage))
    def _check_binding(self,b):
        s=self.spec
        need(type(b)is dict and all(b.get(k)==v for k,v in dict(owner=s.owner,task=s.request,session=s.session,
            epoch=s.epoch,stage=self.stage,model=s.model,layout=s.layout,plan=s.plan_digest,input_snapshot=s.input_snapshot,
            input_digest=s.inputs[0],token_digest=s.token_digest,preprocessing=s.preprocessing,instance=self.instance,
            declaration_digest=self.declaration_digest).items()))
    def bind_control(self,journal,descriptor):
        need(self.control is None and self.scope is None and type(journal)is ControlJournal and type(descriptor)is RoleDescriptor)
        journal.require_ready();self._check_binding(descriptor.binding)
        self.control=journal;self.descriptor=descriptor
    def begin(self,scope):
        self.ready();need(self.scope is None and type(scope)is str and re.fullmatch('[A-Za-z0-9_-]{1,128}',scope) is not None)
        self.scope=scope # Own even a partially failed local claim/open_scope.
        self._claim()
        self.bridge.open_scope(self.spec,0,scope)
    def _load(self):
        self.ready();self.loaded=True
        with _non_suppressing(self.authority.hold_load(self.spec,self.stage)) as a,_non_suppressing(self.device.hold_load(self.spec,self.stage)) as d:
            nothing(a);nothing(d);self._before_load();loaded=synchronous(self.loader.load(self.stage,self.spec))
            need(type(loaded)is Loaded and loaded.model==self.spec.model and loaded.layout==self.spec.layout
                 and loaded.instance==self.instance and loaded.layer_range==self.layer_range)
            self.backend=self.factory(self.stage,loaded,self.spec,self.bridge)
    def _start(self,scope):
        self.ready();need(scope==self.scope and scope in self.bridge.scopes and not self.called)
        self.called=True;self._before_load();self._load()
    def _control_committed(self):
        need(self.control is not None and self.descriptor is not None and not self.retired and self.metadata is not None)
        r=self.control.query(self.descriptor.command_id)
        need(r is not None and r['state']=='COMMITTED' and not r['retired']
             and r['binding']==self.descriptor.binding and r['reservation_id']==self.descriptor.reservation_id
             and r['output_digest']==result_digest(self.metadata))
    def finish(self,scope):
        need(not self.retired and scope==self.scope and self.metadata is not None and self.backend is not None)
        self.bridge.require_finished(scope,self.backend)
    def quarantine(self):
        self.retired=True;errors=[]
        actions=[self._retire_local]
        if self.scope is not None:actions.append(lambda:self.bridge.quarantine(self.scope))
        actions.append(lambda:self.resources.quarantine(self.spec,self.ticket,self.scope))
        if self.loaded:actions.append(lambda:self.loader.quarantine(self.spec))
        if self.backend is not None:actions.append(lambda:self.backend.retire(self.spec))
        for action in actions:
            try:action()
            except BaseException as e:errors.append(e)
        if errors:raise BaseExceptionGroup('local stage quarantine uncertain',errors)

class StageAOperation(_Operation):
    stage=0
    def __init__(self,spec,policy,sender,tokens,*,instance,layer_range,authority=None,resources=None,ticket=None,device=None,loader=None,factories=None):
        _require_coordinated_sender(sender)
        need(type(tokens)is bytes and len(tokens)==8 and hashlib.sha256(tokens).hexdigest()==spec.inputs[0])
        self.sender=sender;self.tokens=tokens;self.owner=None
        self.declaration_digest=hashlib.sha256(sender._coordinator_body.encode()).hexdigest()
        self._init(spec,policy,instance=instance,layer_range=layer_range,authority=authority,resources=resources,ticket=ticket,device=device,loader=loader,factories=factories)
    def _local_check(self,c,owner):
        s,p,tiles,v=self.sender._plan(c,owner,active=True)
        need((owner,s.task_id,s.session_id,s.epoch,s.model_hash,s.layout_hash,p,v)==(self.spec.owner,self.spec.request,self.spec.session,self.spec.epoch,self.spec.model,self.spec.layout,self.spec.plan_digest,self.spec.vocab))
        need(len(tiles)==1 and tiles[0].input.shape==(1,1) and tiles[0].output.shape==(1,1,self.spec.hidden))
        stage=json.loads(c.execute('SELECT body FROM p1_stages WHERE session_id=? AND stage_id=0',(self.spec.session,)).fetchone()[0])
        need((stage['layer_start'],stage['layer_end'])==self.layer_range)
    def _before_load(self):
        with self.sender._authorized() as owner,self.sender._store._tx() as c:
            self._local_check(c,owner)
            need(c.execute('SELECT spec,retired FROM local_stage_calls WHERE id=?',('stage0',)).fetchone()==(self.spec.digest,0))
            need(c.execute('SELECT 1 FROM p1_local_attempts').fetchone() is None)
    def _claim(self):
        _require_coordinated_sender(self.sender)
        with self.sender._authorized() as owner,self.sender._store._tx() as c:
            self._local_check(c,owner)
            need(c.execute('SELECT 1 FROM p1_local_attempts').fetchone() is None)
            c.execute('CREATE TABLE IF NOT EXISTS local_stage_calls(id TEXT PRIMARY KEY,spec TEXT NOT NULL,retired INTEGER NOT NULL)')
            need(c.execute('SELECT 1 FROM local_stage_calls').fetchone() is None)
            c.execute('INSERT INTO local_stage_calls VALUES(?,?,0)',('stage0',self.spec.digest))
    def committed_bytes(self):
        self._control_committed();self.finish(self.scope)
        value=self._committed_bytes()
        need(hashlib.sha256(value).hexdigest()==self.metadata['output_digest'])
        return value
    def _committed_bytes(self):
        with self.sender._authorized() as owner,self.sender._store._tx() as c:
            self._local_check(c,owner)
            need(c.execute('SELECT spec,retired FROM local_stage_calls WHERE id=?',('stage0',)).fetchone()==(self.spec.digest,0))
            row=c.execute('SELECT output,state FROM p1_local_attempts WHERE tile=0').fetchone()
            need(row is not None and row[1]=='OUTPUT_COMMITTED' and type(row[0])is bytes and len(row[0])==self.spec.hidden*4)
            need(c.execute("SELECT payload FROM p1_events WHERE id='local:output:0'").fetchone()==(canonical(['local_output',self.spec.plan_digest,0,hashlib.sha256(row[0]).hexdigest()]),))
            return row[0]
    async def __call__(self,scope):
        try:
            self._start(scope)
            self.owner=self.sender.make_owner(lambda value,**kw:self.backend.operation(scope,value,**kw),(self.tokens,),instance_id=self.instance)
            with _non_suppressing(self.authority.hold_stage0(self.spec)) as value:nothing(value)
            await self.owner.call(self.tokens,token_start=0,token_count=1)
            with _non_suppressing(self.authority.hold_stage0(self.spec)) as value:nothing(value)
            self.metadata={'output_digest':hashlib.sha256(self._committed_bytes()).hexdigest()}
            return dict(self.metadata)
        finally:
            if self.owner is not None:await self.owner.close();self.owner=None
    def _retire_local(self):
        with self.sender._store._tx() as c:
            if c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='local_stage_calls'").fetchone():
                c.execute('UPDATE local_stage_calls SET retired=1')

class StageBOperation(_Operation):
    stage=1
    def __init__(self,spec,policy,receiver,spool,*,journal_path,instance,layer_range,authority=None,resources=None,ticket=None,device=None,loader=None,factories=None):
        need(type(receiver)is ReceiptAttemptJournal and type(spool)is Spool)
        _,reservation=_require_issued_journal(receiver,1)
        self.declaration_digest=hashlib.sha256(reservation.body.encode()).hexdigest()
        b=spool.plan.binding
        need(spool.plan.counts==(1,) and spool.plan.vocab==spec.vocab
             and (b.owner,b.task,b.session_nonce,b.epoch,b.model,b.layout,b.input_snapshot,b.token_digest,b.preprocessing,b.plan_digest)==
             (spec.owner,spec.request,spec.session,spec.epoch,spec.model,spec.layout,spec.input_snapshot,spec.token_digest,spec.preprocessing,spec.plan_digest))
        self.receiver=receiver;self.spool=spool;self.journal_path=journal_path
        self.journal=None;self.driver=None;self.sink=None;self.result=None
        self._init(spec,policy,instance=instance,layer_range=layer_range,authority=authority,resources=resources,ticket=ticket,device=device,loader=loader,factories=factories)
    def received_bytes(self):
        with self.receiver._authorized() as owner,self.receiver._receipt_fence(),self.receiver._store._tx() as c:
            s,p,tiles,values=self.receiver._sources(c,owner)
            need((owner,s.task_id,s.session_id,s.epoch,s.model_hash,s.layout_hash,p)==(self.spec.owner,self.spec.request,self.spec.session,self.spec.epoch,self.spec.model,self.spec.layout,self.spec.plan_digest))
            need(len(values)==1 and len(tiles)==1 and tiles[0].input.shape==(1,1,self.spec.hidden)
                 and type(values[0])is bytes and len(values[0])==self.spec.hidden*4 and hashlib.sha256(values[0]).hexdigest()==self.spec.inputs[0])
            stage=json.loads(c.execute('SELECT body FROM p1_stages WHERE session_id=? AND stage_id=1',(self.spec.session,)).fetchone()[0])
            need((stage['layer_start'],stage['layer_end'])==self.layer_range)
            return values[0]
    def _before_load(self):
        self.received_bytes();self.spool.check(('RESERVED',))
        with self.spool.source.tx() as c:
            need(c.execute('SELECT spec,retired FROM local_stage_calls WHERE id=?',(self.spool.rid,)).fetchone()==(self.spec.digest,0))
    def _claim(self):
        self.received_bytes();self.spool.check(('RESERVED',))
        with self.spool.source.tx() as c:
            c.execute('CREATE TABLE IF NOT EXISTS local_stage_calls(id TEXT PRIMARY KEY,spec TEXT NOT NULL,retired INTEGER NOT NULL)')
            need(c.execute('SELECT 1 FROM local_stage_calls WHERE id=?',(self.spool.rid,)).fetchone() is None)
            c.execute('INSERT INTO local_stage_calls VALUES(?,?,0)',(self.spool.rid,self.spec.digest))
    async def __call__(self,scope):
        hidden=self.received_bytes();self._start(scope)
        self.sink=_PushSpool(self.spool,self.spec);self.journal=Journal(self.journal_path,self.spec)
        self.driver=Driver(self.journal,self.spec,self.policy,
            authority=_BoundAuthority(self.authority,lambda:need(self.received_bytes()==hidden)),
            resources=_LocalResources(self),backend=self.backend,completion=_DriverCompletion(self),sink=self.sink)
        self.result=self.driver.run(0,hidden,scope=scope)
        need(self.result.output_digest==self.sink.digest.hexdigest())
        self.metadata={'output_digest':self.result.output_digest};return dict(self.metadata)
    def enable_read(self):
        self._control_committed()
        self.finish(self.scope);self.received_bytes();self.sink.enable_read()
    def check_export(self):
        """Synchronous per-frame local control/source/scope check; no new grant."""
        self._control_committed();self.finish(self.scope);self.received_bytes()
        self.spool.check(('SEALED','READING'))
        need(self.result is not None and self.result.output_digest==self.metadata['output_digest'])
        with self.spool.source.tx() as c:
            need(c.execute('SELECT spec,retired FROM local_stage_calls WHERE id=?',(self.spool.rid,)).fetchone()==(self.spec.digest,0))
    def _retire_local(self):
        errors=[]
        actions=[]
        if self.driver is not None:actions.append(self.driver.retire)
        if self.sink is not None:actions.append(self.sink.quarantine)
        def mark():
            with self.spool.source.tx() as c:
                if c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='local_stage_calls'").fetchone():
                    c.execute('UPDATE local_stage_calls SET retired=1 WHERE id=?',(self.spool.rid,))
        actions.extend((mark,lambda:self.spool.source.isolate(self.spool.rid),lambda:self.spool.destination.isolate(self.spool.rid)))
        for action in actions:
            try:action()
            except BaseException as e:errors.append(e)
        if errors:raise BaseExceptionGroup('stage B retention uncertain',errors)
    def close(self):
        if self.journal is not None:self.journal.close()
