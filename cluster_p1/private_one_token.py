"""Private single-token composition. Missing trusted execution dependencies DENY.

No listeners/default invocation. Actual auth/resource/device/conduit providers
are external; tests substitute only those boundaries and loaded computation.
"""
from contextlib import contextmanager
from dataclasses import dataclass,replace
import hashlib,json,os,struct,uuid
from .projection_driver import Spec,Policy,Journal,Driver,Denied,need,nothing,synchronous
from .byte_spool import Binding,Plan,Book,Spool,CHUNK,MANIFEST
from .local_attempt import LocalAttemptJournal,_non_suppressing
from .receipt_attempt import ReceiptAttemptJournal
from .nonce_coordinator import _require_coordinated_pair
from .async_role_bridge import AsyncRoleBridge
from .control_exchange import ControlPair,result_digest
from .envelope import Envelope,decode
from .store import canonical
from token_input_snapshot import read_input
from result_artifacts import _Ledger
from isolated_tls_delivery import ReceiveBinding,_binding
from framed_result_transport import ObjectContext,payload,MAX_OBJECT

@dataclass(frozen=True)
class Loaded:
    stage:object
    model:str
    layout:str
    layer_range:tuple
    instance:str

class PinnedStageLoader:
    """Source-pin validating loader; construction does not import/load a model."""
    def __init__(self,roots,devices,config_sha,weight_sha,layout_sha,split,instances):
        self.args=(roots,devices,config_sha,weight_sha,layout_sha,split,instances)
        self.retired=False;self.loaded=[];self.started=set()
    def load(self,stage,spec):
        roots,devices,config,weights,layout,split,instances=self.args
        need(not self.retired and stage in (0,1) and stage not in self.started and spec.model==weights and spec.layout==layout)
        self.started.add(stage)
        from compute.qwen2_tiled_stage import TiledQwen2Stage
        value=TiledQwen2Stage.from_safetensors(roots[stage],expected_sha256=weights,
            expected_config_sha256=config,stage_id=stage,split_layer=split,device=devices[stage])
        self.loaded.append(value)  # keep ownership even if validation below fails
        need(value.meta['hidden']==spec.hidden and value.meta['vocab']==spec.vocab)
        return Loaded(value,weights,layout,value.layer_range,instances[stage])

    def quarantine(self,spec):
        self.retired=True  # retain stages; external load scope owns partial failures

class CompletionBridge:
    """Ownership tracking only; external device adapter must supply REAL fences."""
    def __init__(self,device):
        self.device=device;self.scopes={};self.retired=False
        # Terminal responsibility stays with the original provider, even if
        # a failing cleanup callback changes the live device reference.
        self._device_quarantine=getattr(device,'quarantine',None)
    def open_scope(self,spec,token,scope):
        need(not self.retired and scope not in self.scopes)
        self.scopes[scope]=dict(spec=spec,owner=None,pending=None,finished=False)
        nothing(self.device.open_scope(spec,token,scope))
    def require_open(self,scope,spec,adapter):
        s=self.scopes[scope];need(not self.retired and s['spec']==spec and not s['finished'])
        need(s['owner'] is None or s['owner'] is adapter);s['owner']=adapter
        nothing(self.device.require_open(scope,spec,adapter))
        need(not self.retired)
    def require_materialized(self,scope,adapter):
        s=self.scopes[scope];need(not self.retired and s['owner'] is adapter and s['pending'] is None)
    def register_pending(self,scope,adapter,pending):
        self.require_materialized(scope,adapter);s=self.scopes[scope]
        need(not s['finished'] and pending.scope==scope and pending.owner is adapter)
        s['pending']=pending
    def materialize(self,scope,pending,size):
        s=self.scopes[scope];need(not self.retired and s['pending'] is pending and not s['finished'])
        limit=65536 if getattr(s['owner'],'stage_id',None)==0 else CHUNK
        need(size==pending.count*4 and 0<size<=limit)
        value=synchronous(self.device.materialize(scope,pending,size))
        need(not self.retired and type(value)is bytes and len(value)==size)
        pending.tensor=None;s['pending']=None
        return value
    def finish_scope(self,scope):
        s=self.scopes[scope];need(not self.retired and s['pending'] is None and not s['finished'])
        nothing(self.device.finish_scope(scope));need(not self.retired);s['finished']=True
    def require_finished(self,scope,adapter):
        s=self.scopes[scope];need(not self.retired and s['owner'] is adapter and s['pending'] is None and s['finished'])
        nothing(self.device.require_finished(scope,adapter));need(not self.retired)
    def quarantine(self,scope):self.retired=True;nothing(self._device_quarantine(scope))

class _PushSpool:
    """Concrete single-token sink adapting the existing Book/Spool read format."""
    def __init__(self,spool,spec):
        self.spool=spool;self.spec=spec;self.file=None;self.column=0;self.digest=hashlib.sha256();self.chunks=[]
        b=spool.source
        with b.tx() as c:
            c.execute('CREATE TABLE IF NOT EXISTS private_producer_v1(id TEXT PRIMARY KEY,spec TEXT NOT NULL,state TEXT NOT NULL,working INTEGER NOT NULL,retired INTEGER NOT NULL)')
            need(c.execute('SELECT 1 FROM private_producer_v1 WHERE id=?',(spool.rid,)).fetchone() is None)
            c.execute("INSERT INTO private_producer_v1 VALUES(?,?,'CONSUMED',0,0)",(spool.rid,spec.digest))
        spool.check(('RESERVED',));b.transition(spool.rid,spool.plan,'source','RESERVED','ADMITTED')
        self.file=spool.open_preallocated()
    def append(self,token,column,block):
        self.spool.check();need(token==0 and column==self.column and type(block)is bytes)
        expected=min(8192,self.spec.vocab-column)*4
        need(len(block)==expected and 0<expected<=CHUNK)
        need(all((x[0]&0x7f800000)!=0x7f800000 for x in struct.iter_unpack('<I',block)))
        need(self.file.write(block)==len(block));self.column+=len(block)//4
        self.digest.update(block);self.chunks.append(hashlib.sha256(block).hexdigest())
    def seal(self,token,digest):
        self.spool.check();need(token==0 and self.column==self.spec.vocab and digest==self.digest.hexdigest())
        self.file.flush();os.fchmod(self.file.fileno(),0o400);os.fsync(self.file.fileno());self.file.close();self.file=None
        fd=os.open(self.spool.root,os.O_RDONLY|os.O_DIRECTORY)
        try:os.fsync(fd)
        finally:os.close(fd)
        self.spool.check()
        manifest=canonical({'plan':json.loads(self.spool.plan.encoded()),'objects':[{'bytes':self.spec.output_bytes,'sha256':digest,'chunks':self.chunks}],
            'sha256':digest,'file_identity':self.spool.identity(),'directory_identity':list(self.spool._directory_identity)})
        need(len(manifest.encode())<=MANIFEST)
        self.spool.check()
        self.spool.source.transition(self.spool.rid,self.spool.plan,'source','ADMITTED','SEALED',manifest)
    def enable_read(self):
        with self.spool.source.tx() as c:
            need(c.execute("UPDATE private_producer_v1 SET state='DONE' WHERE id=? AND spec=? AND state='CONSUMED' AND retired=0",(self.spool.rid,self.spec.digest)).rowcount==1)
    def quarantine(self,*args):
        failures=[]
        try:
            if self.file is not None:self.file.close();self.file=None
        except BaseException as e:failures.append(e)
        try:
            with self.spool.source.tx() as c:
                c.execute('UPDATE private_producer_v1 SET retired=1 WHERE id=?',(self.spool.rid,))
                c.execute('UPDATE reservation SET retired=1 WHERE id=?',(self.spool.rid,))
        except BaseException as e:failures.append(e)
        if failures:raise BaseExceptionGroup('spool quarantine incomplete',failures)

class _Resources:
    def __init__(self,provider,ticket):self.p=provider;self.ticket=ticket
    def reserve_request(self,spec,policy):nothing(self.p.verify_stage1(spec,self.ticket));return self.ticket
    def verify_token(self,spec,ticket,token):need(ticket is self.ticket and token==0);nothing(self.p.verify_stage1(spec,ticket))
    def quarantine(self,spec,ticket,scope):nothing(self.p.quarantine(spec,ticket,scope))

class _BoundAuthority:
    def __getattr__(self,name):return getattr(self.parent,name)
    def __init__(self,parent,check):self.parent=parent;self.check=check
    @contextmanager
    def hold_call(self,spec,token):
        with _non_suppressing(self.parent.hold_call(spec,token)) as value:
            nothing(value);self.check();yield None;self.check()
    def finalize(self,spec,token,digest,commit):
        self.check()
        def checked():self.check();commit()
        return self.parent.finalize(spec,token,digest,checked)

def _factories(stage,loaded,spec,completion):
    from compute.qwen2_projection_adapter import Stage0ByteAdapter,Qwen2ProjectionAdapter
    if stage==0:return Stage0ByteAdapter(loaded.stage,spec,completion=completion)
    return Qwen2ProjectionAdapter(loaded.stage,spec,completion=completion,
        expected_model=loaded.model,expected_layout=loaded.layout,expected_layer_range=loaded.layer_range)

def _coalesce(blocks):
    held=b''
    for block in blocks:
        need(type(block)is bytes and 0<len(block)<=CHUNK)
        held+=block
        if len(held)==65536:yield held;held=b''
    if held:yield held

async def run_one_token(*,spec,policy,sender,receiver,artifacts,source_book,destination_book,
                        spool_root,journal_path,result_key,authority=None,resources=None,
                        device=None,loader=None,conduit=None,factories=None,control=None):
    # Check all missing execution dependencies BEFORE I/O, grants, reserve or load.
    need(all(x is not None for x in (authority,resources,device,loader,conduit)))
    need(control is None or type(control)is ControlPair)
    asynchronous=type(conduit)is AsyncRoleBridge
    need(control is None or asynchronous)
    conduit_methods=('require_current','exchange_hidden','publish_result','reservation_generation','transport_buffer_bytes','validate_journal','close') if asynchronous else ('require_current','exchange_hidden','exchange_result','result_binding')
    for provider,methods in (
        (authority,('hold_pipeline','hold_load','hold_stage0','hold_call','finalize','hold_publish')),
        (resources,('reserve_pipeline','verify_stage1','quarantine')+(('bind_control',) if control is not None else ())),
        (device,('hold_load','open_scope','require_open','materialize','finish_scope','require_finished','quarantine')),
        (loader,('load','quarantine')),
        (conduit,conduit_methods)):
        need(all(callable(getattr(provider,name,None)) for name in methods))
    need(type(spec)is Spec and len(spec.inputs)==1 and spec.epoch==1 and type(policy)is Policy)
    _require_coordinated_pair(sender,receiver)
    need(type(artifacts)is _Ledger)
    if asynchronous:conduit.validate_journal(receiver)
    need(type(source_book)is Book and type(destination_book)is Book)
    from .local_stage_operations import StageAOperation,StageBOperation
    required=policy.requirements(spec);factory=factories or _factories
    class LegacyResources:
        # Existing full-pipeline reservation already covers both stages. This
        # adapter preserves the legacy verifier; split roles require verify_stage.
        def verify_stage(self,actual,ticket,stage):nothing(resources.verify_stage1(actual,ticket))
        def quarantine(self,*args):nothing(resources.quarantine(*args))
    local_resources=LegacyResources();op_a=None;op_b=None
    ticket=None;reservation_started=False;sink=None;spool=None;journal=None;result=None
    @contextmanager
    def authority_scope(factory):
        if control is None:
            with _non_suppressing(factory()) as value:
                nothing(value);yield None
        else:
            # Control/data awaits must never retain a local identity RLock.
            with _non_suppressing(factory()) as value:nothing(value)
            yield None
            with _non_suppressing(factory()) as value:nothing(value)
    def confirmed(receipt,metadata):
        need(type(receipt)is dict and receipt.get('state')=='COMMITTED' and receipt.get('retired') is False
             and receipt.get('execution_allowed') is False and receipt.get('result_digest')==result_digest(metadata))
    def source_context():
        _require_coordinated_pair(sender,receiver)
        with sender._authorized() as owner,sender._store._tx() as c:
            s,p,tiles,v=sender._plan(c,owner,active=True)
            need((owner,s.task_id,s.session_id,s.epoch,s.model_hash,s.layout_hash,p,v)==(spec.owner,spec.request,spec.session,spec.epoch,spec.model,spec.layout,spec.plan_digest,spec.vocab))
            need(len(tiles)==1 and tiles[0].input.shape==(1,1) and tiles[0].output.shape==(1,1,spec.hidden))
        return p
    def committed_hidden():
        source_context();row=sender.query(0);need(row is not None and row['state']=='OUTPUT_COMMITTED')
        value=row['output'];need(type(value)is bytes and len(value)==spec.hidden*4)
        with sender._authorized(),sender._store._tx() as c:
            expected=canonical(['local_output',spec.plan_digest,0,hashlib.sha256(value).hexdigest()])
            need(c.execute("SELECT payload FROM p1_events WHERE id='local:output:0'").fetchone()==(expected,))
        with receiver._authorized() as owner,receiver._receipt_fence(),receiver._store._tx() as c:
            s,p,tiles,values=receiver._sources(c,owner)
            need(p==spec.plan_digest and len(values)==1 and values[0]==value)
        return value
    try:
        with authority_scope(lambda:authority.hold_pipeline(spec)) as auth:
            nothing(auth);source_context();need(sender.query(0) is None);nothing(conduit.require_current(spec))
            task,binding=artifacts._metadata(spec.owner,spec.request)
            stored=read_input(artifacts._directory,task.snapshot,binding)
            need(task.state=='STORED' and task.snapshot.digest==spec.input_snapshot and binding.model_digest==spec.model and binding.preprocessing_sha256==spec.preprocessing and binding.vocab_size==spec.vocab)
            need(len(stored.tokens)==1 and stored.token_blob_sha256==spec.token_digest)
            tokens=struct.pack('<q',stored.tokens[0]);need(hashlib.sha256(tokens).hexdigest()==spec.inputs[0])
            attempt=artifacts._db.execute('SELECT session_id,epoch,plan_claim,state FROM inert_attempts WHERE task_id=?',(spec.request,)).fetchone()
            need(attempt==(spec.session,spec.epoch,spec.plan_digest,'PENDING'))
            edge=receiver._receipt_edge
            assignments=artifacts._db.execute('SELECT stage_id,node_id,device_claim,instance_claim FROM inert_holds WHERE task_id=? ORDER BY stage_id',(spec.request,)).fetchall()
            need(len(assignments)==2 and tuple((r[0],r[1]) for r in assignments)==((0,edge.sender_node),(1,edge.receiver_node)))
            with sender._authorized(),sender._store._tx() as c:
                stages=[json.loads(row[0]) for row in c.execute('SELECT body FROM p1_stages WHERE session_id=? ORDER BY stage_id',(spec.session,))]
            need(len(stages)==2)
            ranges=tuple((s['layer_start'],s['layer_end']) for s in stages)
            declaration_digest=hashlib.sha256(sender._coordinator_body.encode()).hexdigest()
            if control is not None:control.check(spec,tuple(assignments),declaration_digest=declaration_digest)
            probe=Envelope(spec.request,spec.session,spec.epoch,0,0,(0,1),spec.model,edge.layout_version,(1,1,spec.hidden),'float32',bytes(spec.hidden*4))
            need(len(probe.encode())<=65536)
            transport_buffers=conduit.transport_buffer_bytes(spec) if asynchronous else 0
            # Four logical receive capacities replace the legacy coalescing buffer.
            # Provider must additionally admit actual decode/copy/allocator peaks.
            need(type(transport_buffers)is int and transport_buffers>=0)
            control_buffers=control.buffer_bytes() if control is not None else 0
            need(type(control_buffers)is int and control_buffers>=0)
            pipeline_host=required['host']+(0 if asynchronous else 65536)+len(probe.encode())+transport_buffers+control_buffers
            need(pipeline_host<=policy.host_capacity)
            if control is not None:
                control.check(spec,tuple(assignments),declaration_digest=declaration_digest,
                              minimum_charge=required['device']+pipeline_host+required['source']+required['destination'])
            reservation_started=True;ticket=synchronous(resources.reserve_pipeline(spec,policy,dict(required,pipeline_host=pipeline_host,transport_buffers=transport_buffers,control_buffers=control_buffers,assignments=tuple(assignments),layer_ranges=ranges)))
            need(ticket is not None and type(ticket)is not bool)
            generation=conduit.reservation_generation(spec) if asynchronous else receiver._receipt_peer.connection_generation
            bp=Plan(Binding(spec.owner,spec.request,spec.session,'single-token',spec.epoch,generation,
                spec.model,spec.layout,spec.input_snapshot,spec.token_digest,spec.preprocessing,spec.plan_digest),(1,),spec.vocab)
            source_book.reserve(result_key,bp,'source');destination_book.reserve(result_key,bp,'destination')
            source_book.confirm(result_key,bp,'source');destination_book.confirm(result_key,bp,'destination')
            # Permanent local consumption precedes load/any possible device work.
            # External single-coordinator registry is still required across books.
            with source_book.tx() as c:
                c.execute('CREATE TABLE IF NOT EXISTS assembly_calls(id TEXT PRIMARY KEY,spec TEXT NOT NULL)')
                need(c.execute('SELECT 1 FROM assembly_calls WHERE id=?',(result_key,)).fetchone() is None)
                c.execute('INSERT INTO assembly_calls VALUES(?,?)',(result_key,spec.digest))
            async def stage0_body():
                nonlocal op_a
                source_context();need(sender.query(0) is None)
                op_a=StageAOperation(spec,policy,sender,tokens,instance=assignments[0][3],layer_range=ranges[0],
                    authority=authority,resources=local_resources,ticket=ticket,device=device,loader=loader,factories=factory)
                scope=uuid.uuid4().hex;op_a.begin(scope)
                with authority_scope(lambda:authority.hold_stage0(spec)) as value:
                    nothing(value);metadata=await op_a(scope)
                op_a.finish(scope)
                return metadata
            if control is None:await stage0_body()
            else:
                descriptor=control.describe(0,spec)
                nothing(await resources.bind_control(spec,ticket,0,descriptor))
                receipt=await control.dispatch(0,spec,stage0_body)
                row=sender.query(0);need(row is not None and row['state']=='OUTPUT_COMMITTED')
                confirmed(receipt,{'output_digest':hashlib.sha256(row['output']).hexdigest()})
            row=sender.query(0);need(row is not None and row['state']=='OUTPUT_COMMITTED')
            envelope=replace(probe,payload=row['output']);raw=envelope.encode()
            received=await conduit.exchange_hidden(raw) if asynchronous else synchronous(conduit.exchange_hidden(raw))
            need(type(received)is bytes and received==raw)
            if not asynchronous:receiver.make_receiver().receive_bytes(received)
            hidden=committed_hidden();actual=replace(spec,inputs=(hashlib.sha256(hidden).hexdigest(),))
            async def stage1_body():
                nonlocal op_b,sink,journal,spool,result
                need(committed_hidden()==hidden)
                spool=Spool(spool_root,source_book,destination_book,result_key,bp)
                op_b=StageBOperation(actual,policy,receiver,spool,journal_path=journal_path,
                    instance=assignments[1][3],layer_range=ranges[1],
                    authority=_BoundAuthority(authority,lambda:need(committed_hidden()==hidden)),
                    resources=local_resources,ticket=ticket,device=device,loader=loader,factories=factory)
                scope=uuid.uuid4().hex;op_b.begin(scope)
                try:
                    metadata=await op_b(scope);op_b.finish(scope)
                    return metadata
                finally:sink,journal,result=op_b.sink,op_b.journal,op_b.result
            if control is None:await stage1_body()
            else:
                descriptor=control.describe(1,actual)
                nothing(await resources.bind_control(actual,ticket,1,descriptor))
                receipt=await control.dispatch(1,actual,stage1_body)
                need(result is not None)
                confirmed(receipt,{'output_digest':result.output_digest})
        # Pipeline authorization exit must also succeed before source export.
        sink.enable_read()
        if asynchronous:
            def check():
                need(committed_hidden()==hidden);nothing(conduit.require_current(actual))
            @contextmanager
            def publication_fence():
                # No await inside this final authority scope/ledger commit.
                with _non_suppressing(authority.hold_publish(actual)) as auth:
                    nothing(auth);check();yield None
            digest=await conduit.publish_result(actual,spool,artifacts,result_key,result.output_digest,check,publication_fence)
            return {'manifest_digest':digest,'output_bytes':spec.output_bytes,'execution_allowed':False,'computation_verified':False}
        with _non_suppressing(authority.hold_publish(actual)) as auth:
            nothing(auth);need(committed_hidden()==hidden);nothing(conduit.require_current(actual))
            binding=synchronous(conduit.result_binding(actual));need(type(binding)is ReceiveBinding);_binding(binding,MAX_OBJECT)
            s=binding.scope
            need((s.direction,s.port,s.attempt,s.peer_node)==('out','logits','single-token',assignments[1][1]))
            need((s.task,s.session,s.epoch,s.plan,s.layout,binding.stage_id,binding.model_hash,binding.layout_version,binding.dtype,binding.tiles)==
                 (spec.request,spec.session,spec.epoch,spec.plan_digest,spec.layout,1,spec.model,edge.layout_version,'float32',((0,1,(1,1,spec.vocab)),)))
            context=ObjectContext(binding,hashlib.sha256(result_key.encode()).hexdigest(),spec.output_bytes,result.output_digest,s.incarnation)
            def blocks():
                for frame in spool.frames():
                    nothing(conduit.require_current(actual));need(committed_hidden()==hidden)
                    wire=payload(context,frame.sequence,frame.offset,frame.block)
                    e=Envelope(spec.request,spec.session,spec.epoch,1,frame.sequence,(0,1),spec.model,binding.layout_version,(len(wire),),'uint8',wire)
                    incoming=synchronous(conduit.exchange_result(e.encode()))
                    parsed=decode(incoming,expected=(spec.request,spec.session,spec.epoch))
                    need(parsed==e);yield parsed.payload[48:]
            def final_guard():
                need(committed_hidden()==hidden);nothing(conduit.require_current(actual))
            digest=artifacts.publish(spec.owner,spec.request,result_key,'prefill_logits_v1',spec.layout,
                ((0,1,result.output_digest),),(_coalesce(blocks()),),_guard=final_guard)
            return {'manifest_digest':digest,'output_bytes':spec.output_bytes,'execution_allowed':False,'computation_verified':False}
    except BaseException as error:
        failures=[]
        for action in ([op_a.quarantine] if op_a is not None else [])+([op_b.quarantine] if op_b is not None else [])+([lambda:resources.quarantine(spec,ticket,None)] if reservation_started else [])+([lambda:source_book.isolate(result_key),lambda:destination_book.isolate(result_key)] if reservation_started else []):
            try:action()
            except BaseException as e:failures.append(e)
        if control is not None:
            try:await control.close()
            except BaseException as e:failures.append(e)
        if asynchronous:
            try:await conduit.close()
            except BaseException as e:failures.append(e)
        if failures:raise BaseExceptionGroup('pipeline failed; quarantine may be unknown',[error,*failures]) from None
        raise
    finally:
        if journal is not None:journal.close()
