"""Private purpose-specific C control. No sockets, grants from receipts, or GPU.

Two local stores and authenticated channels; no cross-process Python handles.
Unknown records keep their charge. New control connections are history-only.
External identity, current permission and physical-resource providers are required.
"""
import asyncio,json,hashlib,secrets,threading
from contextlib import contextmanager
from dataclasses import asdict
from .online_issuance import _Database
from .online_issuance_transport import _json,_parse,_copy,_hash,_hex
from .history_query import _HistoryEndpoint,_context
from .control_exchange import _need,_nothing
from .split_role_transport import transport_scope,ResultIngress,_generation,_result_binding

SCHEMA='CREATE TABLE c_receiving(owner TEXT,task TEXT,descriptor TEXT,connection TEXT,challenge TEXT,actual TEXT,state TEXT,charge INTEGER,recipient TEXT,PRIMARY KEY(owner,task))'
_FIELDS={'version','purpose','owner','task','session','epoch','model','layout','plan_digest','input_snapshot','token_digest','preprocessing','hidden','vocab','consent_id','consent_digest','revision','c_node','b_node','resource_id','charge','receiver','b_placement','declaration_digest','authority_incarnation'}
def descriptor(value):
 d=_copy(value);_need(type(d)is dict and set(d)==_FIELDS and type(d['version'])is int and d['version']==1 and d['purpose']=='result-receive')
 for k in ('owner','task','session','consent_id','c_node','b_node','resource_id'):_need(type(d[k])is str and 0<len(d[k])<=128)
 for k in ('model','layout','plan_digest','input_snapshot','token_digest','preprocessing','consent_digest','declaration_digest'):_hex(d[k])
 _need(type(d['epoch'])is int and d['epoch']==1 and type(d['revision'])is int and d['revision']>=0 and d['c_node']!=d['b_node'])
 _need(type(d['hidden'])is int and 0<d['hidden']*4<=65536 and type(d['vocab'])is int and 0<d['vocab']*4+32768<=2097152)
 _need(type(d['b_placement'])is dict and d['b_placement'].get('node')==d['b_node'] and d['b_placement'].get('stage')==1)
 _need(type(d['authority_incarnation'])is str and len(d['authority_incarnation'])==32)
 r=d['receiver'];_need(type(r)is dict and r.get('version')==1 and r.get('node')==d['c_node'] and r.get('purpose')=='result-store' and d['charge']<=r.get('max_charge_bytes',0))
 _need(type(d['charge'])is int and d['vocab']*4+32768<=d['charge']<=2097152);return d

def actual(value,d):
 v=_copy(value);_need(type(v)is dict and set(v)=={'input_digest','receipt_digest','session','epoch','plan_digest','tile'})
 _hex(v['input_digest']);_hex(v['receipt_digest'])
 _need((v['session'],v['epoch'],v['plan_digest'],v['tile'])==(d['session'],d['epoch'],d['plan_digest'],0) and type(v['tile'])is int and type(v['epoch'])is int);return v

class ReceivingStore(_Database):
 def __init__(self,path):
  self.authority=None;self._open(path)
  if self.fresh:
   with self._tx() as c:
    self._check(True);c.execute(SCHEMA);c.execute("CREATE TABLE c_dispatch_gate(owner TEXT,task TEXT,binding TEXT,nonce TEXT,connection TEXT,state TEXT,PRIMARY KEY(owner,task))");c.execute('CREATE TABLE c_receiving_identity(incarnation TEXT)');c.execute('INSERT INTO c_receiving_identity VALUES(?)',(secrets.token_hex(32),))
  rows=self.db.execute('SELECT incarnation FROM c_receiving_identity').fetchall();_need(len(rows)==1);self.incarnation=rows[0][0];_hex(self.incarnation)
 @classmethod
 def for_authority(cls,authority):
  from .online_issuance import IssuanceAuthority
  _need(type(authority)is IssuanceAuthority and authority.enable_receiving);authority.ready()
  self=object.__new__(cls);self.authority=authority;self.db=authority.db;self.lock=authority.lock;self.incarnation=authority.incarnation;return self
 def _check(self,write=False):
  if self.authority is None:return super()._check(write)
  self.authority._check(write)
 def close(self):
  if self.authority is None:super().close()
 def history(self,owner,task):
  with self.lock:
   self._check();r=self.db.execute('SELECT state,actual,charge FROM c_receiving WHERE owner=? AND task=?',(owner,task)).fetchone()
   return None if r is None else dict(state=r[0],actual=json.loads(r[1]) if r[1] else None,charge=r[2],execution_allowed=False)
 def row(self,d):
  r=self.db.execute('SELECT descriptor,connection,challenge,actual,state,recipient FROM c_receiving WHERE owner=? AND task=?',(d['owner'],d['task'])).fetchone();_need(r is not None and r[0]==_json(d).decode());return r
 def pending(self,d,connection,challenge,recipient=None):
  with self._tx() as c:
   self._check(True);c.execute("INSERT INTO c_receiving VALUES(?,?,?,?,?,NULL,'PENDING',?,?)",(d['owner'],d['task'],_json(d).decode(),connection,challenge,d['charge'],recipient))
 def bind_recipient(self,d,connection,recipient):
  _hex(recipient)
  with self._tx() as c:
   self._check(True);r=self.row(d);_need(r[1]==connection and r[4]=='PENDING' and r[5] is None)
   c.execute('UPDATE c_receiving SET recipient=? WHERE owner=? AND task=?',(recipient,d['owner'],d['task']))
 def move(self,d,connection,old,state,proof=None):
  with self._tx() as c:
   self._check(True);r=self.row(d);_need(r[1]==connection and r[4] in old)
   if proof is not None:_need(r[3] is None);proof=_json(actual(proof,d)).decode()
   c.execute('UPDATE c_receiving SET state=?,actual=COALESCE(?,actual) WHERE owner=? AND task=?',(state,proof,d['owner'],d['task']))
 def unknown(self,d):
  with self._tx() as c:
   self._check(True);c.execute("UPDATE c_receiving SET state='UNKNOWN' WHERE owner=? AND task=? AND state!='REVOKED'",(d['owner'],d['task']))

class _Wire(_HistoryEndpoint):
 def __init__(self,channel,store,d,*,identity=None,authorize=None,maintenance=None,resources=None):
  super().__init__(channel);_need(type(store)is ReceivingStore);self.store=store;self.d=descriptor(d)
  self.identity=identity;self.authorize=authorize;self.maintenance=maintenance;self.resources=resources
  self.original=_generation(channel.w);self.seq=0;self.challenge=None;self.recipient=None;self.dead=False;self.started=False
  _need(not getattr(channel,'_c_receiving_claimed',False) and not channel._start_attempted);channel._c_receiving_claimed=True;channel._single_start=True
  self.scope=dict(task=d['task'],session=d['session'],epoch=d['epoch'],stage=1,model=d['model']) # B-result routing only; NEVER a C execution stage.
 def ready(self):
  self.current();self.store._check(True);_need(not self.dead and _generation(self.channel.w)==self.original)
  for p in (self.identity,self.authorize,self.maintenance,self.resources):_need(callable(getattr(p,'hold',None)))
 @contextmanager
 def fence(self,action):
  self.ready();permission=self.maintenance if action in ('revoke','stop-ack') else self.authorize
  with self.channel.identity_fence(self._identity),_context(self.identity.hold(self._identity)) as who:
   _need(who==self._identity.node_id)
   with _context(permission.hold(_copy(self.d),action)) as permitted:
    _nothing(permitted)
    if action not in ('revoke','stop-ack'):
     with _context(self.resources.hold(self.d['resource_id'],_copy(self.d))) as capacity:_nothing(capacity);yield
    else:yield
 async def start(self):
  self.ready()
  if not self.started:self.started=True;await self.channel.start()
 async def send(self,message,action):
  await self.channel.send(self.envelope(self.scope,self.seq,_json(message)),max_frame_bytes=65536,write_fence=lambda:self.fence(action))
 async def read(self):
  delivery=await self.channel.receive()
  try:
   self.current();self.validate(delivery.envelope,self.scope,self.seq);message=_parse(delivery.envelope.payload)
   await delivery.release();self.current();return message
  finally:delivery.envelope=None
 async def failed(self,error):
  self.dead=True
  try:self.store.unknown(self.d)
  except BaseException as other:error=BaseExceptionGroup('receiving retirement unknown',[error,other])
  await self._failed(error)

 async def close(self):
  self.dead=True
  try:self.store.unknown(self.d)
  finally:await super().close()

class ReceivingController(_Wire):
 def __init__(self,*a,**kw):
  from private_c_receiving_authority import CReceivingPolicy
  super().__init__(*a,**kw)
  _need(type(self.authorize)is CReceivingPolicy and self.maintenance is self.authorize and self.store.authority is self.authorize.issuer and self.d==self.authorize.descriptor)
 async def _request(self,op,proof=None,*,owned=False):
  _need(owned or not self._busy);self._busy=True
  try:
   self.ready();_need(self._identity.node_id==self.d['c_node'])
   with self.fence(op):
    if op=='pending':
     _need(self.challenge is None);self.authorize.claim();self.challenge=secrets.token_hex(32);self.store.pending(self.d,self.original,self.challenge)
    elif op=='activate':self.store.move(self.d,self.original,('PENDING',),'BINDING',proof)
    else:self.store.move(self.d,self.original,('PENDING','ACTIVE','BINDING','UNKNOWN'),'REVOKE_PENDING')
   request=dict(version=1,purpose='result-receive',sequence=self.seq,op=op,descriptor=self.d,challenge=self.challenge,actual=proof,recipient=self.recipient)
   await self.start();await self.send(request,op);reply=await self.read()
   expected='PENDING' if op=='pending' else ('ACTIVE' if op=='activate' else 'REVOKED')
   _need(type(reply)is dict and set(reply)=={'request','state','execution_allowed','recipient'} and reply['request']==_hash(request) and reply['state']==expected and reply['execution_allowed'] is False)
   _hex(reply['recipient'])
   _need(op=='pending' or reply['recipient']==self.recipient)
   with self.fence('stop-ack' if op=='revoke' else op):
    if op=='pending':self.store.bind_recipient(self.d,self.original,reply['recipient']);self.recipient=reply['recipient']
    if op!='pending':self.store.move(self.d,self.original,('BINDING',) if op=='activate' else ('REVOKE_PENDING',),expected)
   self.seq+=1;return self.store.history(self.d['owner'],self.d['task'])
  except BaseException as error:await self.failed(error)
  finally:self._busy=False
 async def pending(self):return await self._request('pending')
 async def activate(self,committed_input):
  from .c_receiving_input import CommittedBInput
  _need(type(committed_input)is CommittedBInput and not self._busy);self._busy=True;delegated=False
  try:
   with self.fence('activate'):_need(self.store.row(self.d)[4]=='PENDING')
   proof=actual(await committed_input.read_committed(_copy(self.d)),self.d)
   with self.fence('activate'):_need(self.store.row(self.d)[4]=='PENDING')
   delegated=True;return await self._request('activate',proof,owned=True)
  except BaseException as error:
   if not delegated:await self.failed(error)
   raise
  finally:self._busy=False
 async def revoke(self):return await self._request('revoke')

class ReceivingRole(_Wire):
 def __init__(self,*a,issuer_node=None,issuer_certificate=None,**kw):
  super().__init__(*a,**kw)
  _need(self.store.authority is None)
  _need(type(issuer_node)is str and issuer_node and self._identity.node_id==issuer_node and self._identity.certificate_sha256==issuer_certificate)
  self._close_owned_task=None;self.gate=threading.RLock();self.receiving=False;self.claimed=False;self.ingress=None;self.active_task=None;self.result_generation=None;self.binding=None;self.spec=None
 async def serve_once(self):
  _need(not self._busy);self._busy=True
  try:
   await self.start();request=await self.read()
   _need(type(request)is dict and set(request)=={'version','purpose','sequence','op','descriptor','challenge','actual','recipient'})
   _need(type(request['version'])is int and request['version']==1 and request['purpose']=='result-receive' and type(request['sequence'])is int and request['sequence']==self.seq and request['descriptor']==self.d)
   op=request['op'];_need(op in ('pending','activate','revoke'));_hex(request['challenge'])
   with self.fence(op),self.gate:
    if op=='pending':
     _need(self.challenge is None and request['actual'] is None and request['recipient'] is None);self.challenge=request['challenge'];self.store.pending(self.d,self.original,self.challenge,self.store.incarnation);state='PENDING'
    else:
     _need(request['challenge']==self.challenge and request['recipient']==self.store.incarnation)
     if op=='activate':
      proof=actual(request['actual'],self.d);self.store.move(self.d,self.original,('PENDING',),'ACTIVE',proof);self.receiving=True;state='ACTIVE'
     else:
      _need(request['actual'] is None);self.receiving=False;self.store.move(self.d,self.original,('PENDING','ACTIVE','RECEIVING','RECEIVED','UNKNOWN'),'REVOKE_PENDING');state='REVOKED'
   if op=='revoke':
    dispatch_gate=getattr(self,'_dispatch_gate',None)
    if dispatch_gate is not None:await dispatch_gate.close()
    # Stop new local fence entries before any await; join the owned receive task.
    task=self.active_task
    if task is not None and task is not asyncio.current_task():
     task.cancel();results=await asyncio.gather(task,return_exceptions=True)
     errors=[x for x in results if isinstance(x,BaseException) and not isinstance(x,asyncio.CancelledError)]
     _need(not errors)
    if self.ingress is not None:await self.ingress.close()
    with self.fence('stop-ack'),self.gate:self.store.move(self.d,self.original,('REVOKE_PENDING',),'REVOKED')
   await self.send(dict(request=_hash(request),state=state,execution_allowed=False,recipient=self.store.incarnation),'stop-ack' if op=='revoke' else op)
   self.seq+=1;return state
  except BaseException as error:await self.failed(error)
  finally:self._busy=False
 @contextmanager
 def hold_receiver(self,context,action):
  with self.fence('receive'),self.gate:
   _need(self.receiving and self.claimed and self.store.history(self.d['owner'],self.d['task'])['state']=='RECEIVING')
   _need(context['recipient_node']==self.d['c_node'] and context['purpose']=='result-store' and context['receive_binding']==self.binding)
   value=context['binding'];scope=transport_scope(self.spec)
   if value!=scope:_need(value.get('scope')==scope and value.get('actual_inputs')==list(self.spec.inputs) and value.get('generation')==self.result_generation and value.get('binding')==self.wire_binding)
   yield None
   _need(self.receiving)
 def attach_result(self,spec,reader,writer,config,metadata_channel,binding,*,identity=None,authorize=None):
  from .projection_driver import Spec
  _need(not self.claimed);self.claimed=True
  try:
   with self.fence('receive'),self.gate:
    row=self.store.history(self.d['owner'],self.d['task']);_need(self.receiving and row['state']=='ACTIVE' and type(spec)is Spec and len(spec.inputs)==1)
    _need(all(getattr(spec,k)==self.d[k] for k in transport_scope(spec) if k!='request') and spec.request==self.d['task'] and spec.inputs==(row['actual']['input_digest'],))
    _need(binding.scope.peer_node==self.d['b_node']);self.store.move(self.d,self.original,('ACTIVE',),'RECEIVING');self.spec=spec;self.binding=json.loads(json.dumps(asdict(binding)));self.result_generation=_generation(writer)
    self.wire_binding=json.loads(json.dumps(asdict(_result_binding(binding,spec,self.result_generation))))
    self.ingress=ResultIngress(reader,writer,config,metadata_channel,binding,identity=identity,authorize=authorize,recipient_node=self.d['c_node'],receive_authorize=self)
  except BaseException:self.receiving=False;self.store.unknown(self.d);raise
 async def receive_result(self,artifacts,check,publication_fence):
  _need(self.ingress is not None and self.active_task is None and self.receiving)
  self.active_task=asyncio.current_task()
  try:
   result=await self.ingress.receive(self.spec,artifacts,check,publication_fence)
   with self.gate:
    self.receiving=False;self.store.move(self.d,self.original,('RECEIVING',),'RECEIVED')
   return result
  except BaseException:
   with self.gate:
    self.receiving=False
    dispatch_gate=getattr(self,'_dispatch_gate',None)
    if dispatch_gate is not None:dispatch_gate.channel._abort()
    # A planned stop keeps REVOKE_PENDING until its owning control task joins us.
    if self.store.row(self.d)[4]!='REVOKE_PENDING':self.store.unknown(self.d);self.channel._abort()
   raise
  finally:self.active_task=None
 async def failed(self,error):
  try:await self.close()
  except BaseException as cleanup:error=BaseExceptionGroup('C cleanup unknown',[error,cleanup])
  raise error
 async def close(self):
  caller=asyncio.current_task()
  if self._close_owned_task is None:
   with self.gate:
    self.receiving=False
    dispatch_gate=getattr(self,'_dispatch_gate',None)
    if dispatch_gate is not None:dispatch_gate.channel._abort()
   async def cleanup():
    errors=[]
    dispatch_gate=getattr(self,'_dispatch_gate',None)
    if dispatch_gate is not None:
     try:await dispatch_gate.close()
     except BaseException as e:errors.append(e)
    task=self.active_task
    if task is not None and task is not caller:
     task.cancel();results=await asyncio.gather(task,return_exceptions=True)
     errors.extend(x for x in results if isinstance(x,BaseException) and not isinstance(x,asyncio.CancelledError))
    if self.ingress is not None:
     try:await self.ingress.close()
     except BaseException as e:errors.append(e)
    try:await super(ReceivingRole,self).close()
    except BaseException as e:errors.append(e)
    if errors:raise BaseExceptionGroup('C cleanup unknown',errors)
   self._close_owned_task=asyncio.create_task(cleanup())
  cancelled=None
  while not self._close_owned_task.done():
   try:await asyncio.shield(self._close_owned_task)
   except asyncio.CancelledError as e:cancelled=e
   except BaseException:break
  self._close_owned_task.result()
  if cancelled is not None:raise cancelled
