"""One-shot descriptor handoff on the original authenticated control Channel.

No listener, process launcher, provider discovery, permission ledger or execution.
Central imports ordinary metadata only; role exports its actual installed records.
READY is not distributed atomicity. Unknown/closed connections never requalify.
"""
import asyncio,json,re,secrets
from contextlib import contextmanager
from private_experiment_admission import ExperimentAdmission,need,digest
from private_experiment_installation import _current
from private_experiment_assembly import _settle
from cluster_p1.channel import Channel
from cluster_p1.control_exchange import _Endpoint,_json,_parse,ControlClient
from cluster_p1.role_control import RoleDescriptor,CoordinatorControl
from cluster_p1.role_launcher import StageLauncher
from cluster_p1.local_attempt import _non_suppressing
from cluster_p1.projection_driver import nothing
from cluster_p1.role_startup import _close_channel
from cluster_p1.delivery_receipt import _with_cleanup

_PURPOSE='installed-control-bootstrap-v1'
_FLAGS=('_descriptor_bootstrap_attempted','_control_requests_started','_role_control_claimed','_online_issuance_claimed','_experiment_action_owned','_launcher_owned','_receipt_state','_dispatch_gate_claimed','_c_receiving_claimed')

def _message(kind,**values):return dict(version=1,purpose=_PURPOSE,kind=kind,**values)
def _typed(v,kind,keys):
 need(type(v)is dict and set(v)=={'version','purpose','kind'}|set(keys) and type(v['version'])is int and v['version']==1 and v['purpose']==_PURPOSE and v['kind']==kind)
def _nonce(v):need(type(v)is str and re.fullmatch('[a-f0-9]{64}',v) is not None)
def _claim(channel):
 need(not getattr(channel,'_descriptor_bootstrap_attempted',False) and not getattr(channel.w,'_descriptor_bootstrap_attempted',False))
 channel._descriptor_bootstrap_attempted=True;channel.w._descriptor_bootstrap_attempted=True;channel._descriptor_bootstrap_ready=False
async def _send(endpoint,scope,seq,value,fence):
 raw=_json(value);need(len(raw)<=8192)
 await endpoint.channel.send(endpoint.envelope(scope,seq,raw),max_frame_bytes=65536,write_fence=fence)
 with fence():pass
async def _receive(endpoint,scope,seq,kind,keys,fence):
 delivery=await endpoint.channel.receive()
 try:
  with fence():endpoint.validate(delivery.envelope,scope,seq);need(len(delivery.envelope.payload)<=8192);value=_parse(delivery.envelope.payload);_typed(value,kind,keys)
  await delivery.release()
  with fence():pass
  return value
 finally:delivery.envelope=None

def _expected(b,stage):
 d=b['declaration'];s=d['session'];p=b['placements'][stage]
 return dict(owner=b['owner'],task=b['execution_task'],session=s['session_id'],epoch=s['epoch'],stage=stage,model=s['model_hash'],layout=s['layout_hash'],plan=d['plan_digest'],device=p['device'],instance=p['instance'],declaration_digest=digest(d),**{k:b['source'][k] for k in ('input_snapshot','token_digest','preprocessing')})

def _import(value,b,stage,charge):
 need(type(value)is dict and set(value)=={'role_node','command_id','reservation_id','binding','charge','resource_id'})
 need(type(value['charge'])is int and value['charge']==charge and value['role_node']==b['placements'][stage]['node'] and value['resource_id']==b['placements'][stage]['resource_id'])
 binding=value['binding'];expected=_expected(b,stage)
 need(type(binding)is dict and set(binding)==set(expected)|{'input_digest','peer'} and all(binding[k]==v for k,v in expected.items()))
 if stage==0:need(binding['input_digest']==b['source']['token_digest'])
 return RoleDescriptor(value['role_node'],value['command_id'],value['reservation_id'],_json(binding),charge)

async def announce_control(launcher):
 """Role-local only. Requires installed operation/control and attached action gate."""
 need(not getattr(launcher,'_control_assembly_pending',False) or asyncio.current_task() is launcher._control_assembly_task)
 need(type(launcher)is StageLauncher and launcher._attached_ready and launcher._actions_ready and asyncio.current_task() not in launcher._jobs)
 installed=launcher.installed;service=installed.role._service;channel=service.channel
 need(not getattr(channel,'_descriptor_bootstrap_attempted',False) and service._sequence==0 and not service._running and installed._active is None)
 d=installed.descriptor;binding=d.binding;scope={k:binding[k] for k in ('task','session','epoch','stage','model')}
 endpoint=_Endpoint(channel);r=installed.startup.runtime['resources'];ticket=installed.startup.runtime['ticket']
 pins=(launcher.installed,installed.role,installed.operation,service,service.channel,service._identity,service.identity,service.identity.hold,service.authorize,service.authorize.hold,r,ticket)
 resource_row=r._row(ticket.reservation_id)
 control_row=r.db.execute('SELECT rid,binding,charge,state FROM controls WHERE parent=?',(ticket.reservation_id,)).fetchone()
 store=installed.startup.store
 install_rows=store.db.execute('SELECT command,reservation,binding,charge FROM install_control ORDER BY command').fetchall()
 def leaf():
  need(pins==(launcher.installed,installed.role,installed.operation,installed.role._service,service.channel,service._identity,service.identity,service.identity.hold,service.authorize,service.authorize.hold,installed.startup.runtime['resources'],installed.startup.runtime['ticket']))
  need(not launcher._closing and not installed._closed and installed.role._descriptor is d and service._sequence==0 and not service._running)
  need(service._call==(d.command_id,d.reservation_id,d.binding,d.charge));endpoint.current()
  need(installed.startup.store is store and ticket.reservation_id not in r.poisoned and r.tickets.get(id(ticket)) is ticket)
  need(r._row(ticket.reservation_id)==resource_row and r.db.execute('SELECT rid,binding,charge,state FROM controls WHERE parent=?',(ticket.reservation_id,)).fetchone()==control_row)
  need(store.db.execute('SELECT command,reservation,binding,charge FROM install_control ORDER BY command').fetchall()==install_rows)
 def check():
  leaf();launcher.check();need(installed.descriptor is d);leaf()
 @contextmanager
 def fence():
  check()
  with service.fence(binding,'bootstrap-control'),_non_suppressing(r.hold_control(ticket.reservation_id,d.reservation_id,binding,d.charge)) as held:
   nothing(held);check();yield None;check()
  check()
 entered=False
 async def exchange():
  nonlocal entered
  entered=True # _run now owns this task and all subsequent error cleanup.
  async with asyncio.timeout(channel.timeout):
   hello=await _receive(endpoint,scope,0,'HELLO',('challenge',),fence);_nonce(hello['challenge'])
   value=dict(role_node=d.role_node,command_id=d.command_id,reservation_id=d.reservation_id,binding=d.binding,charge=d.charge,resource_id=installed.startup.descriptor['resource_id'])
   announcement=_message('ANNOUNCE',challenge=hello['challenge'],descriptor=value)
   await _send(endpoint,scope,0,announcement,fence)
   ack=await _receive(endpoint,scope,1,'ACK',('challenge','announcement'),fence)
   need(ack['challenge']==hello['challenge'] and ack['announcement']==digest(announcement))
   await _send(endpoint,scope,1,_message('READY',challenge=hello['challenge'],announcement=digest(announcement)),fence)
   with fence():pass
   service._sequence=2;channel._descriptor_bootstrap_ready=True
 _claim(channel)
 try:return await launcher._run(exchange)
 except BaseException as error:
  if entered:raise # _run or its external closer already owns cleanup; never rejoin.
  # Preserve the owning caller identity: launcher.close already shields/join
  # handles cancellation and avoids joining a cleanup waiting for this task.
  try:await launcher.close()
  except BaseException as cleanup:raise _with_cleanup(error,cleanup)
  raise

async def receive_control(a,credential,pid,sha,stage,channel,*,identity=None,resources=None,expected_charge=None):
 """Central-local import; accepts no role object, role DB or caller descriptor.

 Fresh central challenge and original installed role identity are mandatory.
 Caller supplies a trusted expected charge; announced metadata cannot choose it.
 """
 need(type(a)is ExperimentAdmission and type(channel)is Channel and type(stage)is int and stage in (0,1))
 need(callable(getattr(identity,'hold',None)) and callable(getattr(resources,'hold',None)))
 need(type(expected_charge)is int and 0<expected_charge<2**63)
 v=a._installations.get((pid,stage));need(v is not None and v['attempted'] and v['credential']==credential and v['sha']==sha)
 need(resources is a._issuer_resources and resources is v['service'].resources)
 old=v['service'].channel;need(channel is not old and channel.w is not old.w and channel._start_attempted)
 need(not any(getattr(channel,k,False) or getattr(channel.w,k,False) for k in _FLAGS))
 endpoint=_Endpoint(channel);peer=endpoint._identity;old_peer=v['service']._identity
 need((peer.node_id,peer.certificate_sha256)==(old_peer.node_id,old_peer.certificate_sha256))
 endpoint.current();need((pid,stage) not in getattr(a,'_control_bootstrap_attempts',set()))
 if not hasattr(a,'_control_bootstrap_attempts'):a._control_bootstrap_attempts=set()
 c=a.consent;owner=v['owner'];body=v['body'];scope={k:_expected(body,stage)[k] for k in ('task','session','epoch','stage','model')}
 pins=(identity,identity.hold,resources,resources.hold,v,v['service'],peer);snapshot=digest(body)
 def leaf():
  need(pins==(identity,identity.hold,resources,resources.hold,a._installations.get((pid,stage)),v['service'],endpoint._identity))
  need(not a.retired and not c.fence.dead and body['source_task'] not in c.fence.tasks and a._issuer_resources is resources)
  need(digest(body)==snapshot and v['sha']==sha);endpoint.current()
  row=a._row(owner,pid);need(row[1]==sha and row[2]==a.boot and row[3]=='REGISTERED')
  need(not a.db.execute('SELECT 1 FROM revoked WHERE owner=? AND source=?',(owner,body['source_task'])).fetchone())
 def check():
  leaf();need(digest(_current(a,v))==snapshot);v['service'].current();leaf()
 @contextmanager
 def fence():
  with c._user(credential,owner),c.fence.lock:
   check()
   with channel.identity_fence(peer),_non_suppressing(identity.hold(scope,peer)) as who,a._policy_scope(body,'bootstrap-control'),_non_suppressing(resources.hold(v['descriptor']['resource_id'],json.loads(json.dumps(v['descriptor'])))) as held:
    nothing(who);nothing(held);check();yield None;check()
   check()
  check()
 a._control_bootstrap_attempts.add((pid,stage));_claim(channel)
 try:
  async with asyncio.timeout(channel.timeout):
   with fence():pass
   challenge=secrets.token_hex(32)
   await _send(endpoint,scope,0,_message('HELLO',challenge=challenge),fence)
   announcement=await _receive(endpoint,scope,0,'ANNOUNCE',('challenge','descriptor'),fence)
   need(announcement['challenge']==challenge);descriptor=_import(announcement['descriptor'],body,stage,expected_charge)
   ack=_message('ACK',challenge=challenge,announcement=digest(announcement))
   await _send(endpoint,scope,1,ack,fence)
   ready=await _receive(endpoint,scope,1,'READY',('challenge','announcement'),fence)
   need(ready==dict(ack,kind='READY'))
   with fence():pass
   client=ControlClient(channel);client._sequence=2
   control=CoordinatorControl(client,descriptor);channel._descriptor_bootstrap_ready=True
   with fence():pass
   return control
 except BaseException as error:
  a._unknown(pid);cleanup=asyncio.create_task(_close_channel(channel))
  try:await _settle(cleanup)
  except BaseException as cleanup:raise _with_cleanup(error,cleanup)
  raise

async def bind_control_ready(launcher,reader,writer,tls,grants,action_channel,*,enabled=False,identity=None,authorize=None,action_identity=None,clock=None,authority_node=None,authority_certificate=None):
 """Role-local attach/actions/announce, using existing owners and records.

 Trusted bootstrap supplies providers and authenticated streams. No provider
 discovery, process/listener, credential or execution authority is created.
 Before take, supplied streams remain caller-owned on rejection. After take,
 every failure closes adopted and unadopted transports; eligibility/charge stay
 consumed/unknown. READY does not prove central receipt or physical device stop.
 """
 need(enabled is True and type(launcher)is StageLauncher)
 launcher._control_dial_entry()
 need(not getattr(launcher,'_control_assembly_attempted',False) and asyncio.current_task() not in launcher._jobs)
 launcher.check()
 need(launcher._installed_ready and launcher.operation is not None and not launcher._attach_attempted and not launcher._actions_attempted)
 session=launcher.startup.client._session
 need(session is not None and session.active);session.check()
 for p in (identity,authorize,action_identity):need(callable(getattr(p,'hold',None)))
 need(callable(getattr(clock,'sample',None)))
 from private_experiment_actions import sample
 sample(clock)
 need(type(authority_node)is str and authority_node==launcher.config.data['issuer_node'])
 need(authority_certificate==launcher.config.data['issuer_certificate'])
 need(type(action_channel)is Channel and not action_channel._start_attempted and writer is not action_channel.w)
 need(not any(getattr(action_channel,k,False) or getattr(action_channel.w,k,False) for k in _FLAGS))
 launcher._available_writer(writer);launcher._available_writer(action_channel.w)
 # Existing complete purpose checks and ownership happen before any await.
 launcher._take_channel(action_channel)
 launcher._control_assembly_attempted=True;launcher._control_assembly_pending=True;launcher._control_assembly_task=asyncio.current_task()
 try:
  role=await launcher.attach(reader,writer,tls,grants,identity=identity,authorize=authorize)
  control=launcher.control;service=role.role._service
  need(control.w is writer and service.channel is control and launcher.installed is role)
  await launcher._attach_taken_actions(action_channel,authority_node=authority_node,authority_certificate=authority_certificate,identity=action_identity,clock=clock)
  await announce_control(launcher)
  launcher.check();session.check()
  need(launcher.installed is role and launcher.control is control and role.role._service is service and service.channel is control)
  need(launcher._control_assembly_task is asyncio.current_task())
  control._descriptor_bootstrap_pending=False;launcher._control_assembly_pending=False;launcher._control_assembly_task=None
  return role
 except BaseException as error:
  errors=[]
  # All adopted transports already belong to launcher. Never create a second
  # task which joins an external close that is itself waiting for this caller.
  if not getattr(writer,'_launcher_owned',False):
   async def orphan():
    failures=[]
    try:writer.close()
    except BaseException as failure:failures.append(failure)
    try:await asyncio.wait_for(writer.wait_closed(),10)
    except BaseException as failure:failures.append(failure)
    if failures:raise BaseExceptionGroup('unadopted control stream cleanup unknown',failures)
   try:await _settle(asyncio.create_task(orphan()))
   except BaseException as failure:errors.append(failure)
  close=launcher._close_task
  try:
   if close is None or launcher._close_caller is asyncio.current_task():await launcher.close()
   elif close.done():close.result()
   # Otherwise the original external owner retains/joins close; this caller
   # must exit so that owner's snapshot of jobs can settle. No refund/stop ACK.
  except BaseException as failure:errors.append(failure)
  if errors:raise _with_cleanup(error,BaseExceptionGroup('role control assembly cleanup unknown',errors))
  raise
