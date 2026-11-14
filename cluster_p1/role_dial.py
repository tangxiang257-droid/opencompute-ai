"""Private raw mTLS dial into one existing launcher; no default host enablement.

Trusted local bootstrap supplies endpoints, TLS material and real providers.
Method shapes are not proof of trust. No listener, install, model or dispatch.
Never pass a preconstructed Channel: it already owns a stream reader.
"""
import asyncio,math,ssl
from .role_launcher import StageLauncher,_stage_preflight
from .channel import TLSConfig,Grant
from .control_exchange import _need
from .online_issuance_transport import _json,_parse
from .prepared_receipt import _retire_failed
from .delivery_receipt import _with_cleanup
from .tls_material import load_context

async def dial_stage(config,host,port,tls,grants,store,d,*,material=None,material_pin=None,server_hostname=None,enabled=False,timeout=10,identity=None,identity_mode=None,authorize=None,resources=None,completion=None,runtime=None):
 _need(enabled is True)
 _need(type(material)is bytes and type(material_pin)is str)
 for value in (host,server_hostname):_need(type(value)is str and 0<len(value)<=253 and not any(c.isspace() or ord(c)<32 for c in value))
 _need(type(port)is int and 1<=port<=65535 and type(timeout) in (int,float) and math.isfinite(timeout) and 0<timeout<=10)
 opts=dict(identity=identity,identity_mode=identity_mode,authorize=authorize,resources=resources,completion=completion,runtime=runtime)
 c=_stage_preflight(config,store,d,**opts)
 _need(type(tls)is TLSConfig and tls.peers=={c['issuer_node']:c['issuer_certificate']})
 _need(type(grants)is dict and set(grants)=={c['issuer_node']} and type(grants[c['issuer_node']]) in (set,frozenset) and bool(grants[c['issuer_node']]))
 _need(all(type(g)is Grant for g in grants[c['issuer_node']]))
 fixed_tls=TLSConfig(tls.certfile,tls.keyfile,tls.cafile,dict(tls.peers))
 fixed_grants={k:set(v) for k,v in grants.items()};descriptor=_parse(_json(d));opts['runtime']=dict(runtime)
 def pins():
  providers=[config,store,store.identity,store.authorize,store.device,identity,authorize,completion,resources,resources.authority,resources.capacity,*opts['runtime'].values()]
  methods=tuple((id(p),name,getattr(p,name,None)) for p in providers for name in ('ready','check','bound','claim','path','hold_control','verify_stage','hold_device','hold','hold_load','hold_stage0','hold_call','finalize','check','begin','finish','quarantine','open_scope','require_open','materialize','finish_scope','require_finished','load'))
  return (config._raw,config._sha,config.root,config._inode,config.pid,store.resources,resources.authority,resources.capacity,methods)
 original=pins();ctx=None;context_pin=None
 def leaf():
  _need(pins()==original);config.check();resources.check(True)
  ticket=opts['runtime']['ticket'];rid=descriptor['resource_id']
  _need(resources.tickets.get(id(ticket)) is ticket and ticket.reservation_id==rid and rid not in resources.poisoned)
  row=resources._row(rid);_need(row[5]=='HELD' and _parse(row[0].encode())==descriptor)
  _need(fixed_tls.peers=={c['issuer_node']:c['issuer_certificate']})
  if ctx is not None:_need((ctx.verify_mode,ctx.check_hostname,ctx.minimum_version)==context_pin)
 def check():
  leaf();_stage_preflight(config,store,descriptor,**opts)
  ticket=opts['runtime']['ticket'];_need(resources.tickets.get(id(ticket)) is ticket and ticket.reservation_id==descriptor['resource_id'])
  with resources.hold(descriptor['resource_id'],descriptor):pass
  leaf()
 check();ctx=load_context(fixed_tls,material,material_pin)
 _need(type(ctx)is ssl.SSLContext and ctx.verify_mode==ssl.CERT_REQUIRED and ctx.check_hostname and ctx.minimum_version>=ssl.TLSVersion.TLSv1_2)
 context_pin=(ctx.verify_mode,ctx.check_hostname,ctx.minimum_version);check()
 # No local fence/transaction spans the actual network await.
 dial=asyncio.create_task(asyncio.open_connection(host,port,ssl=ctx,server_hostname=server_hostname))
 streams=None;launch=None
 try:
  done,_=await asyncio.wait((dial,),timeout=timeout)
  if dial not in done:raise TimeoutError('role TLS dial incomplete')
  streams=dial.result();reader,writer=streams
  check()
  launch=await StageLauncher.create(config,reader,writer,fixed_tls,fixed_grants,store,descriptor,**opts)
  leaf();return launch
 except BaseException as primary:
  if launch is not None:
   try:
    close=launch._close_task
    if close is None or launch._close_caller is asyncio.current_task():await launch.close()
    elif close.done():close.result()
    # An existing external close owns its waiters; never join it via a new task.
   except BaseException as failure:primary=_with_cleanup(primary,failure)
  async def cleanup():
   nonlocal streams
   if not dial.done():dial.cancel()
   try:streams=await dial
   except asyncio.CancelledError as failure:
    if failure.__cause__ is not None or (failure.__context__ is not None and not isinstance(failure.__context__,asyncio.CancelledError)):raise
   except BaseException as failure:
    # Ignore only the same original dial error, never a late cleanup failure.
    if failure is not primary:raise
   if streams is not None:
    _,writer=streams
    if not getattr(writer,'_launcher_owned',False):
     failures=[]
     try:writer.close()
     except BaseException as error:failures.append(error)
     try:await asyncio.wait_for(writer.wait_closed(),10)
     except BaseException as error:failures.append(error)
     if failures:raise BaseExceptionGroup('unadopted dial stream cleanup unknown',failures)
  await _retire_failed(primary,cleanup)

async def dial_control_ready(launcher,raw,pin,tls,materials,*,enabled=False,identity=None,authorize=None,action_identity=None,clock=None,timeout=10):
 """Dial two dedicated purposes into existing installed control assembly.

 Trusted local configuration/providers only. No listener, dispatch, new grant,
 retry, or quota release. Caller retains the returned installed role's launcher.
 """
 import hashlib,re
 from .channel import _accept_unstarted
 from .role_startup import _close_channel
 from .private_one_token import _factories
 from private_experiment_actions import sample
 from private_experiment_topology import bind_control_ready
 _need(enabled is True and type(launcher)is StageLauncher)
 for provider in (identity,authorize,action_identity):_need(callable(getattr(provider,'hold',None)))
 _need(callable(getattr(clock,'sample',None)))
 _need(type(timeout)in(int,float) and math.isfinite(timeout) and 0<timeout<=10)
 _need(type(raw)is bytes and len(raw)<=16384 and hashlib.sha256(raw).hexdigest()==pin)
 d=_parse(raw);_need(set(d)=={'version','purpose','launcher_sha256','control','actions'} and type(d['version'])is int and d['version']==1 and d['purpose']=='role-control-dial')
 _need(type(tls)is dict and type(materials)is dict and set(tls)==set(materials)=={'control','actions'})
 launcher._control_dial_entry();launcher.check()
 _need(not getattr(launcher,'_control_dial_attempted',False) and asyncio.current_task() not in launcher._jobs)
 _need(launcher._installed_ready and launcher.operation is not None and not launcher._attach_attempted and not launcher._actions_attempted)
 c=launcher.config.data;_need(d['launcher_sha256']==launcher.config._sha)
 fixed_tls={};fixed_material={}
 for purpose,node in (('control',c['coordinator_node']),('actions',c['issuer_node'])):
  e=d[purpose];_need(type(e)is dict and set(e)=={'host','port','hostname','peer','certificate','material_sha256'})
  for k in ('host','hostname'):_need(type(e[k])is str and 0<len(e[k])<=253 and not any(x.isspace() or ord(x)<32 for x in e[k]))
  _need(type(e['port'])is int and 1<=e['port']<=65535 and e['peer']==node)
  _need(type(e['certificate'])is str and re.fullmatch('[a-f0-9]{64}',e['certificate']) is not None)
  if purpose=='actions':_need(e['certificate']==c['issuer_certificate'])
  t=tls[purpose];_need(type(t)is TLSConfig and t.peers=={node:e['certificate']})
  _need(type(materials[purpose])is bytes and hashlib.sha256(materials[purpose]).hexdigest()==e['material_sha256'])
  fixed_tls[purpose]=TLSConfig(t.certfile,t.keyfile,t.cafile,dict(t.peers));fixed_material[purpose]=materials[purpose]
 _need((d['control']['host'],d['control']['port'])!=(d['actions']['host'],d['actions']['port']))
 startup=launcher.startup;operation=launcher.operation;resources=startup.runtime['resources'];ticket=startup.runtime['ticket']
 session=startup.client._session;_need(session is not None and session.active)
 def transport_pin():return tuple((k,id(tls[k]),tls[k].certfile,tls[k].keyfile,tls[k].cafile,dict(tls[k].peers),materials[k]) for k in ('control','actions'))
 def provider_pin():
  providers=(identity,authorize,action_identity,clock,resources,resources.authority,resources.capacity,*startup.runtime.values())
  return tuple((id(p),name,getattr(p,name,None)) for p in providers for name in ('hold','sample','verify_stage','quarantine','hold_load','hold_stage0','hold_call','finalize','load','open_scope','require_open','materialize','finish_scope','require_finished'))
 transport=transport_pin();providers=provider_pin();runtime=dict(startup.runtime);spec=operation.spec;spec_digest=spec.digest
 def leaf():
  _need(transport_pin()==transport and provider_pin()==providers and launcher.startup is startup and launcher.operation is operation)
  _need(set(startup.runtime)==set(runtime) and all(startup.runtime[k] is v for k,v in runtime.items()))
  _need(operation.spec is spec and spec.digest==spec_digest and launcher.config._sha==d['launcher_sha256'])
  _need(all(getattr(operation,k) is runtime[k] for k in ('authority','resources','ticket','device','loader')))
  _need(operation.factory is (runtime['factories'] or _factories))
  _need(startup.client._session is session);session.check();launcher.check()
 def check():
  leaf();sample(clock);operation.ready()
  _need(resources.tickets.get(id(ticket)) is ticket and ticket.reservation_id==startup.descriptor['resource_id'])
  with resources.hold(ticket.reservation_id,startup.descriptor):pass
  leaf()
 check();contexts={}
 for purpose in ('control','actions'):
  ctx=load_context(fixed_tls[purpose],fixed_material[purpose],d[purpose]['material_sha256'])
  _need(type(ctx)is ssl.SSLContext and ctx.verify_mode==ssl.CERT_REQUIRED and ctx.check_hostname and ctx.minimum_version>=ssl.TLSVersion.TLSv1_2)
  contexts[purpose]=ctx
 check()
 caller=asyncio.current_task();launcher._control_dial_attempted=True;launcher._control_dial_pending=True;launcher._control_dial_task=caller
 launcher._jobs.add(caller);tasks=[];streams=[];action=None
 async def cleanup():
  errors=[]
  for task in tasks:
   if not task.done():task.cancel()
   try:
    stream=await task
    if not any(stream[1] is v[1] for v in streams):streams.append(stream)
   except asyncio.CancelledError as e:
    if e.__cause__ is not None or e.__context__ is not None:errors.append(e)
   except BaseException as e:
    if e is not primary:errors.append(e)
  if action is not None and not getattr(action.w,'_launcher_owned',False):
   try:await _close_channel(action)
   except BaseException as e:errors.append(e)
  for _,writer in streams:
   if getattr(writer,'_launcher_owned',False) or (action is not None and writer is action.w):continue
   try:writer.close()
   except BaseException as e:errors.append(e)
   try:await asyncio.wait_for(writer.wait_closed(),10)
   except BaseException as e:errors.append(e)
  if errors:raise BaseExceptionGroup('control dial cleanup unknown',errors)
 try:
  for purpose in ('control','actions'):
   check();e=d[purpose];ctx=contexts[purpose]
   _need(ctx.verify_mode==ssl.CERT_REQUIRED and ctx.check_hostname and ctx.minimum_version>=ssl.TLSVersion.TLSv1_2)
   task=asyncio.create_task(asyncio.open_connection(e['host'],e['port'],ssl=ctx,server_hostname=e['hostname']));tasks.append(task)
   done,_=await asyncio.wait((task,),timeout=timeout)
   if task not in done:raise TimeoutError('role control dial incomplete')
   stream=task.result();streams.append(stream);check();launcher._available_writer(stream[1])
   if len(streams)==2:_need(streams[0][1] is not stream[1] and streams[0][0] is not stream[0])
  grant=Grant(spec.request,spec.session,spec.epoch,operation.stage,(0,1),spec.model,2)
  ar,aw=streams[1]
  action=await _accept_unstarted(ar,aw,fixed_tls['actions'],{d['actions']['peer']:{grant}},capacity_bytes=32768,capacity_tiles=1)
  check();launcher._control_dial_entry()
  # No await between release of dial ownership and existing bind's first take.
  launcher._jobs.discard(caller)
  result=await bind_control_ready(launcher,*streams[0],fixed_tls['control'],{d['control']['peer']:{grant}},action,enabled=True,identity=identity,authorize=authorize,action_identity=action_identity,clock=clock,authority_node=c['issuer_node'],authority_certificate=c['issuer_certificate'])
  leaf();launcher._control_dial_pending=False;return result
 except BaseException as error:
  primary=error
  launcher._jobs.discard(caller)
  # Do not join an external close that captured this caller in its job snapshot.
  close=launcher._close_task
  try:
   if close is None or launcher._close_caller is caller:await launcher.close()
   elif close.done():close.result()
  except BaseException as failure:primary=_with_cleanup(primary,failure)
  await _retire_failed(primary,cleanup)
 finally:launcher._jobs.discard(caller)
