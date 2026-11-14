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

async def dial_stage(config,host,port,tls,grants,store,d,*,material=None,material_pin=None,server_hostname=None,enabled=False,timeout=10,identity=None,authorize=None,resources=None,completion=None,runtime=None):
 _need(enabled is True)
 _need(type(material)is bytes and type(material_pin)is str)
 for value in (host,server_hostname):_need(type(value)is str and 0<len(value)<=253 and not any(c.isspace() or ord(c)<32 for c in value))
 _need(type(port)is int and 1<=port<=65535 and type(timeout) in (int,float) and math.isfinite(timeout) and 0<timeout<=10)
 opts=dict(identity=identity,authorize=authorize,resources=resources,completion=completion,runtime=runtime)
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
