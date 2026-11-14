"""Private role-local lifecycle assembly over supplied authenticated connections.

No CLI, listener, connector, dynamic imports, credential issuance or default
providers. A digest supplied by trusted LOCAL bootstrap pins configuration; the
peer cannot choose this digest. Provider method checks are not proof of trust.
The bootstrap must supply genuine identity/task/resource/completion services.
Synthetic test providers establish only mechanics, never deployment permission.
"""
import asyncio,hashlib,os,re,stat
from pathlib import Path
from .control_exchange import _need
from .online_issuance_transport import _json,_parse
from .role_startup import RoleStartup,_methods,_close_channel
from .role_assembly import InstalledStageRole
from .role_resources import RoleResources
from .online_issuance import RoleInstallStore
from .local_stage_operations import StageAOperation,StageBOperation
from .channel import Channel,_accept_unstarted
from .c_receiving import ReceivingRole,ReceivingStore,descriptor
from .c_dispatch_gate import BDispatchGate,CDispatchGate
from .delivery_receipt import _with_cleanup

def launch_document(raw,expected_sha256):
 """Pure bounded public schema check; no path, provider or host validation."""
 _need(type(raw)is bytes and type(expected_sha256)is str and re.fullmatch('[a-f0-9]{64}',expected_sha256) is not None)
 _need(hashlib.sha256(raw).hexdigest()==expected_sha256)
 d=_parse(raw)
 _need(type(d)is dict and set(d)=={'version','role','node','issuer_node','issuer_certificate','coordinator_node','descriptor_sha256','root','charge','control_budget'})
 _need(type(d['version'])is int and d['version']==1 and d['role'] in ('A','B','C'))
 for k in ('node','issuer_node','coordinator_node'):_need(type(d[k])is str and re.fullmatch('[A-Za-z0-9][A-Za-z0-9._:-]{0,127}',d[k]) is not None)
 for k in ('issuer_certificate','descriptor_sha256'):_need(type(d[k])is str and re.fullmatch('[a-f0-9]{64}',d[k]) is not None)
 _need(d['node']!=d['issuer_node'] and d['node']!=d['coordinator_node'])
 _need(type(d['charge'])is int and type(d['control_budget'])is int and 0<d['charge']<=d['control_budget']<2**63)
 _need(type(d['root'])is str and len(d['root'])<=4096)
 return d

class LaunchConfig:
 """Inert canonical bounded config. No provider names or executable hooks."""
 def __init__(self,raw,expected_sha256):
  d=launch_document(raw,expected_sha256)
  root=Path(d['root']);_need(root.is_absolute() and root==root.resolve(strict=True))
  info=root.lstat();_need(stat.S_ISDIR(info.st_mode))
  self._raw=raw;self._sha=expected_sha256;self.root=root;self._inode=(info.st_dev,info.st_ino);self.pid=os.getpid()
 @property
 def data(self):return _parse(self._raw)
 def check(self):
  _need(os.getpid()==self.pid and hashlib.sha256(self._raw).hexdigest()==self._sha)
  s=self.root.lstat();_need(stat.S_ISDIR(s.st_mode) and (s.st_dev,s.st_ino)==self._inode and self.root.resolve(strict=True)==self.root)
 def bound(self,d):
  self.check();c=self.data;_need(hashlib.sha256(_json(d)).hexdigest()==c['descriptor_sha256'])
  _need(d['c_node']==c['node'] and d['charge']==c['charge'] if c['role']=='C' else d['node']==c['node'] and d['stage']==('A','B').index(c['role']))
 def path(self,name):
  self.check();_need(name in ('claim','journal','control','driver'))
  return self.root/('launch-'+self.data['role']+'-'+name+'.sqlite')
 def claim(self):
  self.check();fd=os.open(self.path('claim'),os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
  try:
   data=self._raw;offset=0
   while offset<len(data):
    n=os.write(fd,data[offset:]);_need(n>0);offset+=n
   os.fsync(fd)
  finally:os.close(fd)
  fd=os.open(self.root,os.O_RDONLY|os.O_DIRECTORY)
  try:os.fsync(fd)
  finally:os.close(fd)
  # Permanent local occupancy, not authority. Never delete or resume this marker.
  self.check()

class _Owner:
 def _init_owner(self,config):
  self.config=config;self._loop=asyncio.get_running_loop();self._closing=False;self._close_task=None;self._jobs=set();self._watches=[];self._extra_channels=[];self._raw_writers=[]
 def _seal(self):
  self._closing=True
  if self._close_task is None:
   task=asyncio.create_task(self.close())
   task.add_done_callback(lambda t:t.exception() if not t.cancelled() else None)
 def _live(self):
  _need(not self._closing and asyncio.get_running_loop() is self._loop)
  try:self.config.check()
  except BaseException:self._seal();raise
 async def _run(self,action):
  self._live();task=asyncio.current_task();_need(task not in self._jobs);self._jobs.add(task)
  try:
   result=await action();self._live();return result
  except BaseException as error:
   try:await self.close()
   except BaseException as cleanup:raise _with_cleanup(error,cleanup)
   raise
  finally:self._jobs.discard(task)
 def _available_writer(self,writer):
  _need(writer is not None and not getattr(writer,'_launcher_owned',False))
  known=list(self._extra_channels)
  for name in ('channel','control'):
   c=getattr(self,name,None)
   if c is not None:known.append(c)
  startup=getattr(self,'startup',None)
  if startup is not None:
   known.append(startup.channel)
   if startup.prepared is not None:known.append(startup.prepared.channel)
  _need(all(writer is not c.w for c in known) and all(writer is not w for w in self._raw_writers))
 def _take_channel(self,channel):
  _need(type(channel)is Channel and not channel._start_attempted)
  _need(not any(getattr(channel,k,False) for k in ('_online_issuance_claimed','_receipt_state','_installed_role_control_claimed','_dispatch_gate_claimed','_c_receiving_claimed','_launcher_owned')))
  self._available_writer(channel.w)
  channel._launcher_owned=True;channel.w._launcher_owned=True;self._extra_channels.append(channel)
 def _watch(self,channel):
  def ended(_):
   self._seal()
  channel._task.add_done_callback(ended);self._watches.append((channel._task,ended))
  if channel._task.done():ended(channel._task)
 async def close(self):
  caller=asyncio.current_task();self._closing=True
  if self._close_task is None:
   async def cleanup():
    for reader,callback in self._watches:reader.remove_done_callback(callback)
    tasks=[t for t in self._jobs if t is not caller and not t.done()]
    for t in tasks:t.cancel()
    # _run error paths must not join this same cleanup task recursively.
    errors=[]
    if tasks:
     results=await asyncio.gather(*tasks,return_exceptions=True)
     errors.extend(x for x in results if isinstance(x,BaseException) and not isinstance(x,asyncio.CancelledError))
    try:await self._close_owned()
    except BaseException as error:errors.append(error)
    for channel in self._extra_channels:
     try:await _close_channel(channel)
     except BaseException as error:errors.append(error)
    for writer in self._raw_writers:
     try:writer.close()
     except BaseException as error:errors.append(error)
     try:await asyncio.wait_for(writer.wait_closed(),10)
     except BaseException as error:errors.append(error)
    if errors:raise BaseExceptionGroup('launcher owned cleanup unknown',errors)
   self._close_caller=caller;self._close_task=asyncio.create_task(cleanup())
  if caller in self._jobs and caller is not self._close_caller:return
  cancelled=None
  while not self._close_task.done():
   try:await asyncio.shield(self._close_task)
   except asyncio.CancelledError as error:cancelled=error
   except BaseException:break
  try:self._close_task.result()
  except BaseException as error:
   if cancelled is not None:raise _with_cleanup(cancelled,error)
   raise
  if cancelled is not None:raise cancelled

def _stage_preflight(config,store,d,*,identity=None,authorize=None,resources=None,completion=None,runtime=None):
 _need(type(config)is LaunchConfig and config.data['role'] in ('A','B'))
 config.bound(d);c=config.data
 _need(not os.path.lexists(config.path('claim')))
 _need(type(store)is RoleInstallStore and type(resources)is RoleResources and store.resources is resources)
 _need(type(runtime)is dict and set(runtime)=={'instance','layer_range','authority','resources','ticket','device','loader','factories'})
 _need(runtime['resources'] is resources and runtime['instance']==d['instance'] and runtime['ticket'] is not None)
 for provider in (identity,authorize):_methods(provider,('hold',))
 _methods(completion,('check','begin','finish','quarantine'))
 _methods(runtime['authority'],('hold_load',)+(('hold_stage0',) if c['role']=='A' else ('hold_call','finalize')))
 _methods(runtime['device'],('hold_load','open_scope','require_open','materialize','finish_scope','require_finished','quarantine'))
 _methods(runtime['loader'],('load','quarantine'));store.ready();resources.ready()
 _methods(resources,('hold','hold_control','verify_stage','quarantine'))
 stage=d['declaration']['stages'][d['stage']]
 _need(type(runtime['instance'])is str and type(runtime['layer_range'])is tuple and runtime['layer_range']==(stage['layer_start'],stage['layer_end']))
 _need(runtime['factories'] is None or callable(runtime['factories']))
 return c

class StageLauncher(_Owner):
 def __init__(self):raise TypeError('use await create')
 @classmethod
 async def create(cls,config,reader,writer,tls,grants,store,d,*,identity=None,authorize=None,resources=None,completion=None,runtime=None):
  _need(cls is StageLauncher)
  c=_stage_preflight(config,store,d,identity=identity,authorize=authorize,resources=resources,completion=completion,runtime=runtime)
  _need(not getattr(writer,'_launcher_owned',False))
  config.claim();self=object.__new__(cls);self._init_owner(config);writer._launcher_owned=True
  self.startup=None;self.installed=None;self.operation=None;self.control=None;self.gate=None
  self.actions=None;self._actions_pin=None;self._actions_attempted=False;self._actions_ready=False
  self._install_attempted=False;self._installed_ready=False;self._attached_ready=False;self._attach_attempted=False;self._operation_attempted=False
  self._providers=(identity,authorize,resources,completion);self._runtime=dict(runtime)
  async def create():
   self.startup=await RoleStartup.create(reader,writer,tls,grants,store,d,issuer_node=c['issuer_node'],issuer_certificate=c['issuer_certificate'],identity=identity,authorize=authorize,resources=resources,completion=completion,runtime=runtime)
   self._watch(self.startup.channel);return self
  return await self._run(create)
 def check(self):
  try:
   self._live();_need(self.startup is not None);self.config.bound(self.startup.descriptor);self.startup._preflight()
   client=self.startup.client
   _need(all(a is b for a,b in zip(self._providers,(client.identity,client.authorize,client.resources,client._completion))))
   _need(set(self.startup.runtime)==set(self._runtime) and all(self.startup.runtime[k] is v for k,v in self._runtime.items()))
   if self._actions_ready:
    _need(self.actions is self._actions_pin and self.actions._installed is self.installed)
    self.actions.check()
  except BaseException:self._seal();raise
 async def install(self,*,execution_authorize=None,prepared=None,prepare_fence=None,session_policy=None):
  self.check();_need(not self._install_attempted);self._install_attempted=True
  async def install():
   result=await self.startup.install(self.config.path('journal'),execution_authorize=execution_authorize,prepared=prepared,prepare_fence=prepare_fence,session_policy=session_policy)
   self.check();self._installed_ready=True;return result
  return await self._run(install)
 def build_operation(self,spec,policy,*,tokens=None,spool=None):
  self.check();_need(self._installed_ready and self.startup.journal is not None and not self._operation_attempted);self._operation_attempted=True
  try:
   if self.config.data['role']=='A':
    _need(spool is None);self.operation=StageAOperation(spec,policy,self.startup.journal,tokens,**self._runtime)
   else:
    _need(tokens is None);self.operation=StageBOperation(spec,policy,self.startup.journal,spool,journal_path=self.config.path('driver'),**self._runtime)
    self.operation.received_bytes() # Verify actual committed input, never config-supplied B digest.
   return self.operation
  except BaseException:
   self._seal();raise
 async def attach(self,reader,writer,tls,grants,*,identity=None,authorize=None):
  self.check();_need(self.operation is not None and not self._attach_attempted)
  for p in (identity,authorize):_methods(p,('hold',))
  self._available_writer(writer)
  self._attach_attempted=True;writer._launcher_owned=True
  async def attach():
   self.control=await _accept_unstarted(reader,writer,tls,grants,capacity_bytes=32768,capacity_tiles=1)
   if getattr(self,'_control_assembly_pending',False):self.control._descriptor_bootstrap_pending=True
   c=self.config.data
   self.installed=await InstalledStageRole.attach(self.startup,self.operation,self.control,self.config.path('control'),coordinator_node=c['coordinator_node'],charge=c['charge'],control_budget=c['control_budget'],identity=identity,authorize=authorize)
   self._watch(self.control);await self.control.start();self.check();self._attached_ready=True;return self.installed
  return await self._run(attach)
 def attach_gate(self,channel,d,*,identity=None,authorize=None):
  self.check();_need(self._attached_ready and self.installed is not None and self.gate is None)
  for p in (identity,authorize):_methods(p,('hold',))
  self._take_channel(channel)
  try:self.gate=BDispatchGate(channel,self.installed,d,identity=identity,authorize=authorize);return self.gate
  except BaseException:self._seal();raise
 async def attach_actions(self,channel,*,authority_node=None,authority_certificate=None,identity=None,clock=None):
  """Own a dedicated online action channel for this actual installed role.

  Does not create identity, execution/resource authority or device providers.
  Missing/failed attachment permanently seals this launcher, with no retry.
  """
  _need(not getattr(self,'_control_assembly_pending',False) or asyncio.current_task() is self._control_assembly_task)
  try:
   self.check();_need(self._attached_ready and self.installed is not None and not self._actions_attempted)
   self._take_channel(channel)
   return await self._attach_taken_actions(channel,authority_node=authority_node,authority_certificate=authority_certificate,identity=identity,clock=clock)
  except BaseException:self._seal();raise
 async def _attach_taken_actions(self,channel,*,authority_node=None,authority_certificate=None,identity=None,clock=None):
  """Finish only a transport already exclusively taken by this launcher."""
  from private_experiment_actions import ActionGate
  _need(not getattr(self,'_control_assembly_pending',False) or asyncio.current_task() is self._control_assembly_task)
  try:
   self.check();_need(self._attached_ready and self.installed is not None and not self._actions_attempted)
   _need(any(channel is owned for owned in self._extra_channels) and not channel._start_attempted)
   self._actions_attempted=True
   self.actions=ActionGate(channel,authority_node=authority_node,authority_certificate=authority_certificate,identity=identity,clock=clock)
   self._actions_pin=self.actions;self.actions.attach_installed(self.installed)
   async def attach():
    await channel.start();self._watch(channel);self.actions.check();self._actions_ready=True;self.check();return self.actions
   return await self._run(attach)
  except BaseException:self._seal();raise
 async def serve_once(self):
  _need(not getattr(self,'_control_assembly_pending',False))
  self.check();_need(self._attached_ready and self.installed is not None)
  _need(not self._actions_attempted or self._actions_ready)
  return await self._run(self.installed.serve_once)
 async def _close_owned(self):
  if self.installed is not None:await self.installed.close()
  else:
   errors=[]
   if self.operation is not None:
    try:
     self.operation.quarantine()
     if type(self.operation)is StageBOperation:self.operation.close()
    except BaseException as e:errors.append(e)
   if self.control is not None:
    try:await _close_channel(self.control)
    except BaseException as e:errors.append(e)
   if self.startup is not None:
    try:await self.startup.close()
    except BaseException as e:errors.append(e)
   if errors:raise BaseExceptionGroup('launcher cleanup unknown',errors)

class ReceivingLauncher(_Owner):
 def __init__(self):raise TypeError('use await create')
 @classmethod
 async def create(cls,config,channel,store,d,*,identity=None,authorize=None,maintenance=None,resources=None):
  _need(cls is ReceivingLauncher and type(config)is LaunchConfig and config.data['role']=='C');config.bound(descriptor(d))
  _need(type(store)is ReceivingStore and store.authority is None);store._check(True)
  for p in (identity,authorize,maintenance,resources):_methods(p,('hold',))
  _need(type(channel)is Channel and not channel._start_attempted and not any(getattr(channel,k,False) for k in ('_online_issuance_claimed','_receipt_state','_installed_role_control_claimed','_dispatch_gate_claimed','_c_receiving_claimed','_launcher_owned')))
  _need(not getattr(channel.w,'_launcher_owned',False))
  config.claim();self=object.__new__(cls);self._init_owner(config);c=config.data;channel.w._launcher_owned=True
  self.role=None;self.channel=channel;self.gate=None;self.history=None;self._history_ready=False
  self._providers=(identity,authorize,maintenance,resources)
  async def create():
   self.role=ReceivingRole(channel,store,d,issuer_node=c['issuer_node'],issuer_certificate=c['issuer_certificate'],identity=identity,authorize=authorize,maintenance=maintenance,resources=resources)
   self._watch(channel);return self
  return await self._run(create)
 def check(self):
  try:
   self._live();self.config.bound(self.role.d)
   _need(all(a is b for a,b in zip(self._providers,(self.role.identity,self.role.authorize,self.role.maintenance,self.role.resources))))
  except BaseException:self._seal();raise
 def attach_result(self,spec,reader,writer,tls,metadata,binding,*,identity=None,authorize=None):
  self.check();_need(not self.role.claimed)
  for p in (identity,authorize):_methods(p,('hold',))
  _need(type(metadata)is Channel and writer is not metadata.w);self._available_writer(writer)
  self._take_channel(metadata);writer._launcher_owned=True;self._raw_writers.append(writer)
  try:return self.role.attach_result(spec,reader,writer,tls,metadata,binding,identity=identity,authorize=authorize)
  except BaseException:self._seal();raise
 def attach_gate(self,channel,*,identity=None,authorize=None):
  self.check();_need(self.gate is None)
  for p in (identity,authorize):_methods(p,('hold',))
  self._take_channel(channel)
  try:self.gate=CDispatchGate(channel,self.role,identity=identity,authorize=authorize);return self.gate
  except BaseException:self._seal();raise
 async def attach_history(self,channel,*,identity=None,authorize=None):
  self.check();_need(self.history is None)
  for p in (identity,authorize):_methods(p,('hold',))
  self._take_channel(channel)
  async def attach():
   from .c_receiving_history import ReceivingHistoryService
   self.history=ReceivingHistoryService(channel,self.role.store,identity=identity,authorize=authorize)
   await channel.start();self.check();self._history_ready=True;return self.history
  return await self._run(attach)
 async def query_once(self):
  self.check();_need(self._history_ready and self.history is not None)
  return await self._run(self.history.serve_once)
 async def serve_once(self):self.check();return await self._run(self.role.serve_once)
 async def serve_gate(self):
  self.check();_need(self.gate is not None);return await self._run(self.gate.serve_once)
 async def receive_result(self,*a,**kw):self.check();return await self._run(lambda:self.role.receive_result(*a,**kw))
 async def _close_owned(self):
  errors=[]
  if self.history is not None:
   try:await self.history.close()
   except BaseException as error:errors.append(error)
  try:
   if self.role is not None:await self.role.close()
   else:await _close_channel(self.channel)
  except BaseException as error:errors.append(error)
  if errors:raise BaseExceptionGroup('C launcher cleanup unknown',errors)
