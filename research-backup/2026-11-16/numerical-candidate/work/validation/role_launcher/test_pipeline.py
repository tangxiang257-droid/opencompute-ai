"""Real launcher/role/storage code, ordinary bytes, LAB permissions and memory TLS."""
import asyncio,hashlib,unittest
from cluster_p1.role_launcher import LaunchConfig,StageLauncher,ReceivingLauncher
from cluster_p1.control_exchange import ControlClient
from cluster_p1.online_issuance_transport import _json
from cluster_p1.role_control import CoordinatorControl
from cluster_p1.channel import Channel,Grant
from validation.role_assembly.test_pipeline import Tests as Base
from validation.role_bridge import test_transport as wire
from validation.control_exchange.test_transport import Holds
from validation.split_roles.test_pipeline import as_role

def configuration(root,role,d,issuer='issuer',coordinator='controller'):
 raw=_json(dict(version=1,role=role,node=d['c_node'] if role=='C' else d['node'],issuer_node=issuer,issuer_certificate=hashlib.sha256(issuer.encode()).hexdigest(),coordinator_node=coordinator,descriptor_sha256=hashlib.sha256(_json(d)).hexdigest(),root=str(root),charge=d['charge'] if role=='C' else 2097152,control_budget=4194304))
 return LaunchConfig(raw,hashlib.sha256(raw).hexdigest())
class Tests(Base):
 async def asyncSetUp(self):await super().asyncSetUp();self.launchers={}
 async def create_startup(self,reader,writer,tls,grants,store,d,**opts):
  c=configuration(self.root,('A','B')[d['stage']],d)
  opts.pop('issuer_node');opts.pop('issuer_certificate')
  launch=await StageLauncher.create(c,reader,writer,tls,grants,store,d,**opts)
  self.launchers[d['stage']]=launch;self.addAsyncCleanup(launch.close)
  return launch.startup
 async def install_startup(self,startup,path,**opts):return await self.launchers[startup.stage].install(**opts)
 def launch_operation(self,stage,spec,**opts):return self.launchers[stage].build_operation(spec,self.policy,**opts)
 async def register(self,stage,spec,operation):
  launch=self.launchers[stage];self.assertIs(launch.operation,operation)
  node=('a','b')[stage];cr,cw,sr,sw=wire.streams('controller',node)
  grant=Grant(spec.request,spec.session,1,stage,(0,1),spec.model,2)
  client=Channel(cr,cw,wire.config(node),{node:{grant}},capacity_bytes=32768,capacity_tiles=1)
  self.channels.append(client);await client.start()
  role=await launch.attach(sr,sw,wire.config('controller'),{'controller':{grant}},identity=Holds(),authorize=Holds())
  control=CoordinatorControl(ControlClient(client),role.descriptor);self.controls.append(control)
  self.assertEqual(sum(f[:1]==b'H' for f in sw.frames),1)
  return role,control,role.journal
 async def create_receiver(self,channel,store,d,**opts):
  opts.pop('issuer_node');opts.pop('issuer_certificate')
  c=configuration(self.root,'C',d,issuer='authority')
  self.receiving_launcher=await ReceivingLauncher.create(c,channel,store,d,**opts)
  self.addAsyncCleanup(self.receiving_launcher.close);return self.receiving_launcher.role
 async def dispatch(self,role,coordinator):
  launch=self.launchers[role.startup.stage]
  async def serve():
   await launch.serve_once();await launch.serve_once()
  tasks=[asyncio.create_task(as_role(role.descriptor.role_node,serve())),asyncio.create_task(coordinator.dispatch())]
  try:return (await asyncio.gather(*tasks))[1]
  finally:
   for t in tasks:
    if not t.done():t.cancel()
   await asyncio.gather(*tasks,return_exceptions=True)
 async def test_actual_launchers_full_chain(self):
  await self.online_bootstrap();await self.run_split()
  self.assertEqual(self.receiving_launcher.role.store.history(self.spec.owner,self.spec.request)['state'],'RECEIVED')
  for stage,launch in self.launchers.items():
   self.assertIs(launch.operation,self.operations[stage]);self.assertIs(launch.startup.runtime['resources'],launch.operation.resources)
 async def test_close_waiting_install_joins_before_db_close(self):
  entered=asyncio.Event();release=asyncio.Event()
  async def barrier():entered.set();await release.wait()
  self.prepare_fence=barrier
  task=asyncio.create_task(self.online_bootstrap())
  try:
   await asyncio.wait_for(entered.wait(),5);launch=self.launchers[1]
   self.assertFalse(launch._installed_ready)
   with self.assertRaises(Exception):launch.build_operation(self.spec,self.policy,spool=None)
   await asyncio.wait_for(launch.close(),5)
   self.assertTrue(task.done());self.assertTrue(launch.startup.channel._task.done())
   self.assertEqual(self.role_loaders[1].calls,[])
   self.assertTrue(launch.config.path('claim').exists())
  finally:
   release.set()
   if not task.done():task.cancel()
   await asyncio.gather(task,return_exceptions=True)
 async def test_missing_runtime_device_before_claim(self):
  old=self.kwargs;self.kwargs=lambda stage:dict(old(stage),device=None)
  with self.assertRaises(Exception):await self.online_bootstrap()
  self.assertFalse((self.root/'launch-A-claim.sqlite').exists());self.assertEqual(self.role_loaders[0].calls,[])
 async def test_default_missing_control_authority_rejected(self):
  await self.online_bootstrap();launch=self.launchers[0]
  import struct
  launch.build_operation(self.spec,self.policy,tokens=struct.pack('<q',1))
  with self.assertRaises(Exception):await launch.attach(None,None,None,None)
  self.assertFalse(launch.config.path('control').exists());self.assertEqual(self.role_loaders[0].calls,[])
 async def test_provider_swap_permanently_seals_launcher(self):
  await self.online_bootstrap();launch=self.launchers[0];client=launch.startup.client;old=client.authorize
  client.authorize=Holds()
  with self.assertRaises(Exception):launch.check()
  client.authorize=old
  with self.assertRaises(Exception):launch.check()
  await launch.close();self.assertEqual(self.role_loaders[0].calls,[])
 async def test_original_install_connection_EOF_closes_without_caller_ordering(self):
  await self.online_bootstrap();launch=self.launchers[0]
  launch.startup.channel.r.feed_eof()
  await asyncio.gather(launch.startup.channel._task,return_exceptions=True)
  await launch.close()
  self.assertTrue(launch._closing);self.assertTrue(launch._close_task.done())
  with self.assertRaises(Exception):launch.check()
  self.assertEqual(self.role_loaders[0].calls,[])
 async def test_close_does_not_hide_child_cleanup_failure(self):
  await self.online_bootstrap();launch=self.launchers[0];entered=asyncio.Event()
  async def phase():
   entered.set()
   try:await asyncio.Event().wait()
   except asyncio.CancelledError:raise OSError('child cleanup failed')
  child=asyncio.create_task(launch._run(phase));await entered.wait()
  with self.assertRaises(BaseExceptionGroup):await asyncio.wait_for(launch.close(),5)
  self.assertTrue(child.done());self.assertTrue(launch.startup.channel._task.done())
  # Explicitly consume expected retained terminal failure for registered cleanup.
  old=launch.close
  async def expected_close():
   try:await old()
   except BaseExceptionGroup:pass
  launch.close=expected_close
  # addAsyncCleanup captured old bound method: replace only that registered call.
  self._cleanups=[(expected_close,a,k) if getattr(f,'__self__',None) is launch else (f,a,k) for f,a,k in self._cleanups]
 async def test_close_running_stage_waits_for_worker_exit(self):
  import threading,struct
  from unittest.mock import patch
  from validation.one_token.test_assembly import SyntheticBackend
  await self.online_bootstrap();launch=self.launchers[0]
  op=launch.build_operation(self.spec,self.policy,tokens=struct.pack('<q',1));role,control,journal=await self.register(0,self.spec,op)
  entered=asyncio.Event();retiring=asyncio.Event();release=threading.Event();exited=threading.Event();loop=asyncio.get_running_loop()
  old=SyntheticBackend.operation
  def body(backend,*args,**kw):
   loop.call_soon_threadsafe(entered.set)
   try:
    if not release.wait(5):raise AssertionError('release barrier timeout')
    return old(backend,*args,**kw)
   finally:exited.set()
  from cluster_p1.stage_ownership import StageOwner
  retire=StageOwner._retire
  def retired(owner):
   retire(owner);retiring.set()
  closed=[];original=journal.close
  def closed_journal():self.assertTrue(exited.is_set());closed.append(True);return original()
  task=closing=None
  with patch.object(SyntheticBackend,'operation',body),patch.object(StageOwner,'_retire',retired),patch.object(journal,'close',closed_journal):
   try:
    task=asyncio.create_task(self.dispatch(role,control));await asyncio.wait_for(entered.wait(),5)
    closing=asyncio.create_task(launch.close());await asyncio.wait_for(retiring.wait(),5)
    self.assertFalse(exited.is_set());self.assertEqual(closed,[])
    closing.cancel();release.set()
    result=(await asyncio.gather(closing,return_exceptions=True))[0]
    self.assertIsInstance(result,asyncio.CancelledError);self.assertTrue(exited.is_set());self.assertTrue(closed)
   finally:
    release.set()
    if task is not None:
     if not task.done():task.cancel()
     await asyncio.gather(task,return_exceptions=True)
    if closing is not None:await asyncio.gather(closing,return_exceptions=True)
 async def test_same_C_store_remote_history_never_grants_work(self):
  from cluster_p1.c_receiving_history import ReceivingHistoryClient
  from validation.c_receiving.test_protocol import Identity,Policy
  await self.online_bootstrap();await self.run_split();launch=self.receiving_launcher;d=launch.role.d
  ar,aw,cr,cw=wire.streams('auditor','c');g=Grant(d['task'],d['session'],1,1,(0,1),d['model'],2)
  client=Channel(ar,aw,wire.config('c'),{'c':{g}},capacity_bytes=32768,capacity_tiles=1)
  server=Channel(cr,cw,wire.config('auditor'),{'auditor':{g}},capacity_bytes=32768,capacity_tiles=1)
  self.channels.extend((client,server));await client.start()
  service=await launch.attach_history(server,identity=Identity(),authorize=Policy())
  history=ReceivingHistoryClient(client);before=launch.role.store.history(d['owner'],d['task'])
  _,reply=await asyncio.gather(launch.query_once(),history.query(d))
  self.assertEqual(reply['state'],'RECEIVED');self.assertIs(reply['receiving_allowed'],False)
  self.assertIs(reply['execution_allowed'],False);self.assertEqual(launch.role.store.history(d['owner'],d['task']),before)
  self.assertEqual(service._history.execute('PRAGMA query_only').fetchone(),(1,))
 async def test_existing_gate_rejects_foreign_channel_without_closing_it(self):
  await self.online_bootstrap();await self.run_split();launch=self.launchers[1]
  # A foreign known-control channel is never adopted or closed by gate attach.
  foreign=self.launchers[0].control
  with self.assertRaises(Exception):launch.attach_gate(foreign,self.receiving_launcher.role.d)
  with foreign.identity_fence(foreign.authenticated_identity):pass
  self.assertEqual(self.role_loaders[1].calls,[1])
 async def test_attach_H_drain_barrier_denies_early_serve(self):
  import struct
  await self.online_bootstrap();launch=self.launchers[0];launch.build_operation(self.spec,self.policy,tokens=struct.pack('<q',1))
  cr,cw,sr,sw=wire.streams('controller','a');s=self.spec;g=Grant(s.request,s.session,1,0,(0,1),s.model,2)
  client=Channel(cr,cw,wire.config('a'),{'a':{g}},capacity_bytes=32768,capacity_tiles=1);self.channels.append(client);await client.start()
  entered=asyncio.Event();release=asyncio.Event();drain=sw.drain
  async def blocked():
   if sw.last_kind==b'H':entered.set();await release.wait()
   await drain()
  sw.drain=blocked;task=asyncio.create_task(launch.attach(sr,sw,wire.config('controller'),{'controller':{g}},identity=Holds(),authorize=Holds()))
  try:
   await asyncio.wait_for(entered.wait(),5);self.assertIsNotNone(launch.installed)
   self.assertFalse(launch._attached_ready)
   with self.assertRaises(Exception):await launch.serve_once()
   self.assertEqual(self.role_loaders[0].calls,[])
   release.set();await task;self.assertTrue(launch._attached_ready)
  finally:
   release.set()
   if not task.done():task.cancel()
   await asyncio.gather(task,return_exceptions=True)
if __name__=='__main__':
 r=unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(Tests(n) for n in Tests.__dict__ if n.startswith('test_')));raise SystemExit(not r.wasSuccessful())
