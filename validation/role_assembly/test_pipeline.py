"""Existing full memory-channel/SQLite chain, synthetic authorities and compute."""
import asyncio,unittest
from validation.authority_resources import test_pipeline as fixture
from validation.split_roles import test_pipeline as split
from validation.role_bridge import test_transport as wire
from validation.control_exchange.test_transport import Holds
from cluster_p1.channel import Channel,Grant
from cluster_p1.control_exchange import ControlClient
from cluster_p1.role_control import CoordinatorControl
try:from cluster_p1.role_assembly import InstalledStageRole
except ImportError:InstalledStageRole=None
class Tests(fixture.Tests):
 def kwargs(self,stage):
  if not hasattr(self,'_assembly_kwargs'):self._assembly_kwargs={}
  if stage not in self._assembly_kwargs:self._assembly_kwargs[stage]=super().kwargs(stage)
  return dict(self._assembly_kwargs[stage])
 async def online_bootstrap(self):
  self.assertIsNotNone(InstalledStageRole,'missing installed-role assembly')
  await super().online_bootstrap()
 async def register(self,stage,spec,operation):
  from cluster_p1.role_startup import RoleStartup
  startup=next(x for x in self.endpoints if type(x)is RoleStartup and x.stage==stage)
  node=('a','b')[stage];cr,cw,sr,sw=wire.streams('controller',node)
  grant=Grant(spec.request,spec.session,1,stage,(0,1),spec.model,2)
  client=Channel(cr,cw,wire.config(node),{node:{grant}},capacity_bytes=32768,capacity_tiles=1)
  server=Channel(sr,sw,wire.config('controller'),{'controller':{grant}},capacity_bytes=32768,capacity_tiles=1)
  self.channels.extend((client,server));await asyncio.gather(client.start(),server.start())
  assembled=await InstalledStageRole.attach(startup,operation,server,self.root/f'assembled-{stage}.db',coordinator_node='controller',charge=2097152,control_budget=4194304,identity=getattr(self,'assembly_identity',Holds()),authorize=getattr(self,'assembly_authorize',Holds()))
  self.endpoints.append(assembled);self.addAsyncCleanup(assembled.close)
  self.assertIs(assembled.journal.resources,startup.store.resources)
  self.assertIs(assembled.operation,operation)
  coordinator=CoordinatorControl(ControlClient(client),assembled.descriptor);self.controls.append(coordinator)
  return assembled,coordinator,assembled.journal
 async def prepare_a(self):
  from cluster_p1.local_stage_operations import StageAOperation
  import struct
  await self.online_bootstrap()
  op=StageAOperation(self.spec,self.policy,self.sender,struct.pack('<q',1),**self.kwargs(0));self.operations.append(op)
  return op
 async def test_identity_present_task_permission_denied(self):
  from contextlib import contextmanager
  class Reject:
   @contextmanager
   def hold(self,*args):raise PermissionError('no task permission');yield
  op=await self.prepare_a();self.assembly_authorize=Reject()
  with self.assertRaises(Exception):await self.register(0,self.spec,op)
  self.assertEqual(self.role_loaders[0].calls,[])
  self.assertEqual(self.role_resources[0].query('reservation-a')['state'],'UNKNOWN')
 async def test_missing_identity_denied(self):
  op=await self.prepare_a();self.assembly_identity=None
  with self.assertRaises(Exception):await self.register(0,self.spec,op)
  self.assertEqual(self.role_calls[0],[])
 async def test_same_startup_cannot_attach_twice(self):
  op=await self.prepare_a();role,_,_=await self.register(0,self.spec,op)
  with self.assertRaises(Exception):
   await InstalledStageRole.attach(role.startup,op,role.channel,self.root/'second.db',coordinator_node='controller',charge=2097152,control_budget=4194304,identity=Holds(),authorize=Holds())
  self.assertFalse((self.root/'second.db').exists());self.assertEqual(self.role_loaders[0].calls,[])
 async def test_provider_swap_rejects_and_restoration_does_not_revive(self):
  op=await self.prepare_a();role,_,_=await self.register(0,self.spec,op)
  old=role.startup.runtime['authority'];role.startup.runtime['authority']=object()
  with self.assertRaises(Exception):await role.serve_once()
  role.startup.runtime['authority']=old
  with self.assertRaises(Exception):await role.serve_once()
  self.assertEqual(self.role_loaders[0].calls,[])
 async def test_close_waiting_control_drains_before_journal(self):
  op=await self.prepare_a();role,_,_=await self.register(0,self.spec,op)
  entered=asyncio.Event();original=role.role.serve_once
  async def waiting(instance):entered.set();await original()
  from unittest.mock import patch
  with patch.object(type(role.role),'serve_once',waiting):
   pending=asyncio.create_task(role.serve_once());await entered.wait()
   await role.close();self.assertTrue(pending.done());self.assertTrue(role.channel._task.done())
  self.assertEqual(self.role_loaders[0].calls,[])
 async def test_foreign_operation_is_not_quarantined(self):
  op=await self.prepare_a()
  from cluster_p1.role_startup import RoleStartup
  startup=next(x for x in self.endpoints if type(x)is RoleStartup and x.stage==0)
  foreign=object();op.resources=foreign
  with self.assertRaises(Exception):await self.register(0,self.spec,op)
  self.assertFalse(op.retired);self.assertTrue(startup._installed_role_claimed)
 async def test_device_failure_stays_unknown_no_second_body(self):
  op=await self.prepare_a();role,coordinator,journal=await self.register(0,self.spec,op)
  self.role_devices[0].fail_stage=0
  with self.assertRaises(Exception):await self.dispatch(role,coordinator)
  before=list(self.role_calls[0])
  with self.assertRaises(Exception):await role.serve_once()
  self.assertEqual(self.role_calls[0],before);self.assertEqual(sum(x[0]=='stage0' for x in before),1)
  self.assertEqual(self.role_resources[0].query('reservation-a')['state'],'UNKNOWN')
 async def test_B_wrong_committed_input_refuses_before_load(self):
  from dataclasses import replace
  original=self.register
  async def wrong(stage,spec,operation):
   if stage==1:operation.spec=replace(operation.spec,inputs=('e'*64,))
   return await original(stage,spec,operation)
  self.register=wrong
  await self.online_bootstrap()
  with self.assertRaises(Exception):await self.run_split()
  self.assertEqual(self.role_loaders[1].calls,[])
  self.assertEqual(self.role_resources[1].db.execute('SELECT count(*) FROM controls').fetchone(),(0,))
 async def test_close_active_worker_joins_before_journals_despite_repeated_cancel(self):
  import threading
  from unittest.mock import patch
  from validation.one_token.test_assembly import SyntheticBackend
  from cluster_p1.stage_ownership import StageOwner
  op=await self.prepare_a();role,coordinator,journal=await self.register(0,self.spec,op)
  loop=asyncio.get_running_loop();entered=asyncio.Event();retiring=asyncio.Event()
  release=threading.Event();exited=threading.Event();closed=[]
  original_body=SyntheticBackend.operation;original_retire=StageOwner._retire
  original_close=journal.close;original_startup_close=role.startup.close
  def body(backend,*args,**kwargs):
   loop.call_soon_threadsafe(entered.set)
   try:
    if not release.wait(5):raise AssertionError('worker release barrier timed out')
    return original_body(backend,*args,**kwargs)
   finally:exited.set()
  def retire(owner):
   original_retire(owner)
   if owner is op.owner:retiring.set()
  def close_journal():
   self.assertTrue(exited.is_set(),'control journal closed before worker exited')
   closed.append('control');return original_close()
  async def close_startup():
   self.assertTrue(exited.is_set(),'startup journal closed before worker exited')
   closed.append('startup');await original_startup_close()
  async def checkpoint():
   marker=loop.create_future();loop.call_soon(marker.set_result,None);await marker
  dispatch=closing=None
  with patch.object(SyntheticBackend,'operation',body),patch.object(StageOwner,'_retire',retire),patch.object(journal,'close',close_journal),patch.object(role.startup,'close',close_startup):
   try:
    dispatch=asyncio.create_task(self.dispatch(role,coordinator))
    await asyncio.wait_for(entered.wait(),5)
    closing=asyncio.create_task(role.close())
    await asyncio.wait_for(retiring.wait(),5)
    self.assertFalse(exited.is_set());self.assertEqual(closed,[])
    closing.cancel();await checkpoint();closing.cancel();await checkpoint()
    self.assertFalse(closing.done());self.assertEqual(closed,[])
    release.set()
    outcome=(await asyncio.wait_for(asyncio.gather(closing,return_exceptions=True),5))[0]
    self.assertIsInstance(outcome,asyncio.CancelledError)
    self.assertTrue(exited.is_set());self.assertEqual(closed,['control','startup'])
    self.assertTrue(role._close_task.done());self.assertTrue(role.channel._task.done())
   finally:
    release.set()
    if dispatch is not None and not dispatch.done():dispatch.cancel()
    if dispatch is not None:await asyncio.gather(dispatch,return_exceptions=True)
    if closing is not None:await asyncio.gather(closing,return_exceptions=True)
  self.assertEqual(self.role_resources[0].query('reservation-a')['state'],'UNKNOWN')
 async def test_foreign_issuance_channel_rejected_without_closing_its_owner(self):
  from cluster_p1.role_startup import RoleStartup
  from unittest.mock import patch
  op=await self.prepare_a()
  startup=next(x for x in self.endpoints if type(x)is RoleStartup and x.stage==0)
  foreign=next(x for x in self.endpoints if type(x)is RoleStartup and x.stage==1)
  calls=[];original=foreign.channel.close
  async def close_foreign():calls.append('closed');await original()
  with patch.object(foreign.channel,'close',close_foreign):
   with self.assertRaises(Exception):
    await InstalledStageRole.attach(startup,op,foreign.channel,self.root/'foreign-control.db',coordinator_node='controller',charge=2097152,control_budget=4194304,identity=Holds(),authorize=Holds())
   self.assertEqual(calls,[]);foreign.client.live_guard()
  self.assertFalse((self.root/'foreign-control.db').exists());self.assertFalse(op.retired)
 # Only select known safe full-chain cases; inherited cases use same new register.
if __name__=='__main__':
 names=['test_online_issued_actual_full_chain','test_online_issued_export_retirement_retains_storage']+[n for n in Tests.__dict__ if n.startswith('test_')]
 suite=unittest.TestSuite(Tests(n) for n in names)
 result=unittest.TextTestRunner().run(suite);raise SystemExit(not result.wasSuccessful())
