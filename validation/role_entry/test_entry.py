"""Real private orchestration over LAB streams/providers; no real TLS/device."""
import asyncio,hashlib,unittest
from unittest.mock import patch
import private_experiment_topology as topology
from cluster_p1.installation_session import SessionPolicy
from cluster_p1.role_launcher import StageLauncher
from validation.control_bootstrap.test_topology import Topology
from validation.authority_chain.test_chain import Channel,Grant,wire
from validation.control_exchange.test_transport import Holds

class Entry(unittest.TestCase):
 def test_entry_exists(self):self.assertTrue(callable(getattr(topology,'bind_control_ready',None)),'role-local real attach path missing')

class Flow(Topology):
 async def install_stages(self):
  self.installation_session_policy=SessionPolicy()
  await super().install_stages()
 async def endpoints(self,stage,spec):
  launch=self.launchers[stage];node=self.body['placements'][stage]['node'];cr,cw,sr,sw=wire.streams('controller',node)
  g=Grant(spec.request,spec.session,1,stage,(0,1),spec.model,2)
  client=Channel(cr,cw,wire.config(node),{node:{g}},capacity_bytes=32768,capacity_tiles=1);self.channels.append(client);await client.start()
  pc,ps=self.pair(node,'issuer',stage);await ps.start()
  args=(launch,sr,sw,wire.config('controller'),{'controller':{g}},pc)
  kw=dict(enabled=True,identity=Holds(),authorize=Holds(),action_identity=Holds(),clock=self.clock,authority_node='issuer',authority_certificate=hashlib.sha256(b'issuer').hexdigest())
  self.last_entry=(args,kw,client,ps)
  return args,kw,client,ps
 async def register_role(self,stage,spec,operation,*,bind=True):
  args,kw,client,ps=await self.endpoints(stage,spec)
  role,control=await asyncio.gather(topology.bind_control_ready(*args,**kw),topology.receive_control(self.adm,self.token,self.receipt.proposal_id,self.receipt.proposal_digest,stage,client,identity=Holds(),resources=self.central_resources,expected_charge=2097152))
  self.assertIsNot(control.descriptor,role.descriptor);self.controls[stage]=control;self.owned.append(control)
  async def loop():
   while True:await args[0].serve_once()
  self.spawn_job(loop())
  if not bind:return role,control,ps
  server=await self.bind_server(stage,control,ps);self.action_servers[stage]=server
  async def actions():
   for _ in range(2):await server.serve_once()
  self.spawn_job(actions());return role,control
 async def prepared_endpoints(self):
  import struct
  self.launchers[0].build_operation(self.spec,self.pipe_policy,tokens=struct.pack('<q',1))
  return await self.endpoints(0,self.spec)
 async def test_real_A_B_C_chain(self):
  await self.run_chain()
  for s in (0,1):
   l=self.launchers[s];self.assertFalse(l._control_assembly_pending)
   self.assertTrue(l.control._descriptor_bootstrap_ready)
   self.assertTrue(l.startup.client._session.active)
 async def test_disabled_and_missing_provider_no_take(self):
  args,kw,_,_=await self.prepared_endpoints()
  for change in ({'enabled':False},{'identity':None},{'authorize':None},{'action_identity':None},{'clock':None}):
   with self.assertRaises(Exception):await topology.bind_control_ready(*args,**dict(kw,**change))
   self.assertFalse(getattr(args[0],'_control_assembly_attempted',False));self.assertFalse(args[2].is_closing());self.assertFalse(args[5]._closed)
  self.assertEqual(self.calls,[[],[]])
 async def test_alias_refused_before_take(self):
  args,kw,_,_=await self.prepared_endpoints();bad=(args[0],args[1],args[5].w,*args[3:])
  with self.assertRaises(Exception):await topology.bind_control_ready(*bad,**kw)
  self.assertFalse(getattr(args[0],'_control_assembly_attempted',False));self.assertEqual(self.calls,[[],[]])
 async def test_pending_blocks_both_service_levels_and_cancel_joins(self):
  args,kw,_,_=await self.prepared_endpoints();launch=args[0];entered=asyncio.Event();release=asyncio.Event();old=StageLauncher._attach_taken_actions
  async def paused(obj,*a,**k):entered.set();await release.wait();return await old(obj,*a,**k)
  with patch.object(StageLauncher,'_attach_taken_actions',paused):
   task=asyncio.create_task(topology.bind_control_ready(*args,**kw))
   try:
    await asyncio.wait_for(entered.wait(),5)
    with self.assertRaises(Exception):await launch.serve_once()
    with self.assertRaises(Exception):await launch.installed.role._service.serve_once()
    with self.assertRaises(Exception):await topology.bind_control_ready(*args,**kw)
    self.assertTrue(any(c is args[5] for c in launch._extra_channels))
    with self.assertRaises(ValueError):await old(launch,args[5],authority_node=kw['authority_node'],authority_certificate=kw['authority_certificate'],identity=kw['action_identity'],clock=kw['clock'])
    with self.assertRaises(ValueError):await launch.attach_actions(args[5],authority_node=kw['authority_node'],authority_certificate=kw['authority_certificate'],identity=kw['action_identity'],clock=kw['clock'])
    self.assertFalse(launch._closing)
    self.assertFalse(task.done());self.assertEqual(self.calls,[[],[]]);task.cancel()
    with self.assertRaises(asyncio.CancelledError):await task
   finally:
    release.set()
    if not task.done():task.cancel();await asyncio.gather(task,return_exceptions=True)
  self.assertTrue(launch._closing);self.assertTrue(launch._close_task.done());self.assertTrue(args[5]._closed)
  self.assertTrue(args[5]._task.done());self.assertTrue(args[2].is_closing());self.assertTrue(launch._control_assembly_pending)
 async def test_wrong_authority_pin_refuses_without_take(self):
  args,kw,_,_=await self.prepared_endpoints()
  for change in ({'authority_node':'other'},{'authority_certificate':'0'*64}):
   with self.assertRaises(Exception):await topology.bind_control_ready(*args,**dict(kw,**change))
   self.assertFalse(getattr(args[0],'_control_assembly_attempted',False));self.assertFalse(args[5]._closed)
  self.assertEqual(self.calls,[[],[]])
 async def test_dead_installation_denies_before_take(self):
  args,kw,_,_=await self.prepared_endpoints();launch=args[0]
  await launch.startup.client._session.stop()
  with self.assertRaises(Exception):await topology.bind_control_ready(*args,**kw)
  self.assertFalse(getattr(launch,'_control_assembly_attempted',False));self.assertEqual(self.calls,[[],[]])
 async def test_failed_action_attach_closes_all_owned_transports(self):
  args,kw,_,_=await self.prepared_endpoints();launch=args[0]
  class Reject:
   def hold(self,*a,**k):raise ValueError('injected action identity denial')
  with self.assertRaises(Exception):await topology.bind_control_ready(*args,**dict(kw,action_identity=Reject()))
  self.assertTrue(launch._closing);self.assertTrue(launch._close_task.done())
  self.assertTrue(args[5]._closed);self.assertTrue(args[5]._task.done())
  self.assertTrue(args[2].is_closing());self.assertTrue(launch._control_assembly_pending);self.assertEqual(self.calls,[[],[]])
 async def test_nested_owner_job_rejected_before_take(self):
  args,kw,_,_=await self.prepared_endpoints();launch=args[0]
  async def nested():return await topology.bind_control_ready(*args,**kw)
  with self.assertRaises(ValueError):await asyncio.wait_for(launch._run(nested),2)
  self.assertFalse(getattr(launch,'_control_assembly_attempted',False))
  self.assertFalse(args[5]._closed);self.assertFalse(args[2].is_closing());self.assertTrue(launch._close_task.done())
 async def test_foreign_installed_channel_rejected_without_close(self):
  args,kw,_,_=await self.prepared_endpoints();args[5]._installed_role_control_claimed=True
  with self.assertRaises(Exception):await topology.bind_control_ready(*args,**kw)
  self.assertFalse(getattr(args[0],'_control_assembly_attempted',False))
  self.assertFalse(args[5]._closed);self.assertFalse(args[2].is_closing())
 async def test_external_close_joins_announcement_caller_without_cycle(self):
  args,kw,client,_=await self.prepared_endpoints();launch=args[0]
  entered=asyncio.Event();release=asyncio.Event();old=topology._send
  async def send(ep,scope,seq,value,fence):
   if value['kind']=='ANNOUNCE':entered.set();await release.wait()
   return await old(ep,scope,seq,value,fence)
  with patch.object(topology,'_send',send):
   bind=asyncio.create_task(topology.bind_control_ready(*args,**kw))
   central=asyncio.create_task(topology.receive_control(self.adm,self.token,self.receipt.proposal_id,self.receipt.proposal_digest,0,client,identity=Holds(),resources=self.central_resources,expected_charge=2097152))
   await asyncio.wait_for(entered.wait(),5);self.assertIn(bind,launch._jobs)
   external=asyncio.create_task(launch.close())
   try:
    done,pending=await asyncio.wait((bind,central,external),timeout=5)
    self.assertFalse(pending,'external close must not wait on a caller joining that same close')
    self.assertTrue(bind.cancelled());self.assertIsNone(external.result())
    self.assertIsInstance(central.exception(),Exception)
   finally:
    release.set()
    for task in (bind,central,external):
     if not task.done():task.cancel()
    await asyncio.gather(bind,central,external,return_exceptions=True)
  self.assertTrue(launch._close_task.done());self.assertTrue(args[5]._closed)
  self.assertTrue(args[5]._task.done());self.assertTrue(launch.control._task.done());self.assertEqual(self.calls,[[],[]])
 async def test_repeat_cannot_rebind_live_owner(self):
  await self.prepare_a();args,kw,_,_=self.last_entry
  with self.assertRaises(Exception):await topology.bind_control_ready(*args,**kw)
  self.assertFalse(args[0]._closing);self.assertEqual(self.calls,[[],[]])

def load_tests(loader,tests,pattern):return unittest.TestSuite([Entry('test_entry_exists'),*(Flow(n) for n in Flow.__dict__ if n.startswith('test_'))])
if __name__=='__main__':unittest.main()
