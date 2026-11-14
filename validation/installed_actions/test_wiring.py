"""Real installed role/resource/launcher, ordinary data and memory TLS only."""
import asyncio,hashlib,struct,unittest
from validation.role_launcher.test_pipeline import Tests as Base
from validation.control_exchange.test_transport import Holds
from validation.role_bridge import test_transport as wire
from validation.short_actions.test_authority import Clock
from cluster_p1.channel import Channel,Grant
from private_experiment_actions import ActionGate

class Tests(Base):
 async def setup_role(self):
  await self.online_bootstrap();launch=self.launchers[0]
  operation=launch.build_operation(self.spec,self.policy,tokens=struct.pack('<q',1))
  role,control,journal=await self.register(0,self.spec,operation)
  return launch,role,control,journal
 async def channels_for(self,stage=0):
  node=('a','b')[stage];cr,cw,sr,sw=wire.streams(node,'authority');s=self.spec
  g=Grant(s.request,s.session,1,stage,(0,1),s.model,2)
  client=Channel(cr,cw,wire.config('authority'),{'authority':{g}},capacity_bytes=32768,capacity_tiles=1)
  server=Channel(sr,sw,wire.config(node),{node:{g}},capacity_bytes=32768,capacity_tiles=1)
  self.channels.extend((client,server));return client,server
 async def test_real_launcher_can_attach_exact_installed_mapping(self):
  launch,role,control,journal=await self.setup_role()
  self.assertTrue(callable(getattr(launch,'attach_actions',None)),'installed action attachment missing')
  client,server=await self.channels_for()
  gate,_=await asyncio.gather(launch.attach_actions(client,authority_node='authority',authority_certificate=hashlib.sha256(b'authority').hexdigest(),identity=Holds(),clock=Clock()),server.start())
  self.assertIs(gate.service,role.role._service);self.assertEqual(gate.scope()['binding'],role.descriptor.binding)
  self.assertIs(gate._installed,role);self.assertEqual(self.role_loaders[0].calls,[])
  await launch.close();self.assertTrue(client._closed)
 async def attach(self,launch,stage=0):
  client,server=await self.channels_for(stage)
  gate,_=await asyncio.gather(launch.attach_actions(client,authority_node='authority',authority_certificate=hashlib.sha256(b'authority').hexdigest(),identity=Holds(),clock=Clock()),server.start())
  return gate,server
 async def test_missing_identity_or_clock_seals_no_load(self):
  launch,role,control,journal=await self.setup_role();client,server=await self.channels_for()
  with self.assertRaises(Exception):await launch.attach_actions(client,authority_node='authority',authority_certificate=hashlib.sha256(b'authority').hexdigest())
  self.assertTrue(launch._closing);await launch.close();self.assertEqual(self.role_loaders[0].calls,[])
 async def test_second_attachment_seals_original_no_retry(self):
  launch,role,control,journal=await self.setup_role();gate,server=await self.attach(launch)
  with self.assertRaises(Exception):await launch.attach_actions(gate.channel,identity=Holds(),clock=Clock())
  self.assertTrue(launch._closing);await launch.close();self.assertEqual(self.role_loaders[0].calls,[])
 async def test_install_control_mapping_drift_refuses(self):
  launch,role,control,journal=await self.setup_role();gate,server=await self.attach(launch)
  role.startup.store.db.execute("UPDATE install_control SET reservation='different'")
  with self.assertRaises(Exception):launch.check()
  self.assertTrue(launch._closing);self.assertEqual(self.role_loaders[0].calls,[])
 async def test_resource_mapping_unknown_refuses(self):
  launch,role,control,journal=await self.setup_role();gate,server=await self.attach(launch)
  r=launch.startup.runtime['resources'];r.db.execute("UPDATE controls SET state='UNKNOWN'")
  with self.assertRaises(Exception):launch.check()
  self.assertEqual(self.role_loaders[0].calls,[])
 async def test_action_channel_eof_seals_launcher(self):
  launch,role,control,journal=await self.setup_role();gate,server=await self.attach(launch)
  gate.channel.r.feed_eof();await asyncio.gather(gate.channel._task,return_exceptions=True)
  await launch.close();self.assertTrue(launch._closing);self.assertEqual(self.role_loaders[0].calls,[])
 async def test_waiting_attachment_cannot_dispatch_and_close_joins(self):
  from unittest.mock import patch
  launch,role,control,journal=await self.setup_role();client,server=await self.channels_for()
  entered=asyncio.Event();release=asyncio.Event();old=client.start
  async def start():entered.set();await release.wait();return await old()
  with patch.object(client,'start',start):
   work=asyncio.create_task(launch.attach_actions(client,authority_node='authority',authority_certificate=hashlib.sha256(b'authority').hexdigest(),identity=Holds(),clock=Clock()))
   await asyncio.wait_for(entered.wait(),3)
   with self.assertRaises(Exception):await launch.serve_once()
   await launch.close();self.assertTrue(work.done())
   await asyncio.gather(work,return_exceptions=True)
  self.assertEqual(self.role_loaders[0].calls,[]);self.assertTrue(client._closed)
 async def test_unreachable_authority_no_backend_and_retains_charge(self):
  launch,role,control,journal=await self.setup_role();gate,server=await self.attach(launch)
  # No responder: actual one-second request timeout, never a sleep race oracle.
  try:await self.dispatch(role,control)
  except Exception:pass
  self.assertTrue(gate.dead);self.assertEqual(self.role_loaders[0].calls,[])
  self.assertNotEqual(launch.startup.runtime['resources'].query('reservation-a'),None)
 async def test_full_existing_chain_with_lab_action_responder(self):
  from cluster_p1.control_exchange import _Endpoint,_json,_parse
  from private_experiment_admission import digest
  # ONLY authority replies are a LAB double here; real authority+confirmation
  # is tested separately in short_actions. No genuine remote grant is claimed.
  original_register=self.register;original_dispatch=self.dispatch;servers={};seen=[]
  async def register(stage,spec,operation):
   role,control,journal=await original_register(stage,spec,operation)
   gate,server=await self.attach(self.launchers[stage],stage);servers[stage]=(gate,server)
   return role,control,journal
  async def dispatch(role,control):
   gate,server=servers[role.startup.stage];endpoint=_Endpoint(server)
   async def responses():
    for seq,action in enumerate(('start','commit')):
     delivery=await server.receive();raw=delivery.envelope.payload;request=_parse(raw);scope=gate.scope()
     endpoint.validate(delivery.envelope,scope['binding'],seq)
     self.assertEqual(request['scope'],scope);self.assertEqual(request['action'],action)
     self.assertEqual(request['output_digest'] is None,action=='start');seen.append((role.startup.stage,action))
     await delivery.release();delivery.envelope=None
     reply=dict(reply_to=digest(request),receipt=dict(action=action,output_digest=request['output_digest'],scope_digest=digest(scope),authority_boot='a'*32,ttl_ms=5000,execution_allowed=False))
     await server.send(endpoint.envelope(scope['binding'],seq,_json(reply)),max_frame_bytes=65536)
   task=asyncio.create_task(responses())
   try:
    result=await original_dispatch(role,control);await task;return result
   finally:
    if not task.done():task.cancel()
    await asyncio.gather(task,return_exceptions=True)
  self.register=register;self.dispatch=dispatch
  await self.online_bootstrap();await self.run_split()
  self.assertEqual(seen,[(0,'start'),(0,'commit'),(1,'start'),(1,'commit')])
  self.assertEqual(self.receiving_launcher.role.store.history(self.spec.owner,self.spec.request)['state'],'RECEIVED')

 async def test_resource_exit_mapping_drift_before_fence_body(self):
  from contextlib import contextmanager
  from unittest.mock import patch
  import inspect
  launch,role,control,journal=await self.setup_role();gate,server=await self.attach(launch)
  resources=launch.startup.runtime['resources'];original=resources.hold_control;calls=0;entered=[];injected=[]
  @contextmanager
  def hold(*args):
   nonlocal calls
   with original(*args):yield
   caller=next(f.function for f in inspect.stack()[1:] if f.function not in ('__exit__','_non_suppressing'))
   if caller=='_check_installed':calls+=1
   if calls==2 and not injected:
    role.startup.store.db.execute("UPDATE install_control SET reservation='changed-at-exit'");injected.append(True)
  now=gate._now();gate.permit=('start',now,now[1]+4)
  with patch.object(resources,'hold_control',hold):
   try:
    with gate.fence('start'):entered.append('body')
   except Exception:pass
  self.assertEqual(injected,[True], 'fault injection must execute')
  self.assertEqual(entered,[], 'mapping revoked at last resource exit but body entered')

 async def test_resource_exit_poison_before_fence_body(self):
  from contextlib import contextmanager
  from unittest.mock import patch
  import inspect
  launch,role,control,journal=await self.setup_role();gate,server=await self.attach(launch)
  resources=launch.startup.runtime['resources'];original=resources.hold_control;calls=0;entered=[];injected=[]
  @contextmanager
  def hold(*args):
   nonlocal calls
   with original(*args):yield
   if inspect.stack()[4].function=='_check_installed':calls+=1
   if calls==2:
    injected.append(True);resources.poisoned.add(launch.startup.runtime['ticket'].reservation_id)
  now=gate._now();gate.permit=('start',now,now[1]+4)
  with patch.object(resources,'hold_control',hold):
   try:
    with gate.fence('start'):entered.append('body')
   except Exception:pass
  self.assertTrue(injected, 'injection must occur')
  self.assertEqual(entered,[], 'mapping revoked at last resource exit but body entered')

def load_tests(loader,tests,pattern):return unittest.TestSuite(Tests(n) for n in Tests.__dict__ if n.startswith('test_'))
if __name__=='__main__':unittest.main()
