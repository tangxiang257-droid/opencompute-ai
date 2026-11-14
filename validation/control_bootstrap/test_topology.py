"""Original protocol components over LAB memory streams; no real process/network."""
import importlib.util,unittest
class Entry(unittest.TestCase):
 def test_wire_control_bootstrap_exists(self):
  self.assertIsNotNone(importlib.util.find_spec('private_experiment_topology'),'control descriptor still requires shared role object')

import asyncio,hashlib
from unittest.mock import patch
from validation.config_assembly.test_config import Configured
from validation.control_exchange.test_transport import Holds
from validation.authority_chain.test_chain import Channel,Grant,wire
from private_experiment_topology import announce_control,receive_control
class Topology(Configured):
 async def register_role(self,stage,spec,operation,*,bind=True):
  launch=self.launchers[stage];node=self.body['placements'][stage]['node'];cr,cw,sr,sw=wire.streams('controller',node)
  g=Grant(spec.request,spec.session,1,stage,(0,1),spec.model,2)
  client=Channel(cr,cw,wire.config(node),{node:{g}},capacity_bytes=32768,capacity_tiles=1);self.channels.append(client);await client.start()
  role=await launch.attach(sr,sw,wire.config('controller'),{'controller':{g}},identity=Holds(),authorize=Holds())
  pc,ps=self.pair(node,'issuer',stage)
  await asyncio.gather(launch.attach_actions(pc,authority_node='issuer',authority_certificate=hashlib.sha256(b'issuer').hexdigest(),identity=Holds(),clock=self.clock),ps.start())
  args=dict(identity=Holds(),resources=self.central_resources,expected_charge=2097152);args.update(getattr(self,'bootstrap_changes',{}))
  _,control=await asyncio.gather(announce_control(launch),receive_control(self.adm,self.token,self.receipt.proposal_id,self.receipt.proposal_digest,stage,client,**args))
  self.assertIsNot(control.descriptor,role.descriptor) # Same canonical data arrived by wire, not shared identity.
  self.controls[stage]=control;self.owned.append(control)
  async def control_loop():
   while True:await launch.serve_once()
  self.spawn_job(control_loop())
  if not bind:return role,control,ps
  server=await self.bind_server(stage,control,ps)
  self.action_servers[stage]=server
  async def actions():
   for _ in range(2):await server.serve_once()
  self.spawn_job(actions());return role,control

 async def test_actual_wire_descriptor_A_B_C_result(self):
  await self.run_chain()
  for stage in (0,1):
   client=self.controls[stage]._client;role=self.launchers[stage].installed.role._service
   self.assertGreater(client._sequence,2);self.assertGreater(role._sequence,2)
   for channel in (client.channel,role.channel):
    self.assertEqual(sum(frame[:1]==b'H' for frame in channel.w.frames),1)
    self.assertTrue(channel._descriptor_bootstrap_ready)
 async def test_duplicate_bootstrap_cannot_reconstruct_controller(self):
  _,control=await self.prepare_a()
  with self.assertRaises(Exception):await receive_control(self.adm,self.token,self.receipt.proposal_id,self.receipt.proposal_digest,0,control._client.channel,identity=Holds(),resources=self.central_resources,expected_charge=2097152)
  self.assertEqual(self.calls,[[],[]]);self.assertEqual(self.action_rows(),[(0,'control-bound')])
 async def test_wrong_expected_charge_rejects_before_actions(self):
  self.bootstrap_changes=dict(expected_charge=2097153)
  with self.assertRaises(Exception):await self.prepare_a()
  self.assertEqual(self.calls,[[],[]]);self.assertEqual(self.action_rows(),[])
 async def test_identity_denial_rejects_wire_descriptor(self):
  identity=Holds();identity.fail=True;self.bootstrap_changes=dict(identity=identity)
  with self.assertRaises(Exception):await self.prepare_a()
  self.assertEqual(self.calls,[[],[]]);self.assertEqual(self.action_rows(),[])


 async def test_pending_bootstrap_blocks_actual_client_and_service(self):
  import private_experiment_topology as topology
  from cluster_p1.control_exchange import ControlClient
  entered=asyncio.Event();release=asyncio.Event();old=topology._send;central=[]
  async def pause(endpoint,scope,seq,value,fence):
   if value['kind']=='HELLO':central.append(endpoint.channel);entered.set();await release.wait()
   return await old(endpoint,scope,seq,value,fence)
  with patch.object(topology,'_send',pause):
   work=self.spawn_job(self.prepare_a());await asyncio.wait_for(entered.wait(),3)
   service=self.launchers[0].installed.role._service
   with self.assertRaises(Exception):await service.serve_once()
   with self.assertRaises(Exception):await ControlClient(central[0]).request('query','x','y',{},1)
   self.assertEqual(self.calls,[[],[]]);release.set();await work
  self.assertEqual(self.action_rows(),[(0,'control-bound')])
 async def test_wrong_challenge_replay_rejected(self):
  import private_experiment_topology as topology
  old=topology._send;hit=[]
  async def corrupt(endpoint,scope,seq,value,fence):
   if value['kind']=='ANNOUNCE':value=dict(value,challenge='0'*64);hit.append(1)
   return await old(endpoint,scope,seq,value,fence)
  with patch.object(topology,'_send',corrupt):
   with self.assertRaises(Exception):await self.prepare_a()
  self.assertEqual(hit,[1]);self.assertTrue(self.adm.retired);self.assertEqual(self.calls,[[],[]]);self.assertEqual(self.action_rows(),[])
 async def test_wrong_announced_placement_rejected(self):
  import private_experiment_topology as topology
  old=topology._send;hit=[]
  async def corrupt(endpoint,scope,seq,value,fence):
   if value['kind']=='ANNOUNCE':
    import copy
    value=copy.deepcopy(value);value['descriptor']['binding']['instance']='foreign';hit.append(1)
   return await old(endpoint,scope,seq,value,fence)
  with patch.object(topology,'_send',corrupt):
   with self.assertRaises(Exception):await self.prepare_a()
  self.assertEqual(hit,[1]);self.assertEqual(self.calls,[[],[]]);self.assertEqual(self.action_rows(),[])
 async def test_ack_written_then_failure_no_controller_or_retry(self):
  import private_experiment_topology as topology
  old=topology._send;hit=[]
  async def lost(endpoint,scope,seq,value,fence):
   result=await old(endpoint,scope,seq,value,fence)
   if value['kind']=='ACK':hit.append(1);raise OSError('LAB ACK sent but local completion failed')
   return result
  with patch.object(topology,'_send',lost):
   with self.assertRaises(Exception):await self.prepare_a()
  self.assertEqual(hit,[1]);self.assertTrue(self.adm.retired);self.assertEqual(self.controls,{});self.assertEqual(self.action_rows(),[]);self.assertEqual(self.calls,[[],[]])
 async def test_revoke_while_announcement_waits_denies_controller(self):
  import private_experiment_topology as topology
  old=topology._send;entered=asyncio.Event();release=asyncio.Event()
  async def pause(endpoint,scope,seq,value,fence):
   if value['kind']=='ANNOUNCE':entered.set();await release.wait()
   return await old(endpoint,scope,seq,value,fence)
  with patch.object(topology,'_send',pause):
   work=self.spawn_job(self.prepare_a());await asyncio.wait_for(entered.wait(),3)
   await self.adm.revoke(self.token,self.task.task_id);release.set()
   with self.assertRaises(Exception):await work
  self.assertEqual(self.controls,{});self.assertEqual(self.action_rows(),[]);self.assertEqual(self.calls,[[],[]])
 async def test_no_local_locks_across_bootstrap_wire(self):
  old=wire.Writer.drain;hits=[]
  async def drain(writer):
   self.assertFalse(self.consent.authority._lock.locked());self.assertEqual(self.central_resources.active,0)
   for launch in self.launchers.values():
    r=launch._runtime['resources'];self.assertEqual(r.capacity.active,0);self.assertEqual(r.authority.active,0)
   hits.append(1);return await old(writer)
  with patch.object(wire.Writer,'drain',drain):await self.prepare_a()
  self.assertGreater(len(hits),4);self.assertEqual(self.calls,[[],[]])

 async def test_invalid_local_context_cannot_leave_taken_channel_open(self):
  old=receive_control;seen=[]
  async def inject(a,credential,pid,sha,stage,ch,**kw):
   seen.append(ch);a._installations[(pid,stage)]['body']={}
   return await old(a,credential,pid,sha,stage,ch,**kw)
  with patch.dict(globals(),{'receive_control':inject}):
   with self.assertRaises(Exception):await self.prepare_a()
  self.assertEqual(len(seen),1);ch=seen[0]
  self.assertTrue(not getattr(ch,'_descriptor_bootstrap_attempted',False) or ch.w.is_closing(), 'taken channel escaped post-claim initialization error')

 async def test_final_role_callback_drift_prevents_announcement_write(self):
  import private_experiment_topology as topology
  from cluster_p1.role_assembly import InstalledStageRole
  old_send=topology._send;old_descriptor=InstalledStageRole.descriptor
  armed=[];reads=[];injected=[];channels=[]
  async def send(endpoint,scope,seq,value,fence):
   if value['kind']=='ANNOUNCE':armed.append(True);channels.append(endpoint.channel)
   return await old_send(endpoint,scope,seq,value,fence)
  def descriptor(role):
   value=old_descriptor.fget(role)
   if armed and not injected:
    reads.append(True)
    if len(reads)==2:
     injected.append(True);role.role._service.authorize=Holds()
   return value
  with patch.object(topology,'_send',send),patch.object(InstalledStageRole,'descriptor',property(descriptor)):
   with self.assertRaises(Exception):await self.prepare_a()
  self.assertEqual(injected,[True]);self.assertEqual(len(channels),1)
  self.assertEqual(sum(frame[:1]==b'D' for frame in channels[0].w.frames),0,'ANNOUNCE written after final role provider drift')


 async def test_claimed_writer_cannot_be_wrapped_for_second_bootstrap(self):
  import private_experiment_topology as topology
  _,control=await self.prepare_a();original=control._client.channel
  alias=Channel(original.r,original.w,wire.config('a'),{'a':set(original.grants)},capacity_bytes=32768,capacity_tiles=1)
  try:
   with self.assertRaises(Exception):topology._claim(alias)
   self.assertFalse(getattr(alias,'_descriptor_bootstrap_attempted',False))
  finally:alias._task.cancel();await asyncio.gather(alias._task,return_exceptions=True)
 async def test_cancel_pending_handshake_closes_taken_channels(self):
  import private_experiment_topology as topology
  old=topology._send;entered=asyncio.Event();release=asyncio.Event();channels=[]
  async def pause(endpoint,scope,seq,value,fence):
   if value['kind']=='HELLO':channels.append(endpoint.channel);entered.set();await release.wait()
   return await old(endpoint,scope,seq,value,fence)
  with patch.object(topology,'_send',pause):
   work=self.spawn_job(self.prepare_a());await asyncio.wait_for(entered.wait(),3);work.cancel()
   with self.assertRaises(asyncio.CancelledError):await work
  self.assertTrue(channels[0].w.is_closing());self.assertTrue(self.adm.retired);self.assertEqual(self.controls,{});self.assertEqual(self.calls,[[],[]])

 async def test_post_claim_live_failure_joins_role_cleanup(self):
  import private_experiment_topology as topology
  old_claim=topology._claim;old_announce=announce_control
  close_entered=asyncio.Event();release=asyncio.Event();finished=[];injected=[]
  def claim(ch):
   old_claim(ch);launch=self.launchers[0]
   if ch is launch.control and not injected:
    injected.append(True);old_close=ch.close
    async def close():close_entered.set();await release.wait();return await old_close()
    ch.close=close
    launch._seal() # Actual existing lifecycle transition, as an EOF watcher uses.
  async def announce(launch):
   try:return await old_announce(launch)
   finally:finished.append(True)
  with patch.object(topology,'_claim',claim),patch.dict(globals(),{'announce_control':announce}):
   work=asyncio.create_task(self.prepare_a())
   try:
    await asyncio.wait_for(close_entered.wait(),3)
    checkpoint=asyncio.Event();asyncio.get_running_loop().call_soon(checkpoint.set);await checkpoint.wait()
    early=bool(finished)
   finally:
    release.set();await asyncio.gather(work,return_exceptions=True)
    await self.launchers[0].close()
  self.assertEqual(injected,[True]);self.assertFalse(early,'announce escaped while taken role cleanup still pending')


 async def test_nested_launcher_job_refuses_before_bootstrap_take(self):
  old=announce_control;hits=[]
  async def probe(launch):
   async def nested():
    with self.assertRaises(Exception):await old(launch)
    self.assertFalse(getattr(launch.control,'_descriptor_bootstrap_attempted',False));hits.append(1)
   await launch._run(nested)
   return await old(launch)
  with patch.dict(globals(),{'announce_control':probe}):await self.prepare_a()
  self.assertEqual(hits,[1]);self.assertEqual(self.calls,[[],[]])
 async def test_central_revoke_before_HELLO_write_sends_no_metadata(self):
  import private_experiment_topology as topology
  old=topology._send;entered=asyncio.Event();release=asyncio.Event();channels=[]
  async def pause(endpoint,scope,seq,value,fence):
   if value['kind']=='HELLO':channels.append(endpoint.channel);entered.set();await release.wait()
   return await old(endpoint,scope,seq,value,fence)
  with patch.object(topology,'_send',pause):
   work=self.spawn_job(self.prepare_a());await asyncio.wait_for(entered.wait(),3)
   await self.adm.revoke(self.token,self.task.task_id);release.set()
   with self.assertRaises(Exception):await work
  self.assertEqual(sum(f[:1]==b'D' for f in channels[0].w.frames),0);self.assertEqual(self.controls,{});self.assertEqual(self.calls,[[],[]])


 async def test_bootstrap_claim_cannot_be_reused_as_action_purpose(self):
  import private_experiment_topology as topology
  _,control=await self.prepare_a();role,central=self.pair('a','issuer',0)
  await asyncio.gather(role.start(),central.start());topology._claim(central)
  with self.assertRaises(Exception):await self.configured(0,control,central)
  self.assertFalse(central.w.is_closing());self.assertFalse(self.adm.retired)
  self.assertFalse(getattr(central,'_experiment_action_owned',False))

def load_tests(loader,tests,pattern):return unittest.TestSuite(cls(n) for cls in (Entry,Topology) for n in cls.__dict__ if n.startswith('test_'))
if __name__=='__main__':unittest.main()
