"""Actual authority/launcher chain; external identity/device/capacity/TLS are LAB."""
import unittest,importlib.util,asyncio,json,hashlib,struct
from contextlib import contextmanager
from dataclasses import replace
from unittest.mock import patch
from validation.c_input_import.test_import import Tests as InputBase,LabProvider
from validation.experiment_confirmation import test_entry as confirmation_fixture
from validation.experiment_admission import test_admission as admission_fixture
from private_experiment_confirmation import ExperimentConfirmationApp
from private_experiment_admission import SHORT_ACTION_PROFILE
from cluster_p1.online_issuance import IssuanceAuthority,RoleInstallStore
from private_experiment_control import create_action_server
from private_c_receiving_authority import CReceivingPolicy
from cluster_p1.role_launcher import StageLauncher,ReceivingLauncher
from cluster_p1.role_resources import RoleResources
from cluster_p1.role_control import CoordinatorControl
from cluster_p1.control_exchange import ControlClient
from cluster_p1.projection_driver import Spec,Policy
from cluster_p1.private_one_token import Loaded
from cluster_p1.prepared_receipt import PreparedReceiptConnection
from cluster_p1.channel import Channel,Grant
from cluster_p1.c_receiving import ReceivingStore,ReceivingController
from cluster_p1.c_receiving_input import CommittedBInput
from cluster_p1.byte_spool import Book,Binding,Plan,Spool
from cluster_p1.split_role_transport import HiddenSender,HiddenReceiver,ResultSender,transport_scope
from cluster_p1.envelope import Envelope
from isolated_tls_delivery import ReceiveBinding
from local_delivery_gate import Scope
from validation.role_launcher.test_pipeline import configuration
from validation.control_exchange.test_transport import Holds
from validation.c_receiving.test_protocol import Identity,Policy as CPolicy
from validation.role_bridge import test_transport as wire
from validation.one_token import test_assembly as compute
from validation.online_issuance.test_pipeline import BootstrapDevice
from validation.short_actions.test_authority import Clock
class Entry(unittest.TestCase):
 def test_concrete_coordinator_binding_exists(self):
  self.assertIsNotNone(importlib.util.find_spec('private_experiment_control'),'concrete original-control binding missing')
class Chain(InputBase):
 async def prepare_new(self):
  return await self.adm.prepare(self.token,self.task.task_id,self.declaration,self.policy,self.placements,receiver=self.receiver,c_input_copy=self.copy,issuer=self.strict,runtime_profile=SHORT_ACTION_PROFILE)
 async def asyncSetUp(self):
  self.central_resources=Holds();self.install_policy=Holds()
  def issuer(*a,**kw):return IssuanceAuthority(*a,**dict(kw,resources=self.central_resources,enable_receiving=True))
  def web(*a,**kw):return ExperimentConfirmationApp(*a,**dict(kw,installation_authority=self.install_policy))
  with patch.object(admission_fixture,'IssuanceAuthority',issuer),patch.object(confirmation_fixture,'ExperimentConfirmationApp',web):await super().asyncSetUp()
  await self.registered();await self.run_import()
  self.body=json.loads(self.adm._row(self.owner,self.receipt.proposal_id)[0]);d=self.body['declaration'];s=d['session'];src=self.body['source']
  self.spec=Spec(self.owner,self.body['execution_task'],s['session_id'],1,s['model_hash'],s['layout_hash'],src['input_snapshot'],src['preprocessing'],d['plan_digest'],src['token_digest'],24,100,(src['token_digest'],))
  self.pipe_policy=Policy(100,200,300,1000000,524288,2097152,2097152)
  self.channels=[];self.owned=[];self.jobs=[];self.launchers={};self.controls={};self.action_servers={};self.calls=[[],[]];self.loaders=[];self.clock=Clock()
  self.addAsyncCleanup(self.shutdown)
  self.cp=await CReceivingPolicy.create_experiment(self.importer,self.token,self.receipt.proposal_id,self.receipt.proposal_digest,parent=LabProvider(),maintenance=LabProvider())
  await self.install_stages()
 async def shutdown(self):
  for t in self.jobs:
   if not t.done():t.cancel()
  await asyncio.gather(*self.jobs,return_exceptions=True)
  for o in reversed(self.owned):
   try:await o.close()
   except Exception:pass # Assertions inspect failures; cleanup must drain all owners.
  await asyncio.gather(*(c.close() for c in self.channels),return_exceptions=True)
 def pair(self,left,right,stage,capacity=32768):
  cr,cw,sr,sw=wire.streams(left,right);s=self.spec;g=Grant(s.request,s.session,1,stage,(0,1),s.model,2)
  a=Channel(cr,cw,wire.config(right),{right:{g}},capacity_bytes=capacity,capacity_tiles=1)
  b=Channel(sr,sw,wire.config(left),{left:{g}},capacity_bytes=capacity,capacity_tiles=1)
  self.channels.extend((a,b));return a,b
 def spawn_job(self,coro):
  t=asyncio.create_task(coro);self.jobs.append(t);return t
 async def install_stages(self):
  s=self.spec;ar,aw,br,bw=wire.streams('a','b');g=Grant(s.request,s.session,1,0,(0,1),s.model,2)
  self.hidden_send=Channel(ar,aw,wire.config('b'),{'b':{g}},capacity_bytes=65536,capacity_tiles=1)
  prepared=await PreparedReceiptConnection.create(br,bw,wire.config('a'),{'a':{g}});self.channels.extend((self.hidden_send,prepared.channel));self.prepared=prepared
  registration=self.adm._registered[self.receipt.proposal_id]
  for stage in (0,1):
   p=self.body['placements'][stage];d=self.strict.describe(registration,stage,**{k:p[k] for k in ('node','device','instance','resource_id')})
   r=RoleResources(self.root/f'role-resource-{stage}',node=p['node'],pool_limits=dict(device=1000000,host=1048576,source=2097152,destination=2097152),extra_host_bytes=300000,authority=Holds(),capacity=Holds());self.addCleanup(r.close)
   ticket=r.reserve(p['resource_id'],d,s,self.pipe_policy,charge=2097152)
   self.assertIsNot(r,self.central_resources)
   store=RoleInstallStore(self.root/f'role-store-{stage}',role_node=p['node'],identity=Holds(),authorize=Holds(),resources=r,device=Holds());self.addCleanup(store.close)
   layers=self.body['declaration']['stages'][stage];extent=(layers['layer_start'],layers['layer_end']);instance=p['instance']
   class Loader:
    def __init__(inner,extent,instance):inner.calls=[];inner.extent=extent;inner.instance=instance
    def load(inner,number,spec):inner.calls.append(number);return Loaded(object(),spec.model,spec.layout,inner.extent,inner.instance)
    def quarantine(inner,spec):pass
   loader=Loader(extent,instance);self.loaders.append(loader)
   def factory(number,loaded,spec,completion):return compute.SyntheticBackend(number,spec,completion,self.calls[number])
   runtime=dict(instance=instance,layer_range=extent,authority=compute.SyntheticAuthority(),resources=r,ticket=ticket,device=compute.SyntheticDevice(),loader=loader,factories=factory)
   cr,cw,sr,sw=wire.streams(p['node'],'issuer');g=Grant(s.request,s.session,1,stage,(0,1),s.model,2)
   server=Channel(sr,sw,wire.config(p['node']),{p['node']:{g}},capacity_bytes=32768,capacity_tiles=1);self.channels.append(server)
   service=await self.adm.installation_service(self.token,self.receipt.proposal_id,self.receipt.proposal_digest,stage,server,issuer_node='issuer',issuer_certificate=hashlib.sha256(b'issuer').hexdigest(),identity=Identity(),authorize=Holds(),resources=self.central_resources);self.owned.append(service)
   launch=await StageLauncher.create(configuration(self.root,('A','B')[stage],d),cr,cw,wire.config('issuer'),{'issuer':{g}},store,d,identity=Identity(),authorize=Holds(),resources=r,completion=BootstrapDevice(),runtime=runtime);self.owned.append(launch);self.launchers[stage]=launch
   @contextmanager
   def owner(*args):yield self.owner
   args=dict(execution_authorize=owner) if stage==0 else dict(prepared=prepared)
   await asyncio.gather(service.serve_once(),launch.install(**args))
 async def register_role(self,stage,spec,operation,*,bind=True):
  launch=self.launchers[stage];node=self.body['placements'][stage]['node'];cr,cw,sr,sw=wire.streams('controller',node)
  g=Grant(spec.request,spec.session,1,stage,(0,1),spec.model,2)
  client=Channel(cr,cw,wire.config(node),{node:{g}},capacity_bytes=32768,capacity_tiles=1);self.channels.append(client);await client.start()
  role=await launch.attach(sr,sw,wire.config('controller'),{'controller':{g}},identity=Holds(),authorize=Holds())
  control=CoordinatorControl(ControlClient(client),role.descriptor);self.controls[stage]=control;self.owned.append(control)
  pc,ps=self.pair(node,'issuer',stage)
  await asyncio.gather(launch.attach_actions(pc,authority_node='issuer',authority_certificate=hashlib.sha256(b'issuer').hexdigest(),identity=Holds(),clock=self.clock),ps.start())
  async def control_loop():
   while True:await launch.serve_once()
  self.spawn_job(control_loop())
  if not bind:return role,control,ps
  server=await self.bind_server(stage,control,ps)
  self.action_servers[stage]=server
  async def actions():
   for _ in range(2):await server.serve_once()
  self.spawn_job(actions());return role,control
 async def bind_server(self,stage,control,channel,**changes):
  kw=dict(identity=Holds(),clock=self.clock);kw.update(changes)
  return await create_action_server(self.adm,self.token,self.receipt.proposal_id,self.receipt.proposal_digest,stage,control,channel,**kw)
 async def prepare_a(self,*,bind=True):
  op=self.launchers[0].build_operation(self.spec,self.pipe_policy,tokens=struct.pack('<q',1))
  self.a_operation=op;return await self.register_role(0,self.spec,op,bind=bind)
 async def execute_a(self):
  role,control=await self.prepare_a();
  receipt=await control.dispatch();self.assertEqual(receipt['state'],'COMMITTED')
  hs=HiddenSender(self.hidden_send,scope=transport_scope(self.spec),identity=Identity(),authorize=Holds())
  hr=HiddenReceiver(self.prepared.bound,self.launchers[1].startup.journal,scope=transport_scope(self.spec),identity=Identity(),authorize=Holds());self.owned.extend((hs,hr))
  await self.hidden_send.start();raw=Envelope(self.spec.request,self.spec.session,1,0,0,(0,1),self.spec.model,2,(1,1,self.spec.hidden),'float32',self.a_operation.committed_bytes()).encode()
  a,b=await asyncio.gather(hs.send(raw),hr.receive());self.assertEqual(a,b)
  return replace(self.spec,inputs=(b['payload_digest'],))
 async def run_chain(self):
  actual=await self.execute_a()
  dr,dw,rr,rw=wire.streams('b','c');s=actual;g=Grant(s.request,s.session,1,1,(0,1),s.model,2)
  data=Channel(dr,dw,wire.config('c'),{'c':{g}},capacity_bytes=32816,capacity_tiles=1);self.channels.append(data)
  meta,cmeta=self.pair('b','c',1)
  binding=ReceiveBinding(Scope(s.request,1,s.plan_digest,s.layout,'out','logits',s.session,'single-token','b',hashlib.sha256(b'b').hexdigest(),'placeholder'),1,s.model,2,'float32',((0,1,(1,1,s.vocab)),))
  sender=ResultSender(data,meta,binding,identity=Identity(),authorize=Holds());self.owned.append(sender)
  source=Book(self.root/'source-book',2097152);dest=Book(self.root/'dest-book',2097152);self.addCleanup(source.close);self.addCleanup(dest.close)
  plan=Plan(Binding(s.owner,s.request,s.session,'single-token',1,sender.generation(),s.model,s.layout,s.input_snapshot,s.token_digest,s.preprocessing,s.plan_digest),(1,),s.vocab)
  source.reserve('result',plan,'source');dest.reserve('result',plan,'destination');spool=Spool(self.root/'spool',source,dest,'result',plan)
  op=self.launchers[1].build_operation(actual,self.pipe_policy,spool=spool);self.b_operation=op
  role,control=await self.register_role(1,actual,op)
  ac,cc=self.pair('issuer','c',1);store=ReceivingStore(self.root/'c-receiving');self.addCleanup(store.close)
  d=self.cp.descriptor;controller=ReceivingController(ac,ReceivingStore.for_authority(self.strict),d,identity=Identity(),authorize=self.cp,maintenance=self.cp,resources=CPolicy());self.owned.append(controller)
  receiving=await ReceivingLauncher.create(configuration(self.root,'C',d),cc,store,d,identity=Identity(),authorize=self.cp,maintenance=self.cp,resources=CPolicy());self.owned.append(receiving);self.c_launcher=receiving
  await asyncio.gather(controller.pending(),receiving.serve_once())
  await asyncio.gather(controller.activate(CommittedBInput(control)),receiving.serve_once())
  receiving.attach_result(actual,rr,rw,wire.config('b'),cmeta,binding,identity=Identity(),authorize=Holds())
  bg,cg=self.pair('b','c',1);self.launchers[1].attach_gate(bg,d,identity=Identity(),authorize=CPolicy());receiving.attach_gate(cg,identity=Identity(),authorize=CPolicy());self.spawn_job(receiving.serve_gate())
  receipt=await control.dispatch();self.assertEqual(receipt['state'],'COMMITTED');op.enable_read()
  ack,manifest=await asyncio.gather(sender.send(actual,spool,op.metadata['output_digest'],op.check_export),receiving.receive_result(self.artifacts,lambda:None,lambda:self.cp.hold(d,'receive')))
  self.assertEqual(ack['manifest_digest'],manifest);self.assertEqual(store.history(self.owner,s.request)['state'],'RECEIVED')
  chunks=[];self.artifacts.fetch(self.owner,s.request,lambda _,raw:chunks.append(raw));self.assertEqual(b''.join(chunks),struct.pack('<f',1.)*100)
  return actual
 async def test_actual_confirmation_authority_launchers_B_proof_C_result(self):
  actual=await self.run_chain()
  rows=self.adm.db.execute('SELECT stage,action,scope FROM short_actions ORDER BY stage,action').fetchall()
  self.assertEqual([(r[0],r[1]) for r in rows],[(s,a) for s in (0,1) for a in ('commit','control-bound','start')])
  record=json.loads(next(r[2] for r in rows if r[:2]==(1,'control-bound')))
  self.assertEqual(record['b_input']['input_digest'],actual.inputs[0]);self.assertNotEqual(actual.inputs[0],self.spec.token_digest)
  self.assertNotEqual(record['role_peer']['connection_generation'],record['installation_peer']['connection_generation'])

 def action_rows(self):return self.adm.db.execute('SELECT stage,action FROM short_actions ORDER BY stage,action').fetchall()
 async def test_missing_clock_or_identity_default_denies(self):
  _,control,ch=await self.prepare_a(bind=False)
  for change in (dict(identity=None),dict(clock=None)):
   with self.assertRaises(Exception):await self.bind_server(0,control,ch,**change)
  self.assertEqual(self.action_rows(),[]);self.assertEqual(self.calls,[[],[]])
 async def test_mapping_once_only_cannot_rebind(self):
  _,control=await self.prepare_a();before=self.action_rows()
  with self.assertRaises(Exception):await self.bind_server(0,control,self.action_servers[0].channel)
  self.assertEqual(self.action_rows(),before);self.assertEqual(self.calls,[[],[]])
 async def test_wrong_control_identity_refuses(self):
  _,control,ch=await self.prepare_a(bind=False)
  control._client._identity=replace(control._client._identity,certificate_sha256='0'*64)
  with self.assertRaises(Exception):await self.bind_server(0,control,ch)
  self.assertEqual(self.action_rows(),[]);self.assertEqual(self.calls,[[],[]])
 async def test_original_control_disconnect_denies_action(self):
  _,control=await self.prepare_a();await control._client.channel.close()
  a=self.action_servers[0].authority
  with self.assertRaises(Exception):await a.admit(a.scope,'start')
  self.assertEqual(self.action_rows(),[(0,'control-bound')]);self.assertEqual(self.calls,[[],[]])
 async def test_control_scope_replacement_denies_action(self):
  _,control=await self.prepare_a();control._descriptor=replace(control.descriptor,charge=control.descriptor.charge+1)
  a=self.action_servers[0].authority
  with self.assertRaises(Exception):await a.admit(a.scope,'start')
  self.assertEqual(self.action_rows(),[(0,'control-bound')]);self.assertEqual(self.calls,[[],[]])
 async def test_central_resource_denial_blocks_start(self):
  _,control=await self.prepare_a();self.central_resources.fail=True
  receipt=await control.dispatch();self.assertEqual(receipt['state'],'UNKNOWN')
  self.assertEqual(self.action_rows(),[(0,'control-bound')]);self.assertEqual(self.calls,[[],[]])
 async def test_revoke_before_start_blocks_real_control_body(self):
  _,control=await self.prepare_a();await self.adm.revoke(self.token,self.task.task_id)
  self.assertEqual((await control.dispatch())['state'],'UNKNOWN');self.assertEqual(self.calls,[[],[]])
  self.assertEqual(self.action_rows(),[(0,'control-bound')])
 async def test_unknown_mapping_commit_not_published_or_rebound(self):
  _,control,ch=await self.prepare_a(bind=False);old=self.adm._commit;hit=[]
  def lost(db):old(db);hit.append(1);raise OSError('LAB commit acknowledgement lost')
  with patch.object(self.adm,'_commit',lost):
   with self.assertRaises(Exception):await self.bind_server(0,control,ch)
  self.assertTrue(hit);self.assertTrue(self.adm.retired);self.assertFalse(self.adm._control_bindings)
  self.assertEqual(self.action_rows(),[(0,'control-bound')])
  with self.assertRaises(Exception):await self.bind_server(0,control,ch)
  self.assertEqual(self.calls,[[],[]])
 async def test_mapping_parent_exit_failure_never_publishes(self):
  _,control,ch=await self.prepare_a(bind=False);old=self.adm._policy_scope;hit=[]
  @contextmanager
  def fail(body,action):
   with old(body,action):yield
   if action=='bind-control':hit.append(1);raise OSError('LAB policy exit failed')
  with patch.object(self.adm,'_policy_scope',fail):
   with self.assertRaises(Exception):await self.bind_server(0,control,ch)
  self.assertEqual(hit,[1]);self.assertFalse(self.adm._control_bindings);self.assertTrue(self.adm.retired)
  self.assertEqual(self.action_rows(),[(0,'control-bound')]);self.assertEqual(self.calls,[[],[]])
 async def test_revoke_during_authenticated_prepare_no_lock_or_publication(self):
  _,control,ch=await self.prepare_a(bind=False);entered=asyncio.Event();release=asyncio.Event();old=CoordinatorControl.prepare
  async def pause(ctrl):
   self.assertFalse(self.consent.authority._lock.locked());self.assertEqual(self.central_resources.active,0)
   entered.set();await release.wait();return await old(ctrl)
  with patch.object(CoordinatorControl,'prepare',pause):
   work=self.spawn_job(self.bind_server(0,control,ch));await asyncio.wait_for(entered.wait(),3)
   await self.adm.revoke(self.token,self.task.task_id);release.set()
   with self.assertRaises(Exception):await work
  self.assertFalse(self.adm._control_bindings);self.assertEqual(self.action_rows(),[]);self.assertEqual(self.calls,[[],[]])
 async def test_cancel_waiting_prepare_burns_attempt(self):
  _,control,ch=await self.prepare_a(bind=False);entered=asyncio.Event();release=asyncio.Event();old=CoordinatorControl.prepare
  async def pause(ctrl):entered.set();await release.wait();return await old(ctrl)
  with patch.object(CoordinatorControl,'prepare',pause):
   work=self.spawn_job(self.bind_server(0,control,ch));await asyncio.wait_for(entered.wait(),3);work.cancel()
   with self.assertRaises(asyncio.CancelledError):await work
  with self.assertRaises(Exception):await self.bind_server(0,control,ch)
  self.assertTrue(self.adm.retired);self.assertEqual(self.action_rows(),[]);self.assertEqual(self.calls,[[],[]])
 async def test_unknown_start_commit_never_invokes_or_retries(self):
  _,control=await self.prepare_a();old=self.adm._commit;hit=[]
  def lost(db):old(db);hit.append(1);raise OSError('LAB START acknowledgement lost')
  with patch.object(self.adm,'_commit',lost):receipt=await control.dispatch()
  self.assertTrue(hit);self.assertEqual(receipt['state'],'UNKNOWN');self.assertEqual(self.calls,[[],[]])
  self.assertEqual(self.action_rows(),[(0,'control-bound'),(0,'start')])
  with self.assertRaises(Exception):await control.dispatch() # Original launcher sealed; no recovery/retry.
  self.assertEqual(self.calls,[[],[]])
 async def test_duplicate_start_fails_with_single_record(self):
  await self.prepare_a();a=self.action_servers[0].authority;await a.admit(a.scope,'start')
  with self.assertRaises(Exception):await a.admit(a.scope,'start')
  self.assertEqual(self.action_rows(),[(0,'control-bound'),(0,'start')]);self.assertTrue(self.adm.retired)
 async def test_no_authority_resource_lock_across_wire(self):
  old=wire.Writer.drain;checks=[]
  async def drain(writer):
   self.assertFalse(self.consent.authority._lock.locked());self.assertEqual(self.central_resources.active,0);checks.append(1)
   return await old(writer)
  with patch.object(wire.Writer,'drain',drain):await self.run_chain()
  self.assertGreater(len(checks),10)

 async def test_b_actual_input_proof_wrong_resource_refuses(self):
  old=CoordinatorControl.input_ready;hit=[]
  async def corrupt(ctrl):
   reply=await old(ctrl);hit.append(1);reply['input_ready']['resource_id']='wrong-resource';return reply
  with patch.object(CoordinatorControl,'input_ready',corrupt):
   with self.assertRaises(Exception):await self.run_chain()
  self.assertEqual(hit,[1]);self.assertEqual(self.calls[1],[])
  self.assertEqual(self.action_rows(),[(0,'commit'),(0,'control-bound'),(0,'start')]);self.assertTrue(self.adm.retired)
 async def test_revoke_while_ordinary_body_running_blocks_commit_not_stop(self):
  import threading
  entered=asyncio.Event();release=threading.Event();old=compute.SyntheticBackend.operation;loop=asyncio.get_running_loop();hit=[]
  def pause(backend,*args,**kw):
   hit.append(1);loop.call_soon_threadsafe(entered.set)
   if not release.wait(5):raise RuntimeError('LAB barrier safety timeout')
   return old(backend,*args,**kw)
  _,control=await self.prepare_a()
  with patch.object(compute.SyntheticBackend,'operation',pause):
   work=self.spawn_job(control.dispatch())
   try:
    await asyncio.wait_for(entered.wait(),3);await self.adm.revoke(self.token,self.task.task_id)
    self.assertFalse(work.done());release.set();receipt=await work
   finally:release.set()
  self.assertEqual(hit,[1]);self.assertEqual(receipt['state'],'UNKNOWN')
  self.assertTrue(any(x[0]=='stage0' for x in self.calls[0])) # Revocation did NOT physically stop the thread.
  self.assertEqual(self.action_rows(),[(0,'control-bound'),(0,'start')]);self.assertEqual(self.calls[1],[])
 async def test_reopen_history_cannot_restore_mapping(self):
  await self.prepare_a();self.adm.close()
  from private_experiment_admission import ExperimentAdmission
  reopened=ExperimentAdmission(self.root/'web-experiments.db',self.consent,source_domain='a'*64,trigger=self.web.trigger,policy_authority=self.parent,installation_authority=self.install_policy);self.addCleanup(reopened.close)
  self.assertFalse(reopened.fresh)
  with self.assertRaises(Exception):reopened._ready()
  self.assertEqual(reopened.db.execute('SELECT stage,action FROM short_actions').fetchall(),[(0,'control-bound')])
 async def test_replaced_authority_is_not_accepted(self):
  await self.prepare_a()
  original=self.action_servers[0].authority;adapter=original.authorize
  context=original.context(self.body,'start',None)
  class Replacement:
   def current(self):return original.current()
   def context(self,*args):return original.context(*args)
  foreign=Replacement();adapter.authority=foreign
  self.assertIsNot(adapter.authority,original)
  entered=[]
  try:
   with adapter.hold(context,'start'):entered.append(True)
  except Exception:pass
  self.assertEqual(entered,[], 'replacement authority accepted by pinned adapter')
  self.assertTrue(adapter.dead);self.assertTrue(self.adm.retired)
  adapter.authority=original
  with self.assertRaises(Exception):
   with adapter.hold(context,'start'):pass

def load_tests(loader,tests,pattern):return unittest.TestSuite(cls(n) for cls in (Entry,Chain) for n in cls.__dict__ if n.startswith('test_'))
if __name__=='__main__':unittest.main()
