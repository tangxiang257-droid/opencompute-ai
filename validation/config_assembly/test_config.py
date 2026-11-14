"""Explicit LAB providers and memory channels; no external execution."""
import importlib.util,unittest
class Entry(unittest.TestCase):
 def test_explicit_config_assembly_entry_exists(self):
  self.assertIsNotNone(importlib.util.find_spec('private_experiment_assembly'),'auditable configured assembly missing')

# Real fixture components; only external providers/streams remain LAB.
import asyncio,hashlib,json
from unittest.mock import patch
from types import SimpleNamespace
from dataclasses import asdict
from validation.authority_chain.test_chain import Chain
from validation.control_exchange.test_transport import Holds
from private_experiment_admission import digest
from cluster_p1.control_exchange import _json
from validation.short_actions.test_authority import Clock
class HookClock(Clock):
 def __init__(self):super().__init__();self.hook=None
 def sample(self):
  if self.hook:self.hook()
  return super().sample()
class Configured(Chain):
 def document(self,stage,control,ch):
  v=self.adm._installations[(self.receipt.proposal_id,stage)];d=control.descriptor
  return dict(version=1,purpose='central-action-assembly',experiment=self.receipt.proposal_id,context=self.receipt.proposal_digest,stage=stage,
   placement={k:v['descriptor'][k] for k in ('node','device','instance','resource_id')},charge=d.charge,
   control_sha256=digest(dict(command=d.command_id,reservation=d.reservation_id,binding=d.binding,charge=d.charge)),
   control_peer=asdict(control._client._identity),action_peer=asdict(ch.authenticated_identity),issuer=dict(v['service']._issuer_identity))
 async def configured(self,stage,control,ch,*,enabled=True,document=None,pin=None,**changes):
  from private_experiment_assembly import assemble_actions
  raw=_json(self.document(stage,control,ch) if document is None else document)
  kw=dict(identity=Holds(),clock=self.clock,resources=self.central_resources);kw.update(changes)
  return await assemble_actions(raw,pin or hashlib.sha256(raw).hexdigest(),self.adm,self.token,control,ch,enabled=enabled,**kw)
 async def bind_server(self,stage,control,ch,**changes):
  service=await self.configured(stage,control,ch,**changes);self.owned.append(service);return service
 async def test_configured_actual_A_B_C_result(self):await self.run_chain()
 async def test_default_disabled_and_bad_pin_do_not_take_channel(self):
  _,control,ch=await self.prepare_a(bind=False)
  from private_experiment_assembly import assemble_actions
  raw=_json(self.document(0,control,ch))
  with self.assertRaises(Exception):await assemble_actions(raw,hashlib.sha256(raw).hexdigest(),self.adm,self.token,control,ch,identity=Holds(),clock=self.clock,resources=self.central_resources)
  with self.assertRaises(Exception):await self.configured(0,control,ch,pin='0'*64)
  self.assertFalse(getattr(ch,'_experiment_action_owned',False));self.assertFalse(ch.w.is_closing());self.assertFalse(self.adm.retired);self.assertEqual(self.action_rows(),[])
 async def test_wrong_scope_or_placement_cannot_touch_borrowed(self):
  _,control,ch=await self.prepare_a(bind=False)
  for field in ('control_sha256','placement','issuer','action_peer'):
   d=self.document(0,control,ch)
   if field=='control_sha256':d[field]='0'*64
   else:d[field][next(iter(d[field]))]='wrong'
   with self.assertRaises(Exception):await self.configured(0,control,ch,document=d)
  self.assertFalse(ch.w.is_closing());self.assertFalse(self.adm.retired);self.assertEqual(self.action_rows(),[])
 async def test_missing_or_wrong_resource_provider_rejects_before_take(self):
  _,control,ch=await self.prepare_a(bind=False)
  for change in (dict(identity=None),dict(clock=None),dict(resources=None),dict(resources=Holds())):
   with self.assertRaises(Exception):await self.configured(0,control,ch,**change)
  self.assertFalse(ch.w.is_closing());self.assertFalse(self.adm.retired);self.assertEqual(self.action_rows(),[])
 async def test_original_control_channel_alias_refuses(self):
  _,control,ch=await self.prepare_a(bind=False)
  with self.assertRaises(Exception):await self.configured(0,control,control._client.channel)
  self.assertFalse(control._closed);self.assertFalse(self.adm.retired);self.assertEqual(self.action_rows(),[])
 async def test_assembly_does_not_start_serve_or_load(self):
  _,control,ch=await self.prepare_a(bind=False);s=await self.configured(0,control,ch);self.owned.append(s)
  self.assertIsNone(s.active_task);self.assertEqual(self.action_rows(),[(0,'control-bound')]);self.assertEqual([v.calls for v in self.loaders],[[],[]]);self.assertEqual(self.calls,[[],[]])
 async def test_parallel_serve_rejected_and_close_joins_owned_wait(self):
  from unittest.mock import patch
  _,control,ch=await self.prepare_a(bind=False);s=await self.configured(0,control,ch);entered=asyncio.Event();old=ch.receive
  async def receive():entered.set();return await old()
  with patch.object(ch,'receive',receive):
   job=self.spawn_job(s.serve_once());await asyncio.wait_for(entered.wait(),3)
   with self.assertRaises(Exception):await s.serve_once()
   await s.close()
  self.assertTrue(job.done());self.assertTrue(ch.w.is_closing());self.assertFalse(control._closed);self.assertFalse(self.adm.closed)
  self.assertEqual(self.calls,[[],[]]);self.assertTrue(s.closed)
 async def test_committed_binding_reply_loss_closes_only_taken_channel(self):
  from unittest.mock import patch
  _,control,ch=await self.prepare_a(bind=False);old=self.adm._commit;hit=[]
  def lost(db):old(db);hit.append(1);raise OSError('LAB acknowledgement lost')
  with patch.object(self.adm,'_commit',lost):
   with self.assertRaises(Exception):await self.configured(0,control,ch)
  self.assertTrue(hit);self.assertTrue(ch.w.is_closing());self.assertFalse(control._closed);self.assertTrue(self.adm.retired);self.assertEqual(self.action_rows(),[(0,'control-bound')]);self.assertEqual(self.calls,[[],[]])

 async def test_identity_callback_drift_during_prepare_is_rejected(self):
  from unittest.mock import patch
  from contextlib import contextmanager
  from cluster_p1.role_control import CoordinatorControl
  _,control,ch=await self.prepare_a(bind=False);identity=Holds();old_hold=identity.hold;old=CoordinatorControl.prepare;hit=[]
  @contextmanager
  def changed(*args):
   with old_hold(*args):yield None
  async def drift(ctrl):
   receipt=await old(ctrl);identity.hold=changed;hit.append(1);return receipt
  with patch.object(CoordinatorControl,'prepare',drift):
   with self.assertRaises(Exception):await self.configured(0,control,ch,identity=identity)
  self.assertEqual(hit,[1]);self.assertTrue(ch.w.is_closing());self.assertTrue(self.adm.retired)
 async def test_drifted_channel_cannot_redirect_cleanup(self):
  _,control,ch=await self.prepare_a(bind=False);s=await self.configured(0,control,ch)
  foreign=control._client.channel;s._channel=foreign
  with self.assertRaises(Exception):await s.serve_once()
  self.assertTrue(ch.w.is_closing());self.assertFalse(foreign.w.is_closing())
 async def test_close_error_still_joins_cancelled_owned_service(self):
  _,control,ch=await self.prepare_a(bind=False);s=await self.configured(0,control,ch)
  entered=asyncio.Event();cancelled=asyncio.Event();release=asyncio.Event();close_failed=asyncio.Event();old_receive=ch.receive;old_close=ch.close
  async def receive():
   entered.set()
   try:return await old_receive()
   except asyncio.CancelledError:
    cancelled.set();await release.wait();raise
  async def close():
   await old_close();close_failed.set();raise OSError('injected channel cleanup failure')
  try:
   with patch.object(ch,'receive',receive),patch.object(ch,'close',close):
    job=self.spawn_job(s.serve_once());await asyncio.wait_for(entered.wait(),3)
    closing=asyncio.create_task(s.close());await asyncio.wait_for(cancelled.wait(),3)
    await asyncio.wait_for(close_failed.wait(),3)
    checkpoint=asyncio.Event();asyncio.get_running_loop().call_soon(checkpoint.set);await checkpoint.wait()
  except OSError:pass
  finally:
   early=s.close_task.done() and not job.done()
   release.set();await asyncio.gather(job,closing,return_exceptions=True)
  self.assertFalse(early,'close completed with failure while owned serve still running')
 async def test_repeated_cancel_construction_retains_cleanup(self):
  from cluster_p1.role_control import CoordinatorControl
  _,control,ch=await self.prepare_a(bind=False)
  entered=asyncio.Event();close_entered=asyncio.Event();release=asyncio.Event();close_done=[];old_close=ch.close
  async def prepare(ctrl):entered.set();await asyncio.Event().wait()
  async def close():close_entered.set();await release.wait();await old_close();close_done.append(True)
  with patch.object(CoordinatorControl,'prepare',prepare),patch.object(ch,'close',close):
   work=asyncio.create_task(self.configured(0,control,ch))
   await asyncio.wait_for(entered.wait(),3);work.cancel();await asyncio.wait_for(close_entered.wait(),3)
   work.cancel()
   checkpoint=asyncio.Event();asyncio.get_running_loop().call_soon(checkpoint.set);await checkpoint.wait()
   release.set();await asyncio.gather(work,return_exceptions=True)
  self.assertEqual(close_done,[True],'second cancel abandoned accepted channel cleanup')
 async def test_close_does_not_touch_replacement_authority(self):
  _,control,ch=await self.prepare_a(bind=False);s=await self.configured(0,control,ch)
  original=s.server.authority;foreign=SimpleNamespace(dead=False);s.server.authority=foreign
  await s.close()
  self.assertFalse(foreign.dead,'cleanup retired a foreign authority')
  self.assertTrue(original.dead,'cleanup failed to retire original authority')
 async def test_clock_callback_cannot_change_provider_before_take(self):
  from contextlib import contextmanager
  _,control,ch=await self.prepare_a(bind=False);identity=Holds();old_hold=identity.hold;old_sample=self.clock.sample;hit=[]
  @contextmanager
  def changed(*args):
   with old_hold(*args):yield None
  def sample():identity.hold=changed;hit.append(1);return old_sample()
  with patch.object(self.clock,'sample',sample):
   with self.assertRaises(Exception):await self.configured(0,control,ch,identity=identity)
  self.assertTrue(hit);self.assertFalse(ch.w.is_closing());self.assertFalse(self.adm.retired);self.assertEqual(self.action_rows(),[])

 async def test_identity_provider_denial_prevents_publishing_assembly(self):
  _,control,ch=await self.prepare_a(bind=False);identity=Holds();identity.fail=True
  with self.assertRaises(Exception):await self.configured(0,control,ch,identity=identity)
  self.assertTrue(ch.w.is_closing());self.assertTrue(self.adm.retired);self.assertEqual(self.calls,[[],[]])

 async def test_check_callback_cannot_replace_invoked_server(self):
  self.clock=HookClock()
  _,control,ch=await self.prepare_a(bind=False);s=await self.configured(0,control,ch)
  entered=[];injected=[]
  async def foreign_body():entered.append(True)
  foreign=SimpleNamespace(authority=s.server.authority,serve_once=foreign_body)
  def swap():injected.append(True);s.server=foreign
  self.clock.hook=swap
  try:await s.serve_once()
  except Exception:pass
  finally:self.clock.hook=None;await s.close()
  self.assertTrue(injected)
  self.assertEqual(entered,[],'provider callback replaced the server actually invoked')

def load_tests(loader,tests,pattern):
 own=[cls(n) for cls in (Entry,Configured) for n in cls.__dict__ if n.startswith('test_')]
 # Re-run these existing behavioral scenarios through the configured entry.
 inherited=[Configured(n) for n in ('test_revoke_before_start_blocks_real_control_body','test_unknown_start_commit_never_invokes_or_retries','test_b_actual_input_proof_wrong_resource_refuses','test_cancel_waiting_prepare_burns_attempt')]
 return unittest.TestSuite(own+inherited)
if __name__=='__main__':unittest.main()
