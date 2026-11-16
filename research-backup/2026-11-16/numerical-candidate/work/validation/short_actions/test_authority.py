"""LAB identity/policy/clock; actual source, confirmation and durable cancel domain."""
import asyncio,copy,json,unittest
from unittest.mock import patch
from validation.experiment_admission.test_admission import Tests as Base
from validation.experiment_installation.test_installation import Tests as InstalledBase
from validation.control_exchange.test_transport import Holds
from cluster_p1.online_issuance import _make_declaration
from private_experiment_actions import ActionAuthority,ActionServer,ActionGate
from cluster_p1.control_exchange import ControlClient,ControlService
from cluster_p1.control_journal import ControlJournal
from cluster_p1.projection_driver import Spec
from cluster_p1.channel import Channel,Grant
from validation.role_bridge.test_transport import streams,config
import hashlib,time
import private_experiment_admission as admission

class Tests(Base):
 async def test_short_action_profile_is_explicit_not_legacy(self):
  self.assertTrue(hasattr(admission,'SHORT_ACTION_PROFILE'),'explicit experiment profile missing')
  r=await self.confirmed();await self.dispatch(r)
  from private_experiment_actions import ActionAuthority
  with self.assertRaises(Exception):ActionAuthority(self.adm,self.token,r.proposal_id,r.proposal_digest)

class Clock:
 def __init__(self):self.wall=time.time();self.mono=time.monotonic();self.error=0.1
 def sample(self):return self.wall,self.mono,self.error
 def advance(self,n):self.wall+=n;self.mono+=n

class Wired(InstalledBase):
 async def prepare_new(self):
  return await self.adm.prepare(self.token,self.task.task_id,self.declaration,self.policy,self.placements,receiver=self.receiver,c_input_copy=self.copy,issuer=self.strict,runtime_profile=admission.SHORT_ACTION_PROFILE)
 async def asyncSetUp(self):
  await super().asyncSetUp()
  result,self.install_store=await self.exchange(0)
  for v in result:
   if isinstance(v,BaseException):raise v
  b=json.loads(self.adm._row(self.owner,self.receipt.proposal_id)[0]);self.body=b;d=b['declaration'];s=d['session'];p=b['placements'][0]
  self.spec=Spec(b['owner'],b['execution_task'],s['session_id'],1,s['model_hash'],s['layout_hash'],b['source']['input_snapshot'],b['source']['preprocessing'],d['plan_digest'],b['source']['token_digest'],24,100,(b['source']['token_digest'],))
  async def pair(left,right):
   cr,cw,sr,sw=streams(left,right);g=Grant(b['execution_task'],s['session_id'],1,0,(0,1),s['model_hash'],2)
   c=Channel(cr,cw,config(right),{right:{g}},capacity_bytes=32768,capacity_tiles=1);r=Channel(sr,sw,config(left),{left:{g}},capacity_bytes=32768,capacity_tiles=1)
   self.channels.extend((c,r));await asyncio.gather(c.start(),r.start());return c,r
  cc,sc=await pair('coordinator','a');self.permissions=Holds();self.clock=Clock()
  self.journal=ControlJournal(self.root/'actions-control',1024,resources=Holds());self.addCleanup(self.journal.close)
  self.ctrl=ControlService(sc,self.journal,stage=0,node='a',coordinator_node='coordinator',device=p['device'],instance=p['instance'],identity=Holds(),authorize=Holds());self.client=ControlClient(cc)
  self.ctrl.check(self.spec,admission.digest(d));self.calls=[]
  self.ctrl.bind(self.spec,lambda:self.calls.append(1),'control-once','reservation-once',64)
  self.scope=dict(command='control-once',reservation='reservation-once',binding=self.ctrl._call[2],charge=64)
  self.auth=ActionAuthority(self.adm,self.token,self.receipt.proposal_id,self.receipt.proposal_digest,stage=0,scope=self.scope,authorize=self.permissions,clock=self.clock)
  pc,ps=await pair('a','issuer')
  self.server=ActionServer(ps,self.auth,identity=Holds());self.gate=ActionGate(pc,authority_node='issuer',authority_certificate=hashlib.sha256(b'issuer').hexdigest(),identity=Holds(),clock=self.clock);self.gate.attach(self.ctrl)
 async def control(self,action):
  c,r,b,q=self.ctrl._call
  values=await asyncio.gather(self.ctrl.serve_once(),self.client.request(action,c,r,b,q),return_exceptions=True)
  for x in values:
   if isinstance(x,BaseException):raise x
  return values[1]
 async def execute(self,count=2):
  await self.control('prepare')
  async def serve():
   for _ in range(count):await self.server.serve_once()
  task=asyncio.create_task(serve())
  try:return await self.control('execute')
  finally:
   if not task.done():task.cancel()
   await asyncio.gather(task,return_exceptions=True)
 async def test_original_confirm_install_action_wire_control_complete(self):
  r=await self.execute();self.assertEqual(r['state'],'COMMITTED');self.assertEqual(self.calls,[1])
  rows=self.adm.db.execute('SELECT action,scope FROM short_actions ORDER BY action').fetchall();self.assertEqual([x[0] for x in rows],['commit','start'])
  self.assertEqual(json.loads(rows[0][1])['output_digest'],r['output_digest'])
  self.assertEqual((await self.control('query'))['state'],'COMMITTED');self.assertEqual(self.calls,[1])
 async def test_cancel_before_start_denies_body(self):
  await self.adm.revoke(self.token,self.task.task_id);r=await self.execute(1)
  self.assertEqual(r['state'],'UNKNOWN');self.assertEqual(self.calls,[])
 async def test_cancel_during_body_denies_independent_commit(self):
  entered=asyncio.Event();release=asyncio.Event()
  async def body():self.calls.append(1);entered.set();await release.wait();return None
  self.ctrl._operation=body;self.gate._service_pin=(*self.gate._service_pin[:-1],body)
  work=asyncio.create_task(self.execute());await asyncio.wait_for(entered.wait(),3)
  await self.adm.revoke(self.token,self.task.task_id);release.set();r=await work
  self.assertEqual(r['state'],'UNKNOWN');self.assertEqual(self.calls,[1]);self.assertEqual(self.adm.db.execute('SELECT action FROM short_actions').fetchall(),[('start',)])
 async def test_duplicate_start_cannot_be_reissued(self):
  await self.auth.admit(self.scope,'start')
  with self.assertRaises(Exception):await self.auth.admit(self.scope,'start')
  self.assertTrue(self.adm.retired);self.assertEqual(self.adm.db.execute('SELECT count(*) FROM short_actions').fetchone(),(1,))
 async def test_commit_without_start_refuses(self):
  with self.assertRaises(Exception):await self.auth.admit(self.scope,'commit','a'*64)
 async def test_unknown_commit_no_reissue(self):
  old=self.adm._commit
  def lost(db):old(db);raise OSError('LAB committed acknowledgement lost')
  with patch.object(self.adm,'_commit',lost):
   with self.assertRaises(Exception):await self.auth.admit(self.scope,'start')
  self.assertTrue(self.adm.retired);self.assertTrue(self.auth.dead)
  self.assertEqual(self.adm.db.execute('SELECT count(*) FROM short_actions').fetchone(),(1,))
 async def test_clock_error_or_rollback_denies(self):
  self.clock.error=0.3
  with self.assertRaises(Exception):await self.auth.admit(self.scope,'start')
 async def test_resource_or_task_parent_denial_blocks_start(self):
  self.permissions.fail=True;r=await self.execute(1)
  self.assertEqual(r['state'],'UNKNOWN');self.assertEqual(self.calls,[])
 async def test_changed_input_scope_denies(self):
  bad=copy.deepcopy(self.scope);bad['binding']['input_digest']='f'*64
  with self.assertRaises(Exception):await self.auth.admit(bad,'start')
 async def test_missing_clock_or_authority_denies(self):
  for changes in ({'clock':None},{'authorize':None}):
   kw=dict(stage=0,scope=self.scope,clock=self.clock,authorize=self.permissions);kw.update(changes)
   with self.assertRaises(Exception):ActionAuthority(self.adm,self.token,self.receipt.proposal_id,self.receipt.proposal_digest,**kw)
 async def test_expired_role_permit_denies(self):
  await asyncio.gather(self.server.serve_once(),self.gate.prepare('start'));self.clock.advance(4)
  with self.assertRaises(Exception):
   with self.gate.fence('start'):self.calls.append(1)
  self.assertEqual(self.calls,[])
 async def test_no_local_authority_lock_across_wire_wait(self):
  from validation.role_bridge.test_transport import Writer
  old=Writer.drain
  async def drain(writer):
   self.assertFalse(self.consent.authority._lock.locked());self.assertEqual(self.permissions.active,0);return await old(writer)
  with patch.object(Writer,'drain',drain):self.assertEqual((await self.execute())['state'],'COMMITTED')
 async def test_revoke_after_commit_admission_does_not_retract_original_once(self):
  await asyncio.gather(self.server.serve_once(),self.gate.prepare('start'))
  with self.gate.fence('start'):pass
  await asyncio.gather(self.server.serve_once(),self.gate.prepare('commit','a'*64))
  await self.adm.revoke(self.token,self.task.task_id)
  # Central COMMIT won first. Original connected role may finish that action.
  with self.gate.fence('commit'):self.calls.append('committed')
  with self.assertRaises(Exception):
   with self.gate.fence('commit'):self.calls.append('replayed')
  self.assertEqual(self.calls,['committed'])
 async def test_disconnect_after_admission_denies_start(self):
  await asyncio.gather(self.server.serve_once(),self.gate.prepare('start'));await self.gate.channel.close()
  with self.assertRaises(Exception):
   with self.gate.fence('start'):self.calls.append(1)
  self.assertEqual(self.calls,[])
 async def test_original_install_connection_closed_denies(self):
  await self.auth.v['service'].channel.close()
  with self.assertRaises(Exception):await self.auth.admit(self.scope,'start')
 async def test_role_wall_rollback_denies(self):
  await asyncio.gather(self.server.serve_once(),self.gate.prepare('start'));self.clock.wall-=1
  with self.assertRaises(Exception):
   with self.gate.fence('start'):self.calls.append(1)
 async def test_waiting_start_cancellation_burns_local_action(self):
  entered=asyncio.Event();original=self.gate.channel.receive
  async def receive():entered.set();return await original()
  with patch.object(self.gate.channel,'receive',receive):
   work=asyncio.create_task(self.gate.prepare('start'));await asyncio.wait_for(entered.wait(),3);work.cancel()
   with self.assertRaises(asyncio.CancelledError):await work
  with self.assertRaises(Exception):await self.gate.prepare('start')
  self.assertTrue(self.gate.dead);self.assertEqual(self.calls,[])
 async def test_absent_authority_response_times_out_without_retry(self):
  with self.assertRaises(TimeoutError):await self.gate.prepare('start')
  self.assertTrue(self.gate.dead);self.assertEqual(self.calls,[])
 async def test_old_credential_profile_cannot_gain_actions(self):
  # A separate actual confirmed legacy record is covered by the base test;
  # mutation of the signed/confirmed body cannot silently change it here.
  self.auth.v['body']['runtime_profile']['online_only']=False
  with self.assertRaises(Exception):await self.auth.admit(self.scope,'start')
 async def test_parent_exit_failure_retains_start_and_poison(self):
  from contextlib import contextmanager
  old=self.permissions.hold
  @contextmanager
  def fail(*args):
   with old(*args):yield None
   raise OSError('LAB parent exit uncertain')
  # Install the configured callback before pinning a new unconsumed authority.
  self.permissions.hold=fail;self.auth._pin=(self.permissions,fail,self.clock,self.clock.sample)
  with self.assertRaises(Exception):await self.auth.admit(self.scope,'start')
  self.assertTrue(self.adm.retired);self.assertEqual(self.adm.db.execute('SELECT action FROM short_actions').fetchall(),[('start',)])
 async def test_reopen_cannot_restore_action_eligibility(self):
  await self.auth.admit(self.scope,'start');self.adm.close()
  a=admission.ExperimentAdmission(self.root/'installation-web',self.consent,source_domain='a'*64,trigger=self.web.trigger,policy_authority=self.parent,installation_authority=self.install_parent);self.addCleanup(a.close)
  with self.assertRaises(Exception):ActionAuthority(a,self.token,self.receipt.proposal_id,self.receipt.proposal_digest,stage=0,scope=self.scope,authorize=self.permissions,clock=self.clock)
  self.assertEqual(a.db.execute('SELECT count(*) FROM short_actions').fetchone(),(1,))

 async def test_identity_callback_expiry_blocks_body(self):
  from contextlib import contextmanager
  outer=self
  class Identity:
   enabled=False
   @contextmanager
   def hold(inner,*args):
    if inner.enabled:outer.clock.advance(4)
    yield None
  who=Identity();self.gate.identity=who;self.gate._pin=(who,who.hold,self.clock,self.clock.sample)
  await asyncio.gather(self.server.serve_once(),self.gate.prepare('start'));who.enabled=True;entered=False
  with self.assertRaises(Exception):
   with self.gate.fence('start'):entered=True
  self.assertFalse(entered)
 async def test_commit_cannot_follow_other_control_start(self):
  await self.auth.admit(self.scope,'start')
  other=copy.deepcopy(self.scope);other['command']='other-command';other['reservation']='other-reservation'
  auth=ActionAuthority(self.adm,self.token,self.receipt.proposal_id,self.receipt.proposal_digest,stage=0,scope=other,authorize=self.permissions,clock=self.clock)
  with self.assertRaises(Exception):await auth.admit(other,'commit','a'*64)
  self.assertEqual(self.adm.db.execute('SELECT action FROM short_actions').fetchall(),[('start',)])
 async def test_nested_commit_fence_cannot_reconsume(self):
  await asyncio.gather(self.server.serve_once(),self.gate.prepare('start'))
  with self.gate.fence('start'):pass
  await asyncio.gather(self.server.serve_once(),self.gate.prepare('commit','a'*64))
  entries=[]
  with self.assertRaises(Exception):
   with self.gate.fence('commit'):
    entries.append('outer')
    with self.gate.fence('commit'):entries.append('inner')
  self.assertEqual(entries,['outer'])
 async def test_authority_scope_exit_cannot_extend_admitted_deadline(self):
  from contextlib import contextmanager
  old=self.permissions.hold
  @contextmanager
  def slow(*args):
   with old(*args):yield None
   self.clock.advance(5)
  self.permissions.hold=slow;self.auth._pin=(self.permissions,slow,self.clock,self.clock.sample)
  with self.assertRaises(Exception):await self.auth.admit(self.scope,'start')
  self.assertEqual(self.adm.db.execute('SELECT action FROM short_actions').fetchall(),[('start',)])
  self.assertTrue(self.adm.retired)
 async def test_expired_reply_waiting_writer_lock_not_sent(self):
  entered=asyncio.Event();old=self.server.channel.send
  async def send(*args,**kwargs):
   await self.server.channel._write.acquire();entered.set();return await old(*args,**kwargs)
  before=len(self.server.channel.w.frames)
  with patch.object(self.server.channel,'send',send):
   server=asyncio.create_task(self.server.serve_once());client=asyncio.create_task(self.gate.prepare('start'))
   await asyncio.wait_for(entered.wait(),3);self.clock.advance(5);self.server.channel._write.release()
   with self.assertRaises(Exception):await server
   client.cancel();await asyncio.gather(client,return_exceptions=True)
  self.assertEqual(sum(x[:1]==b'D' for x in self.server.channel.w.frames[before:]),0)
 async def test_role_clock_rollback_between_actions_denies(self):
  local=Clock();self.gate.clock=local;self.gate._pin=(self.gate.identity,self.gate.identity.hold,local,local.sample);self.gate._last=local.sample()
  await asyncio.gather(self.server.serve_once(),self.gate.prepare('start'))
  local.advance(1)
  with self.gate.fence('start'):pass
  local.advance(-0.5)
  with self.assertRaises(Exception):await self.gate.prepare('commit','a'*64)
  self.assertIsNone(self.gate.permit);self.assertTrue(self.gate.dead)

def load_tests(loader,tests,pattern):return unittest.TestSuite(cls(n) for cls in (Tests,Wired) for n in cls.__dict__ if n.startswith('test_'))
if __name__=='__main__':unittest.main()
