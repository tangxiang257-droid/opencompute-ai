"""LAB signed identity, real single-token source/issuer/SQLite; synthetic policy/confirmation."""
import asyncio,copy,json,unittest
from contextlib import contextmanager
from unittest.mock import patch
from validation.task_consent.test_consent import Tests as Base
from validation.input_declaration import test_declaration as source_fixture
from validation.authority_resources.test_source import Permission
from cluster_p1.online_issuance import IssuanceAuthority,_make_declaration
from private_experiment_admission import ExperimentAdmission,Confirmation,PURPOSE,digest
class Trigger:
 async def confirm(self,credential,receipt,body):return Confirmation(receipt.proposal_id,receipt.proposal_digest,body['owner'])
class Policy:
 def __init__(self):self.fail=None;self.calls=[]
 @contextmanager
 def hold(self,body,action):
  self.calls.append(action);yield None
  if self.fail==action:raise RuntimeError('scope exit uncertain')
class Tests(Base):
 async def asyncSetUp(self):
  publish=source_fixture.snapshots.publish_input
  with patch.object(source_fixture.snapshots,'publish_input',side_effect=lambda fd,b,t:publish(fd,b,(1,))):await super().asyncSetUp()
  d=copy.deepcopy(self.declaration);d['session']['expected_tokens']=1
  for tile in d['plan']['tiles']:
   tile['token_count']=1;tile['input']['shape'][1]=1;tile['output']['shape'][1]=1
  self.policy['privacy']='L1';d['task'].update(privacy_level='L1',cost_cap=self.policy['max_cost_microunits']/1000000,deadline=self.policy['expires_at'])
  self.declaration=_make_declaration(d['task'],d['session'],d['stages'],d['plan']['tiles'],24,100,d['config_sha256'])
  self.parent=Policy();self.trigger=Trigger();self.adm=ExperimentAdmission(self.root/'experiment.db',self.consent,source_domain='a'*64,trigger=self.trigger,policy_authority=self.parent);self.addCleanup(self.adm.close)
  self.receiver=dict(node='c',identity='c-cert',purpose='result-store',storage_policy='private',retention_seconds=60,max_charge_bytes=65536)
  self.copy=dict(allowed=True,destination='c',purpose='result-store',input_snapshot=self.ref.digest)
  self.strict=IssuanceAuthority(self.root/'experiment-issuer.db',namespace='b'*64,source_provider=self.adm,authorize=self.adm,resources=Permission(),require_consent=True);self.addCleanup(self.strict.close)
 async def prepare_new(self):return await self.adm.prepare(self.token,self.task.task_id,self.declaration,self.policy,self.placements,receiver=self.receiver,c_input_copy=self.copy,issuer=self.strict)
 async def confirmed(self):
  r=await self.prepare_new();return await self.adm.confirm(self.token,r.proposal_id,r.proposal_digest)
 async def dispatch(self,r):return await self.adm.dispatch(self.token,r.proposal_id,r.proposal_digest,self.strict)
 async def test_actual_source_confirmation_to_actual_issuer_inert(self):
  before=list(self.ledger._db.iterdump());r=await self.confirmed();done=await self.dispatch(r)
  self.assertEqual(done.state,'REGISTERED');self.assertFalse(done.execution_allowed)
  row=self.strict.db.execute('SELECT task,source_id,body FROM registrations').fetchone();self.assertNotEqual(row[0],self.task.task_id);self.assertEqual(row[1],digest(['a'*64,self.owner,self.task.task_id,PURPOSE]))
  context=json.loads(self.strict.db.execute('SELECT context FROM authorizations').fetchone()[0]);self.assertEqual(context['body']['source_task'],self.task.task_id);self.assertEqual(context['body']['c_input_copy'],self.copy)
  self.assertEqual(before,list(self.ledger._db.iterdump()))
  with self.assertRaises(Exception):self.ledger.bind_plan(self.owner,self.task.task_id,b'{}')
  with self.assertRaises(Exception):
   with self.adm.hold({},'claim'):pass
 async def test_no_confirmation_no_dispatch(self):
  r=await self.prepare_new()
  with self.assertRaises(Exception):await self.dispatch(r)
  self.assertEqual(self.strict.db.execute('SELECT count(*) FROM registrations').fetchone(),(0,))
 async def test_missing_real_policy_default_denies(self):
  a=ExperimentAdmission(self.root/'missing.db',self.consent,source_domain='a'*64,trigger=None,policy_authority=None);self.addCleanup(a.close)
  with self.assertRaises(Exception):await a.prepare(self.token,self.task.task_id,self.declaration,self.policy,self.placements,receiver=self.receiver,c_input_copy=self.copy,issuer=self.strict)
 async def test_C_copy_requires_explicit_exact_context(self):
  for change in [dict(allowed=False),dict(destination='elsewhere'),dict(input_snapshot='f'*64)]:
   self.copy.update(change)
   with self.assertRaises(Exception):await self.prepare_new()
   self.copy=dict(allowed=True,destination='c',purpose='result-store',input_snapshot=self.ref.digest)
  self.assertEqual(self.adm.db.execute('SELECT count(*) FROM experiments').fetchone(),(0,))
 async def test_policy_privacy_and_cost_cannot_disagree(self):
  self.policy['privacy']='L2'
  with self.assertRaises(Exception):await self.prepare_new()
  self.policy['privacy']='L1';self.policy['max_cost_microunits']+=1
  with self.assertRaises(Exception):await self.prepare_new()
 async def test_revoke_before_proposal_tombstone(self):
  await self.adm.revoke(self.token,self.task.task_id)
  with self.assertRaises(Exception):await self.prepare_new()
  self.assertEqual(self.adm.db.execute('SELECT count(*) FROM experiments').fetchone(),(0,))
 async def test_revoke_while_confirmation_waits(self):
  entered=asyncio.Event();release=asyncio.Event();original=self.adm._confirm
  async def confirm(*args):entered.set();await release.wait();return await original(*args)
  self.trigger.confirm=confirm;self.adm._confirm=confirm
  r=await self.prepare_new();task=asyncio.create_task(self.adm.confirm(self.token,r.proposal_id,r.proposal_digest));await entered.wait()
  self.assertFalse(self.a._lock.locked());await self.adm.revoke(self.token,self.task.task_id);release.set()
  with self.assertRaises(Exception):await task
  self.assertNotIn(r.proposal_id,self.adm.published)
 async def test_confirm_scope_exit_failure_no_eligibility(self):
  r=await self.prepare_new();self.parent.fail='confirm'
  with self.assertRaises(Exception):await self.adm.confirm(self.token,r.proposal_id,r.proposal_digest)
  self.assertNotIn(r.proposal_id,self.adm.published);self.assertTrue(self.adm.retired)
  self.parent.fail=None
  with self.assertRaises(Exception):await self.dispatch(r)
 async def test_confirm_commit_unknown_no_recovery(self):
  r=await self.prepare_new();original=self.adm._commit;count=0
  def commit(db):
   nonlocal count
   original(db);count+=1
   if count==2:raise OSError('after CONFIRMED commit')
  with patch.object(self.adm,'_commit',side_effect=commit):
   with self.assertRaises(Exception):await self.adm.confirm(self.token,r.proposal_id,r.proposal_digest)
  self.assertTrue(self.adm.retired);self.assertNotIn(r.proposal_id,self.adm.published)
 async def test_dispatch_commit_unknown_before_issuer_never_retries(self):
  r=await self.confirmed();original=self.adm._commit
  def commit(db):original(db);raise OSError('unknown')
  with patch.object(self.adm,'_commit',side_effect=commit):
   with self.assertRaises(Exception):await self.dispatch(r)
  self.assertEqual(self.strict.db.execute('SELECT count(*) FROM registrations').fetchone(),(0,))
  with self.assertRaises(Exception):await self.dispatch(r)
 async def test_issuer_commit_unknown_retains_one_registration(self):
  r=await self.confirmed();original=self.strict._commit
  def commit(db):original(db);raise OSError('reply lost')
  with patch.object(self.strict,'_commit',side_effect=commit):
   with self.assertRaises(Exception):await self.dispatch(r)
  self.assertEqual(self.strict.db.execute('SELECT count(*) FROM registrations').fetchone(),(1,));self.assertTrue(self.strict.retired)
  with self.assertRaises(Exception):await self.dispatch(r)
 async def test_register_permission_exit_failure_burns_both(self):
  r=await self.confirmed();self.parent.fail='register'
  with self.assertRaises(Exception):await self.dispatch(r)
  self.assertTrue(self.adm.retired and self.strict.retired);self.assertEqual(self.strict.db.execute('SELECT count(*) FROM registrations').fetchone(),(1,))
 async def test_two_dispatches_only_one_registration(self):
  r=await self.confirmed();values=await asyncio.gather(self.dispatch(r),self.dispatch(r),return_exceptions=True)
  self.assertEqual(sum(not isinstance(x,BaseException) for x in values),1);self.assertEqual(self.strict.db.execute('SELECT count(*) FROM registrations').fetchone(),(1,))
 async def test_restart_history_only(self):
  r=await self.confirmed();self.adm.close();other=ExperimentAdmission(self.root/'experiment.db',self.consent,source_domain='a'*64,trigger=self.trigger,policy_authority=self.parent);self.addCleanup(other.close)
  self.assertEqual((await other.history(self.token,r.proposal_id)).state,'CONFIRMED');self.assertFalse(other.published)
  with self.assertRaises(Exception):await other.dispatch(self.token,r.proposal_id,r.proposal_digest,self.strict)
 async def test_wrong_confirmation_action_refuses(self):
  async def wrong(credential,r,body):return Confirmation(r.proposal_id,r.proposal_digest,body['owner'],'confirm-task-execution')
  self.trigger.confirm=wrong;self.adm._confirm=wrong;r=await self.prepare_new()
  with self.assertRaises(Exception):await self.adm.confirm(self.token,r.proposal_id,r.proposal_digest)
 async def test_context_hash_tampering_refuses(self):
  r=await self.prepare_new()
  with self.assertRaises(Exception):await self.adm.confirm(self.token,r.proposal_id,'f'*64)
 async def test_source_cancel_after_confirm_refuses(self):
  r=await self.confirmed();await self.a.cancel(self.owner,self.task.task_id)
  with self.assertRaises(Exception):await self.dispatch(r)
 async def test_snapshot_corruption_refuses(self):
  import os
  r=await self.confirmed();p=self.root/'inputs'/(self.ref.digest+'.tokens');os.chmod(p,0o600);p.write_bytes(b'bad')
  with self.assertRaises(Exception):await self.dispatch(r)
 async def test_provider_substitution_refuses(self):
  r=await self.confirmed();self.adm.policy=Policy()
  with self.assertRaises(Exception):await self.dispatch(r)
 async def test_final_USER_exit_failure_does_not_publish(self):
  r=await self.prepare_new();original=self.consent._user;calls=0
  @contextmanager
  def user(*args):
   nonlocal calls
   calls+=1
   with original(*args):yield
   if calls==2:
    self.assertEqual(self.adm._row(self.owner,r.proposal_id)[3],'CONFIRMED')
    self.assertNotIn(r.proposal_id,self.adm.published)
    raise RuntimeError('USER exit failure')
  with patch.object(self.consent,'_user',side_effect=user):
   with self.assertRaises(Exception):await self.adm.confirm(self.token,r.proposal_id,r.proposal_digest)
  self.assertTrue(self.adm.retired);self.assertFalse(self.adm.published)
 async def test_swallowed_cancel_cannot_publish(self):
  entered=asyncio.Event()
  async def trigger(credential,r,body):
   entered.set()
   try:await asyncio.Event().wait()
   except asyncio.CancelledError:return Confirmation(r.proposal_id,r.proposal_digest,body['owner'])
  self.trigger.confirm=trigger;self.adm._confirm=trigger;r=await self.prepare_new()
  task=asyncio.create_task(self.adm.confirm(self.token,r.proposal_id,r.proposal_digest));await entered.wait();task.cancel()
  with self.assertRaises(Exception):await task
  self.assertFalse(self.adm.published)
 async def test_same_source_other_admission_same_issuer_cannot_duplicate(self):
  r=await self.confirmed();await self.dispatch(r)
  other=ExperimentAdmission(self.root/'other.db',self.consent,source_domain='a'*64,trigger=self.trigger,policy_authority=self.parent);self.addCleanup(other.close)
  self.strict.source_provider=other;self.strict.authorize=other
  q=await other.prepare(self.token,self.task.task_id,self.declaration,self.policy,self.placements,receiver=self.receiver,c_input_copy=self.copy,issuer=self.strict)
  await other.confirm(self.token,q.proposal_id,q.proposal_digest)
  self.strict.source_provider=other;self.strict.authorize=other
  with self.assertRaises(Exception):await other.dispatch(self.token,q.proposal_id,q.proposal_digest,self.strict)
  self.assertEqual(self.strict.db.execute('SELECT count(*) FROM registrations').fetchone(),(1,))
 async def test_observed_provider_drift_permanently_retires(self):
  r=await self.confirmed();original=self.adm.trigger;self.adm.trigger=Trigger()
  with self.assertRaises(Exception):await self.dispatch(r)
  self.adm.trigger=original
  with self.assertRaises(Exception):await self.dispatch(r)
  self.assertTrue(self.adm.retired);self.assertFalse(self.adm.published)
 async def test_observed_policy_method_drift_permanently_retires(self):
  r=await self.confirmed();original=self.parent.hold;self.parent.hold=Permission().hold
  with self.assertRaises(Exception):await self.dispatch(r)
  self.parent.hold=original
  with self.assertRaises(Exception):await self.dispatch(r)
 async def test_old_task_revoke_invalidates_new_confirmation(self):
  r=await self.confirmed();await self.consent.revoke(self.token,self.task.task_id)
  with self.assertRaises(Exception):await self.dispatch(r)
  self.assertEqual(self.strict.db.execute('SELECT count(*) FROM registrations').fetchone(),(0,))
 async def test_unconfirmed_issuer_cannot_replace_target(self):
  r=await self.confirmed();other=IssuanceAuthority(self.root/'wrong-issuer.db',namespace='c'*64,source_provider=self.adm,authorize=self.adm,resources=Permission(),require_consent=True);self.addCleanup(other.close)
  with self.assertRaises(Exception):await self.adm.dispatch(self.token,r.proposal_id,r.proposal_digest,other)
  self.assertEqual(other.db.execute('SELECT count(*) FROM registrations').fetchone(),(0,))
def load_tests(loader,tests,pattern):return unittest.TestSuite(Tests(n) for n in Tests.__dict__ if n.startswith('test_'))
if __name__=='__main__':unittest.main()
