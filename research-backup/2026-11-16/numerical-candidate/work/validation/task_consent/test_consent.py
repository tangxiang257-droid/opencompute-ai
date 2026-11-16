"""LAB signed USER + real fresh task/source/SQLite; synthetic explicit action."""
import asyncio,copy,json,time,unittest
from unittest.mock import patch
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding,PublicFormat
from validation.authority_resources import test_source as fixture
from validation.input_declaration import test_declaration as base
from validation.signed_identity.test_adapter import b64,DOMAIN
from private_signed_identity import SignedIdentityAdapter
from private_local_admission import LocalAdmission
from private_task_consent import TaskConsent,ConsentLedger,Confirmation,ConsentRejected,open_consent_service
from cluster_p1.online_issuance import _make_declaration
class Trigger:
 async def confirm(self,credential,receipt,body):return Confirmation(receipt.proposal_id,receipt.proposal_digest,body['owner'])
class Tests(unittest.IsolatedAsyncioTestCase):
 make_reader=base.Checks.make_reader
 async def asyncSetUp(self):
  def fresh(*args,**kw):
   kw.pop('initialize');kw.pop('enable_unified');return open_consent_service(*args,**kw)
  with patch.object(base.app,'open_service',side_effect=fresh):await fixture.Tests.asyncSetUp(self)
  self.key=Ed25519PrivateKey.generate();now=int(time.time())
  raw=json.dumps(dict(v=1,typ='local-identity',iss='synthetic-declaration-test',aud='consent-lab',sub='alice',sid='session',iat=now,nbf=now,exp=now+120),sort_keys=True,separators=(',',':')).encode()
  self.token=b64(raw)+'.'+b64(self.key.sign(DOMAIN+raw))
  adapter=SignedIdentityAdapter(issuer='synthetic-declaration-test',audience='consent-lab',public_key=self.key.public_key().public_bytes(Encoding.Raw,PublicFormat.Raw))
  self.local=LocalAdmission.create(self.root/'signed.db',issuer='synthetic-declaration-test',audience='consent-lab',adapter=adapter)
  self.service._verifier=self.local;await self.local.prepare_user(self.token)
  self.consent=TaskConsent(self.reader,identity_fence=self.local,trigger=Trigger())
  self.declaration=_make_declaration(self.ptask,self.session,self.stages,self.tiles,24,100,base.MODEL['config_sha256'])
  self.policy=dict(max_cost_microunits=100,currency='CNY',price_policy='price-v1',privacy='private',privacy_policy='privacy-v1',expires_at=now+60,actions=sorted(['register','claim','install','load','execute','commit','export']),offline=False)
  self.placements=[dict(stage=i,node=n,device='cuda:0',instance='fresh-'+n,resource_id='held-'+n) for i,n in enumerate(('a','b'))]
 async def asyncTearDown(self):
  if hasattr(self,'local'):await self.local.close()
  await base.Checks.asyncTearDown(self)
 def set_trigger(self,trigger):self.consent=TaskConsent(self.reader,identity_fence=self.local,trigger=trigger)
 async def prepare(self):return await self.consent.prepare(self.token,self.declaration,self.policy,self.placements)
 async def active(self):
  r=await self.prepare();return await self.consent.confirm(self.token,r.proposal_id,r.proposal_digest)
 async def test_real_signed_identity_real_stored_task_complete_binding(self):
  r=await self.active();self.assertFalse(r.execution_allowed)
  self.assertEqual(await self.consent.check_context(self.token,r.proposal_id,r.proposal_digest,'execute'),r)
  body=json.loads(self.ledger._db.execute('SELECT body FROM task_consents').fetchone()[0])
  self.assertEqual(body['source']['input_snapshot'],self.ref.digest);self.assertEqual(body['policy'],self.policy);self.assertEqual(body['declaration'],self.declaration);self.assertEqual(body['placements'],self.placements)
  with self.assertRaises(Exception):self.ledger.bind_plan(self.owner,self.task.task_id,b'{}')
  self.assertEqual(self.ledger._db.execute('SELECT count(*) FROM structural_plans').fetchone(),(0,))
 async def test_no_explicit_trigger_denies_even_valid_identity(self):
  self.set_trigger(None)
  with self.assertRaises(Exception):await self.prepare()
  self.assertEqual(self.ledger._db.execute('SELECT count(*) FROM task_consents').fetchone(),(0,))
 async def test_bad_identity_denies(self):
  with self.assertRaises(Exception):await self.consent.prepare('admin',self.declaration,self.policy,self.placements)
 async def test_wrong_confirmation_digest_denies(self):
  r=await self.prepare()
  with self.assertRaises(Exception):await self.consent.confirm(self.token,r.proposal_id,'f'*64)
 async def test_offline_and_invalid_budget_deny(self):
  for value in (True,0,-1,2**53):
   self.policy['max_cost_microunits']=value
   with self.assertRaises(Exception):await self.prepare()
  self.policy['max_cost_microunits']=100;self.policy['offline']=True
  with self.assertRaises(Exception):await self.prepare()
 async def test_revoke_before_proposal_leaves_tombstone(self):
  await self.consent.revoke(self.token,self.task.task_id)
  with self.assertRaises(Exception):await self.prepare()
 async def test_revoke_pending_blocks_confirm(self):
  r=await self.prepare();await self.consent.revoke(self.token,self.task.task_id)
  with self.assertRaises(Exception):await self.consent.confirm(self.token,r.proposal_id,r.proposal_digest)
 async def test_revoke_while_user_waits_no_lock_held(self):
  entered=asyncio.Event();release=asyncio.Event()
  class Wait:
   async def confirm(inner,*args):entered.set();await release.wait();return await Trigger().confirm(*args)
  self.set_trigger(Wait());r=await self.prepare();job=asyncio.create_task(self.consent.confirm(self.token,r.proposal_id,r.proposal_digest));await entered.wait()
  self.assertFalse(self.a._lock.locked());await self.consent.revoke(self.token,self.task.task_id);release.set()
  with self.assertRaises(Exception):await job
  self.assertEqual(self.ledger._db.execute('SELECT state FROM task_consents').fetchone(),('REVOKED',))
 async def test_identity_revoke_during_confirmation(self):
  class Revoke:
   async def confirm(inner,*args):await self.local.revoke_subject('alice');return await Trigger().confirm(*args)
  self.set_trigger(Revoke());r=await self.prepare()
  with self.assertRaises(Exception):await self.consent.confirm(self.token,r.proposal_id,r.proposal_digest)
  self.assertEqual(self.consent.live,set())
 async def test_task_cancel_during_confirmation(self):
  class Cancel:
   async def confirm(inner,*args):await self.a.cancel(self.owner,self.task.task_id);return await Trigger().confirm(*args)
  self.set_trigger(Cancel());r=await self.prepare()
  with self.assertRaises(Exception):await self.consent.confirm(self.token,r.proposal_id,r.proposal_digest)
 async def test_new_instance_does_not_restore_active_row(self):
  r=await self.active();other=TaskConsent(self.reader,identity_fence=self.local,trigger=Trigger())
  with self.assertRaises(Exception):await other.check_context(self.token,r.proposal_id,r.proposal_digest,'execute')
  with self.assertRaises(Exception):await other.confirm(self.token,r.proposal_id,r.proposal_digest)
 async def test_duplicate_confirmation_never_reinvokes_trigger(self):
  r=await self.active()
  with self.assertRaises(Exception):await self.consent.confirm(self.token,r.proposal_id,r.proposal_digest)
 async def test_changed_policy_needs_new_action_not_old_receipt(self):
  r=await self.active();self.policy['privacy']='public'
  with self.assertRaises(Exception):await self.prepare()
  with self.assertRaises(Exception):await self.consent.check_context(self.token,r.proposal_id,'f'*64,'execute')
 async def test_wrong_model_and_plan_reject(self):
  self.declaration['config_sha256']='f'*64
  with self.assertRaises(Exception):await self.prepare()
 async def test_trigger_exception_retains_failed_record(self):
  class Fail:
   async def confirm(inner,*args):raise RuntimeError('response unknown')
  self.set_trigger(Fail());r=await self.prepare()
  with self.assertRaises(RuntimeError):await self.consent.confirm(self.token,r.proposal_id,r.proposal_digest)
  self.assertEqual(self.ledger._db.execute('SELECT state FROM task_consents').fetchone(),('FAILED',))
  with self.assertRaises(Exception):await self.consent.confirm(self.token,r.proposal_id,r.proposal_digest)
 async def test_expired_revoke_still_works(self):
  r=await self.active();self.consent.clock=lambda:self.policy['expires_at']+1
  with self.assertRaises(Exception):await self.consent.check_context(self.token,r.proposal_id,r.proposal_digest,'execute')
  await self.consent.revoke(self.token,self.task.task_id)
 async def test_sql_immutable_body(self):
  await self.prepare()
  with self.assertRaises(Exception):
   with self.ledger._transaction():self.ledger._db.execute("UPDATE task_consents SET body='{}'")
 async def test_cancel_confirmation_wait_leaves_failed_no_live(self):
  entered=asyncio.Event();release=asyncio.Event()
  class Wait:
   async def confirm(inner,*args):entered.set();await release.wait();return await Trigger().confirm(*args)
  self.set_trigger(Wait());r=await self.prepare();job=asyncio.create_task(self.consent.confirm(self.token,r.proposal_id,r.proposal_digest));await entered.wait();job.cancel()
  with self.assertRaises(asyncio.CancelledError):await job
  self.assertEqual(self.consent.live,set());self.assertEqual((await self.consent.history(self.token,r.proposal_id)).state,'FAILED')
 async def test_trigger_replacement_cannot_confirm_old_proposal(self):
  r=await self.prepare();self.consent.trigger=Trigger()
  with self.assertRaises(Exception):await self.consent.confirm(self.token,r.proposal_id,r.proposal_digest)
 async def test_expiry_while_waiting_user_denies(self):
  class Expire:
   async def confirm(inner,*args):self.consent.clock=lambda:self.policy['expires_at']+1;return await Trigger().confirm(*args)
  self.set_trigger(Expire());r=await self.prepare()
  with self.assertRaises(Exception):await self.consent.confirm(self.token,r.proposal_id,r.proposal_digest)
 async def test_ledger_copy_cannot_restore_live_context(self):
  r=await self.active()
  # Copy ordinary SQLite via backup: persisted facts contain no usable live grant.
  import sqlite3
  path=self.root/'copied.sqlite';copy_db=sqlite3.connect(path)
  try:
   self.ledger._db.backup(copy_db)
   self.assertEqual(copy_db.execute('SELECT body,digest,state FROM task_consents').fetchone(),self.ledger._db.execute('SELECT body,digest,state FROM task_consents').fetchone())
  finally:copy_db.close()
  other=TaskConsent(self.reader,identity_fence=self.local,trigger=Trigger())
  self.assertEqual((await other.history(self.token,r.proposal_id)).state,'ACTIVE')
  with self.assertRaises(Exception):await other.check_context(self.token,r.proposal_id,r.proposal_digest,'execute')
  # Deliberately demonstrate trusted-process copy boundary, NOT global protection.
  other.boot=self.consent.boot;other.live=set(self.consent.live)
  self.assertFalse((await other.check_context(self.token,r.proposal_id,r.proposal_digest,'execute')).execution_allowed)
 async def test_pending_body_must_not_change_with_caller_dict(self):
  r=await self.prepare();self.policy['privacy']='public';self.placements[0]['device']='other'
  await self.consent.confirm(self.token,r.proposal_id,r.proposal_digest)
  body=json.loads(self.ledger._db.execute('SELECT body FROM task_consents').fetchone()[0]);self.assertEqual(body['policy']['privacy'],'private');self.assertEqual(body['placements'][0]['device'],'cuda:0')
 async def test_confirmation_commit_unknown_seals_eligibility(self):
  from contextlib import contextmanager
  r=await self.prepare();old=self.ledger._transaction
  @contextmanager
  def uncertain(*args,**kw):
   with old(*args,**kw):yield
   row=self.ledger._db.execute('SELECT state FROM task_consents').fetchone()
   if row==('ACTIVE',):raise OSError('commit response lost')
  with patch.object(self.ledger,'_transaction',uncertain):
   with self.assertRaises(OSError):await self.consent.confirm(self.token,r.proposal_id,r.proposal_digest)
  self.assertEqual(self.consent.live,set());self.assertEqual((await self.consent.history(self.token,r.proposal_id)).state,'FAILED')
 async def test_revoke_unknown_seals_handle(self):
  from contextlib import contextmanager
  await self.active();old=self.ledger._transaction
  @contextmanager
  def uncertain(*args,**kw):
   with old(*args,**kw):yield
   raise OSError('commit response lost')
  with patch.object(self.ledger,'_transaction',uncertain):
   with self.assertRaises(OSError):await self.consent.revoke(self.token,self.task.task_id)
  self.assertTrue(self.consent.retired);self.assertEqual(self.consent.live,set())
 async def test_revoke_failure_seals_all_handles_same_authority(self):
  from contextlib import contextmanager
  r=await self.active();other=TaskConsent(self.reader,identity_fence=self.local,trigger=Trigger());old=self.ledger._transaction
  @contextmanager
  def rollback(*args,**kw):
   with old(*args,**kw):
    yield
    raise OSError('revoke rollback')
  with patch.object(self.ledger,'_transaction',rollback):
   with self.assertRaises(OSError):await other.revoke(self.token,self.task.task_id)
  self.assertEqual(self.ledger._db.execute('SELECT state FROM task_consents').fetchone(),('ACTIVE',))
  with self.assertRaises(Exception):await self.consent.check_context(self.token,r.proposal_id,r.proposal_digest,'execute')
 async def test_revoke_missing_task_does_not_poison_other_consent(self):
  r=await self.active()
  with self.assertRaises(Exception):await self.consent.revoke(self.token,'f'*32)
  self.assertFalse((await self.consent.check_context(self.token,r.proposal_id,r.proposal_digest,'execute')).execution_allowed)
 async def test_user_exit_expiry_is_rechecked(self):
  from contextlib import contextmanager
  r=await self.active();old=self.local.hold
  @contextmanager
  def late(credential):
   with old(credential):yield
   self.consent.clock=lambda:self.policy['expires_at']+1
  with patch.object(self.local,'hold',late):
   with self.assertRaises(Exception):await self.consent.check_context(self.token,r.proposal_id,r.proposal_digest,'execute')
if __name__=='__main__':unittest.main()
