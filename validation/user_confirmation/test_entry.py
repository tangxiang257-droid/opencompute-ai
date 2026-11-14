"""LAB signed identity and synthetic authenticated web session, no listener."""
import asyncio,json,re,unittest
from urllib.parse import urlencode
from validation.task_consent.test_consent import Tests as Base
try:
 from private_user_confirmation import ConfirmationApp
except ImportError:ConfirmationApp=None
class Tests(Base):
 async def asyncSetUp(self):
  from unittest.mock import patch
  from validation.input_declaration import test_declaration as source_fixture
  from cluster_p1.online_issuance import _make_declaration
  original=source_fixture.snapshots.publish_input
  with patch.object(source_fixture.snapshots,'publish_input',side_effect=lambda fd,b,t:original(fd,b,(1,))):await super().asyncSetUp()
  self.session=self.session.model_copy(update={'expected_tokens':1})
  for tile in self.tiles:
   tile['token_count']=1
   for direction in ('input','output'):
    shape=list(tile[direction]['shape']);shape[1]=1;tile[direction]['shape']=tuple(shape)
  self.declaration=_make_declaration(self.ptask,self.session,self.stages,self.tiles,24,100,self.declaration['config_sha256'])
  self.assertIsNotNone(ConfirmationApp,'explicit user confirmation entry absent')
  # This resolver is synthetic. Actual hosting must supply authenticated session binding.
  self.app=ConfirmationApp(self.reader,identity_fence=self.local,origin='https://lab.invalid',credential_for_scope=lambda scope:self.token)
  self.consent=self.app.consent
  self.receiver=dict(version=1,node='c',identity='c-policy-v1',purpose='result-store',storage_policy='private-v1',retention_seconds=60,max_charge_bytes=33168)
 async def prepare(self):return await self.consent.prepare(self.token,self.declaration,self.policy,self.placements,receiver=self.receiver)
 async def request(self,method,query='',body=b'',origin='https://lab.invalid'):
  messages=[];sent=False
  async def receive():
   nonlocal sent
   if sent:return {'type':'http.disconnect'}
   sent=True;return {'type':'http.request','body':body,'more_body':False}
  async def send(v):messages.append(v)
  scope=dict(type='http',scheme='https',method=method,path='/confirm',query_string=query.encode(),headers=[(b'host',b'lab.invalid'),(b'origin',origin.encode()),(b'content-type',b'application/x-www-form-urlencoded')])
  await self.app(scope,receive,send)
  return messages[0]['status'],b''.join(x.get('body',b'') for x in messages).decode()
 async def review(self):
  r=await self.prepare();status,page=await self.request('GET',urlencode(dict(id=r.proposal_id,digest=r.proposal_digest)))
  self.assertEqual(status,200);challenge=re.search('name="challenge" value="([a-f0-9]+)"',page).group(1)
  return r,page,dict(id=r.proposal_id,digest=r.proposal_digest,challenge=challenge,decision='confirm')
 async def test_get_only_displays_complete_context_never_activates(self):
  r,page,form=await self.review();self.assertIn('private-v1',page);self.assertIn('max_cost_microunits',page);self.assertIn('input_snapshot',page)
  self.assertEqual((await self.consent.history(self.token,r.proposal_id)).state,'PENDING')
 async def test_explicit_post_activates_once(self):
  r,page,form=await self.review();self.assertEqual((await self.request('POST',body=urlencode(form).encode()))[0],200)
  self.assertEqual((await self.consent.check_context(self.token,r.proposal_id,r.proposal_digest,'export')).state,'ACTIVE')
  self.assertEqual((await self.request('POST',body=urlencode(form).encode()))[0],403)
 async def test_cross_origin_denies_without_confirm(self):
  r,page,form=await self.review();self.assertEqual((await self.request('POST',body=urlencode(form).encode(),origin='https://evil.invalid'))[0],403)
  self.assertEqual((await self.consent.history(self.token,r.proposal_id)).state,'PENDING')
 async def test_revoke_pending_before_post_denies(self):
  r,page,form=await self.review();await self.consent.revoke(self.token,self.task.task_id)
  self.assertEqual((await self.request('POST',body=urlencode(form).encode()))[0],403)
 async def test_no_session_resolver_denies(self):
  self.app.credential_for_scope=None
  self.assertEqual((await self.request('GET'))[0],403)
 async def test_direct_confirm_without_user_post_denies(self):
  r=await self.prepare()
  with self.assertRaises(Exception):await self.consent.confirm(self.token,r.proposal_id,r.proposal_digest)
 async def test_cancel_button_retires_pending(self):
  r,page,form=await self.review();form['decision']='cancel'
  self.assertEqual((await self.request('POST',body=urlencode(form).encode()))[0],200)
  self.assertEqual((await self.consent.history(self.token,r.proposal_id)).state,'REVOKED')
 async def test_changed_digest_never_confirms(self):
  r,page,form=await self.review();form['digest']='f'*64
  self.assertEqual((await self.request('POST',body=urlencode(form).encode()))[0],403)
  self.assertEqual((await self.consent.history(self.token,r.proposal_id)).state,'PENDING')
 async def test_duplicate_form_keys_denied(self):
  r,page,form=await self.review()
  self.assertEqual((await self.request('POST',body=(urlencode(form)+'&decision=confirm').encode()))[0],403)
 async def test_identity_revocation_denies_post(self):
  r,page,form=await self.review();await self.local.revoke_subject('alice')
  self.assertEqual((await self.request('POST',body=urlencode(form).encode()))[0],403)
 async def test_old_proposal_without_receiver_not_rendered(self):
  r=await self.consent.prepare(self.token,self.declaration,self.policy,self.placements)
  self.assertEqual((await self.request('GET',urlencode(dict(id=r.proposal_id,digest=r.proposal_digest))))[0],403)
 async def test_receiving_budget_cannot_cover_only_payload(self):
  self.receiver['max_charge_bytes']=400
  with self.assertRaises(Exception):await self.prepare()
 async def test_active_page_can_revoke_without_new_consent(self):
  r,page,form=await self.review();self.assertEqual((await self.request('POST',body=urlencode(form).encode()))[0],200)
  status,page=await self.request('GET',urlencode(dict(id=r.proposal_id,digest=r.proposal_digest)))
  self.assertEqual(status,200);form['challenge']=re.search('name="challenge" value="([a-f0-9]+)"',page).group(1);form['decision']='cancel'
  self.assertEqual((await self.request('POST',body=urlencode(form).encode()))[0],200)
  self.assertEqual((await self.consent.history(self.token,r.proposal_id)).state,'REVOKED')
 async def test_receiver_terms_frozen_before_authority_wait(self):
  from unittest.mock import patch
  entered=asyncio.Event();old=self.consent._proposal
  def observed(*args):
   result=old(*args);entered.set();return result
  await self.a._lock.acquire()
  try:
   with patch.object(self.consent,'_proposal',observed):
    job=asyncio.create_task(self.prepare());await entered.wait()
    self.receiver['storage_policy']='substituted'
  finally:self.a._lock.release()
  r=await job
  body=json.loads(self.ledger._db.execute('SELECT body FROM task_consents WHERE id=?',(r.proposal_id,)).fetchone()[0])
  self.assertEqual(body['receiver']['storage_policy'],'private-v1')
 async def test_unknown_confirmation_commit_does_not_return_success(self):
  from unittest.mock import patch
  r,page,form=await self.review();old=self.consent.confirm
  async def unknown(*args):await old(*args);raise OSError('response unknown')
  with patch.object(self.consent,'confirm',unknown):self.assertEqual((await self.request('POST',body=urlencode(form).encode()))[0],403)
  self.assertEqual((await self.consent.history(self.token,r.proposal_id)).state,'ACTIVE')
  self.assertEqual((await self.request('POST',body=urlencode(form).encode()))[0],403)
 async def test_three_token_plan_cannot_authorize_single_token_receiver(self):
  other=Base('test_real_signed_identity_real_stored_task_complete_binding')
  await other.asyncSetUp()
  try:
   with self.assertRaises(Exception):await other.consent.prepare(other.token,other.declaration,other.policy,other.placements,receiver=self.receiver)
  finally:
   await other.asyncTearDown();other.doCleanups()
if __name__=='__main__':
 r=unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(Tests(n) for n in Tests.__dict__ if n.startswith('test_')));raise SystemExit(not r.wasSuccessful())
