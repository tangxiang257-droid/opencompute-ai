"""Direct ASGI messages, LAB signatures, actual admission/issuer; no listening."""
import asyncio,base64,copy,json,re,time,unittest
from urllib.parse import urlencode
from unittest.mock import patch
from validation.experiment_admission.test_admission import Tests as Base
from validation.signed_identity.test_adapter import b64,DOMAIN
from private_experiment_confirmation import ExperimentConfirmationApp,PATH
class Tests(Base):
 async def asyncSetUp(self):
  await super().asyncSetUp()
  self.web=ExperimentConfirmationApp(self.root/'web-experiments.db',self.consent,source_domain='a'*64,origin='https://lab.invalid',credential_for_scope=lambda scope:scope.get('lab_credential',self.token),policy_authority=self.parent)
  self.adm=self.web.admission;self.addCleanup(self.adm.close);self.strict.source_provider=self.adm;self.strict.authorize=self.adm
  self.receipt=await self.prepare_new()
 def fields(self):return dict(id=self.receipt.proposal_id,digest=self.receipt.proposal_digest)
 async def request(self,method='GET',fields=None,*,credential=None,origin='https://lab.invalid',extra_headers=(),send_override=None,receive_override=None):
  fields=self.fields() if fields is None else fields
  raw=urlencode(fields).encode();scope=dict(type='http',scheme='https',path=PATH,method=method,query_string=raw if method=='GET' else b'',headers=[(b'host',b'lab.invalid')]+list(extra_headers))
  if credential is not None:scope['lab_credential']=credential
  if method=='POST':scope['headers'] += [(b'origin',origin.encode()),(b'content-type',b'application/x-www-form-urlencoded')]
  messages=[]
  async def receive():return dict(type='http.request',body=raw,more_body=False)
  async def send(v):messages.append(v)
  await self.web(scope,receive_override or receive,send_override or send)
  return messages[0]['status'],messages[-1]['body'].decode()
 async def get_form(self):
  status,page=await self.request();self.assertEqual(status,200)
  challenge=re.search(r'name="challenge" value="([a-f0-9]+)"',page).group(1)
  return dict(self.fields(),challenge=challenge,decision='confirm-register',copy_input='allow-exact-snapshot')
 def count(self):return self.strict.db.execute('SELECT count(*) FROM registrations').fetchone()[0]
 async def test_get_displays_exact_context_no_confirmation(self):
  status,page=await self.request();self.assertEqual(status,200)
  self.assertIn(self.task.task_id,page);self.assertIn(self.ref.digest,page);self.assertIn('micro-units',page);self.assertIn('not been verified',page)
  checkbox=re.search(r'<input type="checkbox"[^>]+>',page).group(0);self.assertNotIn('checked',checkbox)
  self.assertEqual(self.count(),0);self.assertEqual(self.adm._row(self.owner,self.receipt.proposal_id)[3],'PENDING')
 async def test_explicit_post_to_actual_issuer(self):
  form=await self.get_form();status,page=await self.request('POST',form)
  self.assertEqual(status,200);self.assertIn('Execution remains disabled',page);self.assertEqual(self.count(),1);self.assertFalse(self.web.trigger.pending)
  context=json.loads(self.strict.db.execute('SELECT context FROM authorizations').fetchone()[0]);self.assertEqual(context['body']['c_input_copy'],self.copy)
 async def test_missing_checkbox_no_confirm_no_registration(self):
  form=await self.get_form();form.pop('copy_input');self.assertEqual((await self.request('POST',form))[0],403);self.assertEqual(self.count(),0);self.assertFalse(self.adm.published)
 async def test_wrong_checkbox_refuses(self):
  form=await self.get_form();form['copy_input']='yes';self.assertEqual((await self.request('POST',form))[0],403);self.assertEqual(self.count(),0)
 async def test_cancel_without_copy_permission(self):
  form=await self.get_form();form.pop('copy_input');form['decision']='cancel';self.assertEqual((await self.request('POST',form))[0],200)
  self.assertEqual(self.adm._row(self.owner,self.receipt.proposal_id)[3],'REVOKED');self.assertEqual(self.count(),0)
 async def test_cross_origin_refuses(self):
  form=await self.get_form();self.assertEqual((await self.request('POST',form,origin='https://evil.invalid'))[0],403);self.assertEqual(self.count(),0)
 async def test_duplicate_host_refuses(self):
  self.assertEqual((await self.request(extra_headers=[(b'host',b'lab.invalid')]))[0],403)
 async def test_duplicate_copy_field_refuses(self):
  form=list((await self.get_form()).items());form.append(('copy_input','allow-exact-snapshot'))
  self.assertEqual((await self.request('POST',form))[0],403);self.assertEqual(self.count(),0)
 async def test_challenge_replay_no_new_registration(self):
  form=await self.get_form();self.assertEqual((await self.request('POST',form))[0],200);self.assertEqual((await self.request('POST',form))[0],403);self.assertEqual(self.count(),1)
 async def test_confirmed_history_cannot_auto_dispatch(self):
  form=await self.get_form();key=(self.receipt.proposal_id,self.receipt.proposal_digest)
  import hashlib
  self.web.trigger.pending[key]=(hashlib.sha256(self.token.encode()).hexdigest(),self.owner)
  await self.adm.confirm(self.token,*key)
  status,page=await self.request();self.assertEqual(status,200);self.assertNotIn('value="confirm-register"',page)
  challenge=re.search(r'name="challenge" value="([a-f0-9]+)"',page).group(1);form['challenge']=challenge
  self.assertEqual((await self.request('POST',form))[0],403);self.assertEqual(self.count(),0)
 async def test_response_lost_is_history_not_retry(self):
  form=await self.get_form()
  async def fail(v):raise OSError('client lost')
  with self.assertRaises(OSError):await self.request('POST',form,send_override=fail)
  self.assertEqual(self.count(),1);status,page=await self.request();self.assertEqual(status,200);self.assertIn('REGISTERED',page);self.assertNotIn('value="confirm-register"',page)
  self.assertEqual((await self.request('POST',form))[0],403)
 async def test_expired_challenge_refuses(self):
  form=await self.get_form();v=self.web.challenges[form['challenge']];self.web.challenges[form['challenge']]=v[:4]+(0,)
  self.assertEqual((await self.request('POST',form))[0],403);self.assertEqual(self.count(),0)
 async def test_same_owner_other_credential_cannot_use_challenge(self):
  form=await self.get_form();payload=json.loads(base64.urlsafe_b64decode(self.token.split('.')[0]+'=='));payload['sid']='other-session'
  raw=json.dumps(payload,sort_keys=True,separators=(',',':')).encode();token=b64(raw)+'.'+b64(self.key.sign(DOMAIN+raw));await self.local.prepare_user(token)
  self.assertEqual((await self.request('POST',form,credential=token))[0],403);self.assertEqual(self.count(),0)
 async def test_no_authenticated_mapping_refuses(self):
  self.assertEqual((await self.request(credential='not-a-credential'))[0],403);self.assertEqual(self.count(),0)
 async def test_source_revoked_between_review_and_post(self):
  form=await self.get_form();await self.consent.revoke(self.token,self.task.task_id)
  self.assertEqual((await self.request('POST',form))[0],403);self.assertEqual(self.count(),0)
 async def test_commit_unknown_no_retry(self):
  form=await self.get_form();original=self.strict._commit
  def commit(db):original(db);raise OSError('unknown')
  with patch.object(self.strict,'_commit',side_effect=commit):self.assertEqual((await self.request('POST',form))[0],403)
  self.assertEqual(self.count(),1);status,page=await self.request();self.assertEqual(status,200);self.assertIn('History only',page);self.assertNotIn('<form',page)
 async def test_cancel_between_confirm_and_dispatch_burns_qualification(self):
  form=await self.get_form();entered=asyncio.Event();release=asyncio.Event();original=self.adm.dispatch
  async def dispatch(*args):entered.set();await release.wait();return await original(*args)
  with patch.object(self.adm,'dispatch',side_effect=dispatch):
   task=asyncio.create_task(self.request('POST',form));await entered.wait();task.cancel()
   with self.assertRaises(asyncio.CancelledError):await task
  self.assertTrue(self.adm.retired);self.assertFalse(self.adm.published);self.assertFalse(self.web.trigger.pending);self.assertEqual(self.count(),0)
 async def test_resolver_drift_after_await_permanently_seals(self):
  entered=asyncio.Event();release=asyncio.Event()
  async def resolver(scope):entered.set();await release.wait();return self.token
  self.web._resolver=resolver;self.web.credential_for_scope=resolver
  task=asyncio.create_task(self.request());await entered.wait();self.web.credential_for_scope=lambda s:self.token;release.set()
  self.assertEqual((await task)[0],403);self.web.credential_for_scope=resolver
  self.assertEqual((await self.request())[0],403);self.assertTrue(self.adm.retired)
 async def test_reopen_readonly_no_confirmation_controls(self):
  form=await self.get_form();await self.request('POST',form);self.adm.close()
  web=ExperimentConfirmationApp(self.root/'web-experiments.db',self.consent,source_domain='a'*64,origin='https://lab.invalid',credential_for_scope=lambda s:self.token,policy_authority=self.parent)
  self.addCleanup(web.admission.close);self.web=web
  status,page=await self.request();self.assertEqual(status,200);self.assertIn('History only',page);self.assertNotIn('<form',page)
 async def test_old_confirmation_route_not_accepted(self):
  out=[]
  async def send(m):out.append(m)
  async def receive():raise AssertionError('not read')
  await self.web(dict(type='http',scheme='https',path='/confirm',method='GET',headers=[(b'host',b'lab.invalid')]),receive,send)
  self.assertEqual(out[0]['status'],403)
 async def test_missing_resolver_defaults_to_refusal(self):
  web=ExperimentConfirmationApp(self.root/'no-login.db',self.consent,source_domain='a'*64,origin='https://lab.invalid',policy_authority=self.parent)
  self.addCleanup(web.admission.close);self.web=web
  self.assertEqual((await self.request())[0],403);self.assertEqual(self.count(),0)
 async def test_missing_policy_cannot_prepare(self):
  web=ExperimentConfirmationApp(self.root/'no-policy.db',self.consent,source_domain='a'*64,origin='https://lab.invalid',credential_for_scope=lambda s:self.token)
  self.addCleanup(web.admission.close)
  with self.assertRaises(Exception):await web.prepare(self.token,self.task.task_id,self.declaration,self.policy,self.placements,receiver=self.receiver,c_input_copy=self.copy,issuer=self.strict)
  self.assertEqual(web.admission.db.execute('SELECT count(*) FROM experiments').fetchone(),(0,))
def load_tests(loader,tests,pattern):return unittest.TestSuite(Tests(n) for n in Tests.__dict__ if n.startswith('test_'))
if __name__=='__main__':unittest.main()
