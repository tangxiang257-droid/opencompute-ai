"""Real CPU Ed25519 with ephemeral LAB keys; no real issuer authority."""
import asyncio,base64,json,time,tempfile,unittest
from pathlib import Path
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding,PublicFormat
from private_local_admission import LocalAdmission,AdmissionRejected
try:from private_signed_identity import SignedIdentityAdapter
except ImportError:SignedIdentityAdapter=None
DOMAIN=b'opencompute/local-identity/v1\0'
def b64(x):return base64.urlsafe_b64encode(x).rstrip(b'=').decode()
class Tests(unittest.IsolatedAsyncioTestCase):
 def setUp(self):
  self.assertIsNotNone(SignedIdentityAdapter,'signed adapter missing')
  self.key=Ed25519PrivateKey.generate();self.pub=self.key.public_key().public_bytes(Encoding.Raw,PublicFormat.Raw)
  self.adapter=SignedIdentityAdapter(issuer='lab-issuer',audience='lab-only',public_key=self.pub)
  self.now=int(time.time());self.claims=dict(v=1,typ='local-identity',iss='lab-issuer',aud='lab-only',sub='alice',sid='session',iat=self.now,nbf=self.now,exp=self.now+60)
 def token(self,claims=None,key=None,raw=None):
  raw=raw if raw is not None else json.dumps(claims or self.claims,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()
  return b64(raw)+'.'+b64((key or self.key).sign(DOMAIN+raw))
 async def test_valid_real_signature(self):
  o=await self.adapter.observe(self.token());self.assertEqual((o.issuer,o.subject,o.session,o.audience),('lab-issuer','alice','session','lab-only'))
 async def test_wrong_key_tamper_and_domain(self):
  token=self.token();payload,sig=token.split('.')
  wrong=[self.token(key=Ed25519PrivateKey.generate()),payload+'.'+b64(b'0'*64),payload+'.'+b64(self.key.sign(base64.urlsafe_b64decode(payload+'='*(-len(payload)%4))))]
  for value in wrong:
   with self.assertRaises(AdmissionRejected):await self.adapter.observe(value)
 async def test_claim_confusion(self):
  for change in ({'iss':'other'},{'aud':'other'},{'typ':'execution-grant'},{'exp':self.now-1},{'nbf':self.now+100},{'iat':True},{'exp':self.now+301},{'extra':1},{'sub':'\n'},{'v':True}):
   with self.assertRaises(AdmissionRejected):await self.adapter.observe(self.token(dict(self.claims,**change)))
 async def test_noncanonical_and_oversized(self):
  raw=json.dumps(self.claims).encode()
  for token in (self.token(raw=raw),self.token()+'=',self.token()+'.x','x'*4097,self.token(raw=b'{"v":1,"v":1}')):
   with self.assertRaises(AdmissionRejected):await self.adapter.observe(token)
 async def test_existing_ledger_subject_revoke_and_reopen(self):
  with tempfile.TemporaryDirectory() as root:
   path=Path(root)/'admission.db';a=LocalAdmission.create(path,issuer='lab-issuer',audience='lab-only',adapter=self.adapter);token=self.token()
   try:
    await a.prepare_user(token);self.assertEqual(a.verify(token).subject,'alice')
    await a.revoke_subject('alice');self.assertIsNone(a.verify(token))
    with self.assertRaises(AdmissionRejected):await a.prepare_user(token)
   finally:await a.close()
   h=LocalAdmission.open_existing(path)
   try:
    self.assertIsNone(h.verify(token))
    with self.assertRaises(AdmissionRejected):await h.prepare_user(token)
    self.assertIn('revoke_subject',str(h.history()))
   finally:await h.close()
 async def test_session_revoke_and_observation_race(self):
  with tempfile.TemporaryDirectory() as root:
   a=LocalAdmission.create(Path(root)/'a.db',issuer='lab-issuer',audience='lab-only',adapter=self.adapter);token=self.token();entered=asyncio.Event();release=asyncio.Event();original=self.adapter.observe
   async def observe(value):
    result=await original(value);entered.set();await release.wait();return result
   self.adapter.observe=observe
   try:
    pending=asyncio.create_task(a.prepare_user(token));await entered.wait();await a.revoke_session('alice','session');release.set()
    with self.assertRaises(AdmissionRejected):await pending
    self.assertIsNone(a.verify(token))
   finally:release.set();await a.close()
 async def test_revoke_commit_response_lost_stays_poisoned(self):
  with tempfile.TemporaryDirectory() as root:
   path=Path(root)/'a.db';a=LocalAdmission.create(path,issuer='lab-issuer',audience='lab-only',adapter=self.adapter);token=self.token()
   try:
    await a.prepare_user(token);original=a._commit
    def uncertain(c):original(c);raise OSError('injected commit reply loss')
    a._commit=uncertain
    with self.assertRaises(AdmissionRejected):await a.revoke_subject('alice')
    self.assertEqual(a.state,'POISONED');self.assertIsNone(a.verify(token))
   finally:await a.close()
   h=LocalAdmission.open_existing(path)
   try:self.assertIn('revoke_subject',str(h.history()));self.assertIsNone(h.verify(token))
   finally:await h.close()
 async def test_no_adapter_remains_denied(self):
  with tempfile.TemporaryDirectory() as root:
   a=LocalAdmission.create(Path(root)/'a.db',issuer='lab-issuer',audience='lab-only')
   try:
    with self.assertRaises(AdmissionRejected):await a.prepare_user(self.token())
   finally:await a.close()
if __name__=='__main__':unittest.main()
