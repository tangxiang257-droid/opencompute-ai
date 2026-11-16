"""Synthetic providers only; real C ledger and original HTTP download adapter."""
import unittest,json,base64
from validation.signed_identity.test_adapter import b64,DOMAIN
from contextlib import contextmanager
from unittest.mock import patch
from validation.c_service.test_service import Flow as Base
from validation.authority_chain.test_chain import LabProvider
from private_c_result_route import LocalCResultRoute
import result_download_api as api
class Policy:
 def __init__(self):self.exit=None;self.actions=[]
 @contextmanager
 def hold(self,context,action):
  assert action=='result-download';self.actions.append(action)
  yield None
  if self.exit:self.exit()
class Checks(Base):
 async def download(self,task,token=None):
  messages=[]
  async def receive():return {'type':'http.request','body':b'','more_body':False}
  async def send(value):messages.append(value)
  await api.create_app(service=self.service)({'type':'http','method':'GET','path':'/tasks/'+task+'/result','query_string':b'','headers':[(b'authorization',('Bearer '+(token or self.token)).encode())]},receive,send)
  return messages[0]['status']
 async def test_route_security(self):
  actual=await self.run_chain();task=actual.request;p=Policy()
  for kwargs in ({},{'enabled':True},{'authorize':p}):
   with self.assertRaises(Exception):LocalCResultRoute.install(self.service,self.cp,**kwargs)
  route=LocalCResultRoute.install(self.service,self.cp,authorize=p,enabled=True)
  self.assertEqual(await self.download(task),200)
  self.assertEqual(await self.download(task),200) # read is idempotent, not a new execution
  self.assertEqual(await self.download(task,'unrecognized-token'),404)
  self.assertEqual(await self.download('0'*32),404)
  with self.assertRaises(Exception):LocalCResultRoute.install(self.service,self.cp,authorize=p,enabled=True)
  self.assertEqual(await self.download(task),200)
  # A provider's failing exit cannot be ignored or turn into a successful send.
  def fail():raise RuntimeError('synthetic exit failure')
  p.exit=fail;self.assertEqual(await self.download(task),404);p.exit=None
  self.assertEqual(await self.download(task),200)
  # Change routing after bytes were read but while JSON is encoded.
  original=api.json.dumps
  def swapped(value,*a,**kw):
   result=original(value,*a,**kw)
   if type(value)is dict and value.get('package')=='opencompute-result-download-v1':self.service._c_result_routes[task]=object()
   return result
  with patch.object(api.json,'dumps',swapped):self.assertEqual(await self.download(task),404)
  self.service._c_result_routes[task]=route
  self.assertEqual(await self.download(task),200)
  # Original live context expiry is checked again immediately before send.
  now=self.adm.consent._now
  def expire(value,*a,**kw):
   result=original(value,*a,**kw)
   if type(value)is dict and value.get('package')=='opencompute-result-download-v1':self.adm.consent._now=lambda:10**20
   return result
  with patch.object(api.json,'dumps',expire):self.assertEqual(await self.download(task),404)
  self.adm.consent._now=now
  self.assertTrue(p.actions)
  # Metadata callback runs after payload reads: a revocation there still denies.
  source=self.body['source_task']
  def revoked(_):self.consent.fence.tasks.add(source)
  with self.assertRaises(Exception):await route.fetch(self.token,lambda *args:None,metadata=revoked,max_bytes=4194304)
  self.assertEqual(await self.download(task),404)
  self.consent.fence.tasks.remove(source) # test-only restoration for orderly teardown
  # Preparing a second credential clears the original LocalAdmission cache;
  # perform this destructive identity transition last, never restore execution.
  body=json.loads(base64.urlsafe_b64decode(self.token.split('.')[0]+'=='));body['sub']='bob';body['sid']='bob-session'
  raw=json.dumps(body,sort_keys=True,separators=(',',':')).encode();bob=b64(raw)+'.'+b64(self.key.sign(DOMAIN+raw))
  await self.local.prepare_user(bob)
  self.assertNotEqual(self.service._download_owner(bob),self.owner)
  self.assertEqual(await self.download(task,bob),404)
  self.assertFalse(self.importer.retired)
def load_tests(*args):return unittest.TestSuite([Checks('test_route_security')])
if __name__=='__main__':unittest.main()
