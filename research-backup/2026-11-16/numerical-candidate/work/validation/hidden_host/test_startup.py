from validation.hidden_host.test_pipeline import *
from validation.hidden_host.test_pipeline import Flow as Base
from private_b_hidden_host import BHiddenHost

class Startup(Base):
 async def serve_installation_endpoint(self,service,launch,args,policy):
  if launch.config.data['role']=='A':return await super().serve_installation_endpoint(service,launch,args,policy)
  original=BHiddenHost.start
  async def start(host,*,enabled=False):
   if not enabled:return await original(host)
   if self.mode=='wrong_tls':
    self.hidden_raw[1].cert=b'wrong'
    return await original(host,enabled=True)
   entered=asyncio.Event()
   async def late(*a):
    entered.set()
    try:await asyncio.Event().wait()
    except asyncio.CancelledError:return self.hidden_raw
   with patch.object(self.hidden_module,'_accepted_tls_streams',late):
    task=asyncio.create_task(original(host,enabled=True))
    await asyncio.wait_for(entered.wait(),3);task.cancel()
    return await task
  ended=asyncio.Event();errors=[]
  async def observed(host,*,enabled=False):
   try:return await start(host,enabled=enabled)
   except BaseException as error:
    if enabled:errors.append(error)
    raise
   finally:
    if enabled:ended.set()
  with patch.object(BHiddenHost,'start',observed):
   with self.assertRaises((ValueError,asyncio.CancelledError,asyncio.IncompleteReadError)):
    await super().serve_installation_endpoint(service,launch,args,policy)
   await asyncio.wait_for(ended.wait(),6)
  self.assertEqual(len(errors),1)
  self.assertIsInstance(errors[0],ValueError if self.mode=='wrong_tls' else asyncio.CancelledError)
 async def asyncSetUp(self):
  self.mode='wrong_tls' if self._testMethodName=='test_wrong_tls' else 'late'
  await super().asyncSetUp()
 def check_retired(self):
  h=self.hidden_host
  self.assertTrue(h.claimed);self.assertTrue(h.launcher._closing);self.assertEqual(h.state,'UNKNOWN')
  self.assertTrue(self.hidden_raw[1].closed);self.assertTrue(all(f[:1] not in (b'H',b'C',b'D') for f in self.hidden_raw[1].frames))
  self.assertEqual(h.resources.query(h.ticket.reservation_id)['state'],'UNKNOWN');self.assertEqual(self.calls,[[],[]])
 async def test_wrong_tls(self):self.check_retired()
 async def test_cancel_late_tls(self):self.check_retired()
def load_tests(*args):return unittest.TestSuite(Startup(x) for x in Startup.__dict__ if x.startswith('test_'))
if __name__=='__main__':unittest.main()
