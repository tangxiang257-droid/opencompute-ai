import asyncio,unittest
from unittest.mock import patch
from validation.b_result_dial.test_pipeline import Flow,dial_module,host_module
class Review(Flow):
 async def _close_during_attach(self,module,attribute):
  entered=asyncio.Event();cancelled=asyncio.Event();original=module._accept_unstarted
  async def blocked(*a,**kw):
   entered.set()
   try:await asyncio.Event().wait()
   except asyncio.CancelledError:cancelled.set();raise
  with patch.object(module,'_accept_unstarted',blocked):
   chain=asyncio.create_task(self.run_chain())
   close=None
   try:
    await asyncio.wait_for(entered.wait(),25)
    close=asyncio.create_task(getattr(self,attribute).close())
    await asyncio.wait_for(cancelled.wait(),2)
    done,pending=await asyncio.wait((chain,close),timeout=1)
    self.assertFalse(pending,'attach exception rejoined host cleanup that is awaiting launcher/task')
    results=await asyncio.gather(chain,close,return_exceptions=True);self.assertIsInstance(results[0],asyncio.CancelledError);self.assertIsNone(results[1])
   finally:
    if close is not None:await asyncio.gather(chain,close,return_exceptions=True)
    else:chain.cancel();await asyncio.gather(chain,return_exceptions=True)
 async def test_external_dial_close_during_attach_does_not_self_join(self):await self._close_during_attach(dial_module,'result_dial')
 async def test_external_host_close_during_attach_does_not_self_join(self):await self._close_during_attach(host_module,'result_host')
def load_tests(*args):return unittest.TestSuite(Review(n) for n in Review.__dict__ if n.startswith('test_'))
if __name__=='__main__':unittest.main()
