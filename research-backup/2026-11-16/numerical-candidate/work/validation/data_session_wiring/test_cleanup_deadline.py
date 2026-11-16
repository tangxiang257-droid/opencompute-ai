"""Only a controlled writer substitutes for transport close, not authority."""
import asyncio,unittest
from types import SimpleNamespace
from cluster_p1.hidden_session import _ADataOwner
class CloseDeadline(unittest.IsolatedAsyncioTestCase):
 async def test_second_writer_wait_is_bounded_and_error_retained(self):
  entered=asyncio.Event();release=asyncio.Event()
  async def closed():pass
  async def blocked():entered.set();await release.wait()
  owner=object.__new__(_ADataOwner);owner.transport_cleanup=None;owner.retire=lambda:None
  owner.channel=SimpleNamespace(close=closed,w=SimpleNamespace(wait_closed=blocked),timeout=.02)
  task=asyncio.create_task(owner.close_transports())
  try:
   await entered.wait();done,_=await asyncio.wait((task,),timeout=.3)
   self.assertIn(task,done,'second raw wait_closed has no production deadline')
   with self.assertRaises(BaseExceptionGroup) as got:task.result()
   self.assertTrue(any(isinstance(e,TimeoutError) for e in got.exception.exceptions))
   with self.assertRaises(BaseExceptionGroup) as again:owner.transport_cleanup.result()
   self.assertIs(again.exception,got.exception)
  finally:release.set();await asyncio.gather(task,return_exceptions=True)
if __name__=='__main__':unittest.main()
