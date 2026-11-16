import asyncio,unittest
from unittest.mock import patch
from validation.b_result_dial.test_failures import Cases,module
class Quarantine(Cases):
 async def test_terminal_promise_precedes_blocked_writer_close(self):
  b,c=await self.candidate();entered=asyncio.Event();release=asyncio.Event();calls=[];writer=self.result_streams['data'][0][1]
  async def opened(*a,**k):
   calls.append(1)
   if len(calls)==2:raise OSError('LAB second dial failed')
   return self.result_streams['data'][0]
  async def wait_closed():entered.set();await release.wait()
  with patch.object(module,'load_context',self.context),patch.object(module.asyncio,'open_connection',opened),patch.object(writer,'wait_closed',wait_closed):
   start=asyncio.create_task(b.prepare(enabled=True))
   try:
    await asyncio.wait_for(entered.wait(),2)
    self.assertEqual(b.resources.query(b.ticket.reservation_id)['state'],'UNKNOWN','retire retained promise before potentially unbounded transport close')
   finally:release.set();results=await asyncio.gather(start,return_exceptions=True)
   self.assertIsInstance(results[0],OSError)
  self.assertEqual(self.calls[1],[])
def load_tests(*args):return unittest.TestSuite([Quarantine('test_terminal_promise_precedes_blocked_writer_close')])
if __name__=='__main__':unittest.main()
