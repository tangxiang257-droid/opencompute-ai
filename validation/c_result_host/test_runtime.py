import asyncio,unittest
from validation.c_result_host.test_host import Flow
from validation.c_result_host.test_failures import Stop
from validation.authority_chain.test_chain import Identity
class Runtime(Flow):
 async def deliver_c_result(self,receiving,control,sender,actual,spool,op,d):
  def changed():self.c_host.identity=Identity()
  async def deliver():
   receipt=await control.dispatch()
   if receipt['state']=='COMMITTED':
    op.enable_read();return await sender.send(actual,spool,op.metadata['output_digest'],op.check_export)
   return receipt
  results=await asyncio.gather(self.c_host.serve_once(self.artifacts,changed,lambda:self.cp.hold(d,'receive'),enabled=True),deliver(),return_exceptions=True)
  self.assertIsInstance(results[0],BaseException)
  self.assertEqual(self.calls[1],[],'host provider drift must reject before C ACK allows B computation')
  self.assertEqual(self.c_host.state,'UNKNOWN')
  raise Stop
 async def test_host_pin_check_is_composed_before_C_ACK(self):
  with self.assertRaises(Stop):await self.run_chain()
def load_tests(*args):return unittest.TestSuite([Runtime('test_host_pin_check_is_composed_before_C_ACK')])
if __name__=='__main__':unittest.main()
