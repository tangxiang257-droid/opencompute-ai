"""Existing A/B/C chain, ordinary bytes and LAB providers, no real network."""
import asyncio,unittest
from validation.install_host.test_host import Flow as Base
class Flow(Base):
 async def deliver_c_result(self,receiving,control,sender,actual,spool,op,d):
  job=asyncio.create_task(receiving.serve_result_once(self.artifacts,lambda:None,lambda:self.cp.hold(d,'receive'),enabled=True));self.jobs.append(job)
  receipt=await control.dispatch();self.assertEqual(receipt['state'],'COMMITTED');op.enable_read()
  return await asyncio.gather(sender.send(actual,spool,op.metadata['output_digest'],op.check_export),job)
 async def test_owned_C_ACK_through_persistent_result(self):await self.run_chain()
def load_tests(*args):return unittest.TestSuite([Flow('test_owned_C_ACK_through_persistent_result')])
if __name__=='__main__':unittest.main()
