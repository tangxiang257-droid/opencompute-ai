"""Actual launcher/SQLite/memory chain under synthetic-only host supervision."""
import asyncio,unittest
from cluster_p1.thin_host import _OwnedRun
from validation.role_launcher.test_pipeline import Tests as Base
class Tests(Base):
 async def create_startup(self,*args,**kw):
  startup=await super().create_startup(*args,**kw)
  self.host.own_launcher(self.launchers[startup.stage]);return startup
 async def create_receiver(self,*args,**kw):
  role=await super().create_receiver(*args,**kw);self.host.own_launcher(self.receiving_launcher);return role
 async def test_owned_full_byte_chain(self):
  self.host=_OwnedRun(work_seconds=20,close_seconds=10)
  async def body(host):await self.online_bootstrap();await self.run_split()
  report=await self.host._synthetic_run(body,asyncio.Event())
  self.assertEqual(report['status'],'COMPLETED');self.assertFalse(report['execution_allowed'])
  self.assertEqual(len(self.host.launchers),3)
  self.assertTrue(all(x._closing for x in self.host.launchers))
def load_tests(loader,tests,pattern):return unittest.TestSuite([Tests('test_owned_full_byte_chain')])
if __name__=='__main__':unittest.main()
