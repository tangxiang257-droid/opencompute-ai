"""Acceptance probe: preserve expected HTTP 200; do not skip/xfail.

Explicit trusted C route borrows the original ledger. Providers remain LAB.
No DB copy, authority replacement or real network.
"""
import unittest,json
from validation.c_service.test_service import Flow as Base
import result_download_api as api
class Checks(Base):
 async def test_C_persistent_result_reaches_original_download(self):
  actual=await self.run_chain()
  self.assertIsNot(self.artifacts,self.service._authority._store._managed._ledger)
  self.assertEqual(self.service._download_owner(self.token),actual.owner)
  self.assertEqual(self.c_launcher.role.store.history(actual.owner,actual.request)['state'],'RECEIVED')
  self.assertEqual(self.artifacts._metadata(actual.owner,actual.request)[0].state,'RESULT_STORED')
  self.assertIsNone(self.service._authority._store._managed._ledger._db.execute('SELECT state FROM stored_tasks WHERE owner_id=? AND task_id=?',(actual.owner,actual.request)).fetchone())
  from private_c_result_route import LocalCResultRoute
  from validation.authority_chain.test_chain import LabProvider
  LocalCResultRoute.install(self.service,self.cp,authorize=LabProvider(),enabled=True)
  messages=[]
  async def receive():return {'type':'http.request','body':b'','more_body':False}
  async def send(value):messages.append(value)
  await api.create_app(service=self.service)({'type':'http','method':'GET','path':'/tasks/'+actual.request+'/result','query_string':b'','headers':[(b'authorization',('Bearer '+self.token).encode())]},receive,send)
  self.assertEqual(messages[0]['status'],200,'C result is committed but original application ledger cannot locate it')
def load_tests(*args):return unittest.TestSuite([Checks('test_C_persistent_result_reaches_original_download')])
if __name__=='__main__':unittest.main()
