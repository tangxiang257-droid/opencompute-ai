"""Independent ordinary-byte review; no listener/device/model."""
import unittest,json,base64,sqlite3
from unittest.mock import patch
import private_local_admission as admission_module
from contextlib import contextmanager
from validation.c_service.test_service import Flow as Base
from validation.signed_identity.test_adapter import b64,DOMAIN
from validation.c_input_import.test_import import LabProvider
from private_c_result_route import LocalCResultRoute
import result_download_api as api
class Checks(Base):
 async def request_result(self,task):
  messages=[]
  async def receive():return {'type':'http.request','body':b'','more_body':False}
  async def send(x):messages.append(x)
  await api.create_app(service=self.service)({'type':'http','method':'GET','path':'/tasks/'+task+'/result','query_string':b'','headers':[(b'authorization',('Bearer '+self.token).encode())]},receive,send)
  return messages[0]['status']
 async def test_alias_must_deny(self):
  actual=await self.run_chain()
  policy=LabProvider()
  route=LocalCResultRoute.install(self.service,self.cp,authorize=policy,enabled=True)
  self.assertEqual(await self.request_result(actual.request),200)
  alias='e'*32
  self.assertNotEqual(alias,actual.request)
  self.service._c_result_routes[alias]=route
  self.assertEqual(await self.request_result(alias),404,'Wrong request task aliases live C result')
 async def test_exit_slot_and_final_guards(self):
  actual=await self.run_chain()
  policy=LabProvider()
  with self.assertRaises(Exception):LocalCResultRoute.install(self.service,self.cp,enabled=True)
  route=LocalCResultRoute.install(self.service,self.cp,authorize=policy,enabled=True)
  self.assertEqual(await self.request_result(actual.request),200)
  self.assertEqual(await self.request_result(actual.request),200)
  claims=json.loads(base64.urlsafe_b64decode(self.token.split('.')[0]+'=='))
  with self.assertRaises(Exception):await route.fetch('unknown-credential',lambda *args:None,metadata=lambda _:None,max_bytes=4194304)
  self.assertFalse(self.importer.retired);self.assertFalse(route.e.dead)
  with self.assertRaises(sqlite3.IntegrityError):
   self.artifacts._db.execute("UPDATE artifact_tickets SET digest=? WHERE task_id=?",('f'*64,actual.request))
  self.assertEqual(route._current(self.token),route.manifest)
  policy.fail_exit=True
  self.assertEqual(await self.request_result(actual.request),404)
  policy.fail_exit=False
  self.assertFalse(self.importer.retired)
  def mutate(_):self.adm.db.execute("UPDATE c_import_slots SET state='UNKNOWN' WHERE experiment=?",(self.receipt.proposal_id,))
  with self.assertRaises(Exception):await route.fetch(self.token,lambda *args:None,metadata=mutate,max_bytes=4194304)
  self.assertEqual(await self.request_result(actual.request),404)
  # Explicit fixture restoration only; never recovery behavior of production.
  self.adm.db.execute("UPDATE c_import_slots SET state='IMPORTED' WHERE experiment=?",(self.receipt.proposal_id,))
  digest=await route.fetch(self.token,lambda *args:None,metadata=lambda _:None,max_bytes=4194304)
  self.service._c_result_routes[actual.request]=object()
  with self.assertRaises(Exception):self.service._download_final(self.token,actual.request,actual.owner,digest,route)
  self.service._c_result_routes[actual.request]=route
  self.assertEqual(await self.request_result(actual.request),200)
  with patch.object(admission_module.time,'time',return_value=claims['exp']+1):
   with self.assertRaises(Exception):await route.fetch(self.token,lambda *args:None,metadata=lambda _:None,max_bytes=4194304)
  self.assertFalse(self.importer.retired);self.assertFalse(route.e.dead)

def load_tests(*args):return unittest.TestSuite(Checks(n) for n in ('test_alias_must_deny','test_exit_slot_and_final_guards'))

if __name__=="__main__":unittest.main()
