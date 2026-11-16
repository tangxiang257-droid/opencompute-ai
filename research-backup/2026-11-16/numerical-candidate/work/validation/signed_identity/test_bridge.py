"""Signed LAB identity through existing inert v12 source bridge and cancellation."""
import unittest,time,json
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding,PublicFormat
from validation.input_declaration import test_declaration as fixture
from validation.signed_identity.test_adapter import b64,DOMAIN
from private_signed_identity import SignedIdentityAdapter
from private_local_admission import LocalAdmission
import private_source_bridge as bridge
class Tests(unittest.IsolatedAsyncioTestCase):
 make_reader=fixture.Checks.make_reader
 async def asyncSetUp(self):
  await fixture.Checks.asyncSetUp(self)
  key=Ed25519PrivateKey.generate();now=int(time.time())
  raw=json.dumps(dict(v=1,typ='local-identity',iss='synthetic-declaration-test',aud='lab-only',sub='alice',sid='session',iat=now,nbf=now,exp=now+60),sort_keys=True,separators=(',',':')).encode()
  self.token=b64(raw)+'.'+b64(key.sign(DOMAIN+raw))
  adapter=SignedIdentityAdapter(issuer='synthetic-declaration-test',audience='lab-only',public_key=key.public_key().public_bytes(Encoding.Raw,PublicFormat.Raw))
  self.local=LocalAdmission.create(self.root/'identity.db',issuer='synthetic-declaration-test',audience='lab-only',adapter=adapter);self.service._verifier=self.local
  await self.local.prepare_user(self.token)
  self.b=await bridge._open_source_bridge(service=self.service,model=fixture.MODEL,preprocessing_version='declared-test-v1',contract_sha256=fixture.CONTRACT,source_namespace='e'*64,coordinator_path=self.root/'co',identity_fence=self.local,credential=self.token)
 async def asyncTearDown(self):
  await self.b.close();await self.local.close();await fixture.Checks.asyncTearDown(self)
 async def test_existing_bridge_signed_identity_stays_nonexecuting(self):
  result=await self.b.register_current_source(self.token,self.task.task_id)
  self.assertFalse(result.execution_allowed);self.assertEqual(self.b._co._reservations,{})
  with self.assertRaises(Exception):self.ledger.bind_plan(self.owner,self.task.task_id,b'{}')
 async def test_persistent_user_revoke_blocks_actual_bridge(self):
  await self.local.revoke_subject('alice')
  with self.assertRaises(bridge.SourceBridgeRejected):await self.b.register_current_source(self.token,self.task.task_id)
 async def test_existing_task_cancel_blocks_signed_identity(self):
  await self.a.cancel(self.owner,self.task.task_id)
  self.assertIsNotNone(self.local.verify(self.token))
  with self.assertRaises(bridge.SourceBridgeRejected):await self.b.register_current_source(self.token,self.task.task_id)
if __name__=='__main__':unittest.main()
