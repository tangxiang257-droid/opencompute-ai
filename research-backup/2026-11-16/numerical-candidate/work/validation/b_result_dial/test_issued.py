import asyncio,unittest
from unittest.mock import patch
from validation.b_result_dial.test_failures import Cases,module
from cluster_p1.result_dial import BResultDial
class Issued(Cases):
 async def test_removed_original_issuance_rejects_before_dial(self):
  b,c=await self.candidate();online=b.journal._online_issuer;row=online.issued.pop(id(b.journal));calls=[]
  async def opened(*a,**kw):calls.append(1);raise OSError('LAB no network')
  try:
   with patch.object(module,'load_context',self.context),patch.object(module.asyncio,'open_connection',opened):
    with self.assertRaises(Exception):await b.prepare(enabled=True)
   self.assertEqual(calls,[],'unregistered journal must reject before TLS dialing')
  finally:online.issued[id(b.journal)]=row
 async def test_removed_issued_after_qualification_denies_local_leaf(self):
  b,c=await self.candidate();b._check();online=b.journal._online_issuer;row=online.issued[id(b.journal)]
  self.assertIs(b.issued_handle,row[1]);online.issued.pop(id(b.journal));self.assertNotIn(id(b.journal),online.issued)
  try:
   with self.assertRaises(Exception):b._leaf()
  finally:online.issued[id(b.journal)]=row
def load_tests(*args):return unittest.TestSuite(Issued(n) for n in Issued.__dict__ if n.startswith('test_'))
if __name__=='__main__':unittest.main()
