"""Actual consent wrapper mapping; real confirmation DB, synthetic parent permissions."""
import unittest
from validation.consent_runtime.test_wiring import Tests as Base
class Tests(Base):
 async def test_input_ready_uses_install_permission_and_parent_without_execution(self):
  view,s,b=await self.view()
  with view.hold(b,'input-ready'):pass
  self.assertEqual(self.parent.calls[-1][0],'hold');self.assertEqual(self.parent.calls[-1][1][-1],'input-ready')
  self.assertFalse(any(x[0] in ('stage0','call','finalize') for x in self.parent.calls))
  await self.consent.revoke(self.token,s.request)
  with self.assertRaises(Exception):
   with view.hold(b,'input-ready'):pass
if __name__=='__main__':
 r=unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite([Tests('test_input_ready_uses_install_permission_and_parent_without_execution')]));raise SystemExit(not r.wasSuccessful())
