import unittest
from unittest.mock import patch
from validation.hidden_host.test_pipeline import Flow
import private_b_hidden_host as module
class OriginExit(Flow):
 async def test_last_launcher_callback_cannot_change_pinned_config(self):
  h=self.hidden_host;old=h.launcher.check;calls=[]
  def count():calls.append(1);return old()
  with patch.object(h.launcher,'check',count):h._check()
  total=len(calls);self.assertGreater(total,0);calls.clear();injected=[]
  def changed():
   result=old();calls.append(1)
   if len(calls)==total:h.c['hostname']='changed.example';injected.append(True)
   return result
  with patch.object(h.launcher,'check',changed):
   with self.assertRaises(Exception):h._check()
  self.assertEqual(injected,[True])
 async def test_last_origin_callback_cannot_change_pinned_config(self):
  h=self.hidden_host;old=module._require_issued_journal;calls=[]
  def count(*a,**kw):calls.append(1);return old(*a,**kw)
  with patch.object(module,'_require_issued_journal',count):h._check()
  total=len(calls);self.assertGreater(total,0);calls.clear();injected=[]
  def changed(*a,**kw):
   result=old(*a,**kw);calls.append(1)
   if len(calls)==total:h.c['hostname']='changed.example';injected.append(True)
   return result
  with patch.object(module,'_require_issued_journal',changed):
   with self.assertRaises(Exception):h._check()
  self.assertEqual(injected,[True])
def load_tests(*a):return unittest.TestSuite(OriginExit(n) for n in OriginExit.__dict__ if n.startswith('test_'))
if __name__=='__main__':unittest.main()
