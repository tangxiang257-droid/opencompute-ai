"""The test loader must remain a strict gate, not learn arbitrary imports."""
import pathlib,unittest
from validation.controlled_authority import fixtures as f
class Checks(unittest.TestCase):
 def raw(self):return (f.ROOT/'snapshot_task_ledger.py').read_bytes()
 def check(self,raw):return f._check_imports('snapshot_task_ledger',raw,f.ALLOWED['snapshot_task_ledger'])
 def test_current_seven_exact_dependencies(self):
  for name,allowed in f.ALLOWED.items():f._check_imports(name,(f.ROOT/(name+'.py')).read_bytes(),allowed)
 def test_unreviewed_import_still_refuses(self):
  for added in (b'\nimport torch\n',b'\nfrom math import sin\n'):
   with self.assertRaises(AssertionError):self.check(self.raw()+added)
 def test_moved_local_import_refuses(self):
  raw=self.raw().replace(b'            from result_artifacts import _Ledger',b'            pass')
  with self.assertRaises(AssertionError):self.check(b'from result_artifacts import _Ledger\n'+raw)
 def test_alias_and_module_import_refuse(self):
  for new in (b'from result_artifacts import _Ledger as Other',b'import result_artifacts'):
   raw=self.raw().replace(b'from result_artifacts import _Ledger',new)
   with self.assertRaises(AssertionError):self.check(raw)
if __name__=='__main__':unittest.main()
