"""Ordinary configuration and local occupancy, never an execution grant."""
import unittest,tempfile,json,hashlib,os
from pathlib import Path
from unittest.mock import patch
try:from cluster_p1.role_launcher import LaunchConfig
except ImportError:LaunchConfig=None
from cluster_p1.online_issuance_transport import _json
class Tests(unittest.TestCase):
 def setUp(self):
  self.assertIsNotNone(LaunchConfig,'private launcher missing')
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
  self.d=dict(version=1,role='A',node='a',issuer_node='issuer',issuer_certificate='a'*64,coordinator_node='controller',descriptor_sha256='b'*64,root=str(self.root),charge=32,control_budget=64)
 def make(self,d=None):
  raw=_json(self.d if d is None else d);return LaunchConfig(raw,hashlib.sha256(raw).hexdigest())
 def test_configuration_requires_independent_pin(self):
  with self.assertRaises(Exception):LaunchConfig(_json(self.d),'0'*64)
 def test_unknown_fields_and_bool_numbers_denied(self):
  for d in [dict(self.d,module='evil'),dict(self.d,version=True),dict(self.d,charge=True),dict(self.d,control_budget=1.5),dict(self.d,input_digest='e'*64)]:
   with self.subTest(d=d),self.assertRaises(Exception):self.make(d)
 def test_duplicate_keys_and_noncanonical_bytes_denied(self):
  for raw in [b'{"version":1,"version":1}',json.dumps(self.d).encode()]:
   with self.assertRaises(Exception):LaunchConfig(raw,hashlib.sha256(raw).hexdigest())
 def test_symlink_root_denied(self):
  link=self.root/'link';link.symlink_to(self.root,target_is_directory=True)
  with self.assertRaises(Exception):self.make(dict(self.d,root=str(link)))
 def test_claim_permanent_no_resume(self):
  c=self.make();c.claim();raw=c.path('claim').read_bytes()
  with self.assertRaises(FileExistsError):self.make().claim()
  self.assertEqual(c.path('claim').read_bytes(),raw)
 def test_claim_failure_preserves_occupancy(self):
  c=self.make()
  with patch('os.fsync',side_effect=OSError('unknown durable write')):
   with self.assertRaises(OSError):c.claim()
  with self.assertRaises(FileExistsError):self.make().claim()
 def test_root_replacement_denied(self):
  c=self.make();old=self.root/'old';old.mkdir()
  with patch.object(c,'_inode',(-1,-1)):
   with self.assertRaises(Exception):c.check()
 def test_process_change_denied(self):
  c=self.make()
  with patch('os.getpid',return_value=c.pid+1):
   with self.assertRaises(Exception):c.check()
 def test_descriptor_and_role_substitution_denied(self):
  d=dict(node='a',stage=0);self.d['descriptor_sha256']=hashlib.sha256(_json(d)).hexdigest();c=self.make();c.bound(d)
  for fake in [dict(d,node='b'),dict(d,stage=1)]:
   with self.assertRaises(Exception):c.bound(fake)
 def test_config_property_returns_copy(self):
  c=self.make();v=c.data;v['node']='evil';self.assertEqual(c.data['node'],'a')
 def test_paths_not_selected_by_peer(self):
  c=self.make()
  with self.assertRaises(Exception):c.path('../other')
if __name__=='__main__':unittest.main()
