"""Real fallocate/fsync ordinary files; fault injections are not power-loss tests."""
import os,unittest
from unittest.mock import patch
from validation.logits_spool import test_prototype as fixture
class Tests(unittest.TestCase):
 setUp=fixture.Tests.setUp
 plan=fixture.Tests.plan
 setup=fixture.Tests.setup
 body=fixture.Tests.body
 def test_preallocated_final_inode_before_body(self):
  x,s,d,p=self.setup();seen=[]
  def body(write):
   st=x.path.stat();self.assertEqual(st.st_size,p.total);seen.append(st.st_ino)
   self.body(p)(write)
  x.produce(body)
  self.assertEqual(x.path.stat().st_ino,seen[0]);self.assertEqual(len(b''.join(f.block for f in x.frames())),p.total)
 def test_missing_fallocate_denies_without_body_keeps_fee(self):
  x,s,d,p=self.setup();called=[]
  with patch.object(os,'posix_fallocate',None):
   with self.assertRaises(Exception):x.produce(lambda write:called.append(1))
  self.assertEqual(called,[]);self.assertEqual(s.used(),p.charge);self.assertEqual(d.used(),p.charge)
 def test_partial_fallocate_failure_keeps_final_file_and_isolated_fee(self):
  x,s,d,p=self.setup();called=[];real=os.posix_fallocate
  def fail(fd,offset,size):real(fd,offset,max(4,size//2));raise OSError('injected partial allocation')
  with patch.object(os,'posix_fallocate',fail):
   with self.assertRaises(OSError):x.produce(lambda w:called.append(1))
  self.assertEqual(called,[]);self.assertTrue(x.path.exists());self.assertGreater(x.path.stat().st_size,0)
  self.assertEqual(s.record('r')['state'],'ISOLATED');self.assertEqual(s.used(),p.charge)
 def test_directory_replacement_after_allocation_denies(self):
  x,s,d,p=self.setup()
  def hook(phase):
   if phase=='allocated':x.root.rename(self.root/'original');x.root.mkdir()
  x.hook=hook
  with self.assertRaises(Exception):x.produce(self.body(p))
  self.assertEqual(s.used(),p.charge)
 def test_push_sink_reuses_preallocated_inode(self):
  from cluster_p1.private_one_token import _PushSpool
  from cluster_p1.projection_driver import Spec
  import hashlib
  p=self.plan((1,),3);x,s,d,p=self.setup(p)
  spec=Spec('owner','task','nonce',1,'a'*64,'b'*64,'c'*64,'e'*64,'f'*64,'d'*64,4,3,('d'*64,))
  sink=_PushSpool(x,spec);self.addCleanup(sink.quarantine)
  before=x.path.stat();self.assertEqual(before.st_size,12)
  block=bytes(12);sink.append(0,0,block);digest=hashlib.sha256(block).hexdigest();sink.seal(0,digest);sink.enable_read()
  self.assertEqual(x.path.stat().st_ino,before.st_ino);self.assertEqual(b''.join(f.block for f in x.frames()),block)
 def test_file_and_both_directories_fsynced_before_body(self):
  x,s,d,p=self.setup();seen=[];real=os.fsync
  def fsync(fd):seen.append(os.fstat(fd).st_mode);return real(fd)
  def body(w):
   import stat
   self.assertEqual(len(seen),3);self.assertTrue(stat.S_ISREG(seen[0]));self.assertTrue(all(stat.S_ISDIR(m) for m in seen[1:]));self.body(p)(w)
  with patch.object(os,'fsync',fsync):x.produce(body)
 def test_rebuilt_spool_cannot_retry_partial_failure(self):
  from cluster_p1.byte_spool import Spool
  x,s,d,p=self.setup()
  with patch.object(os,'posix_fallocate',side_effect=OSError('injected failure')):
   with self.assertRaises(OSError):x.produce(self.body(p))
  inode=x.path.stat().st_ino;y=Spool(x.root,s,d,'r',p)
  with self.assertRaises(Exception):y.produce(self.body(p))
  self.assertEqual(x.path.stat().st_ino,inode);self.assertEqual(s.used(),p.charge)
 def test_parent_directory_replacement_denies_before_body(self):
  from pathlib import Path
  parent=self.root/'owned';parent.mkdir();old=self.root;self.root=parent
  x,s,d,p=self.setup();self.root=old
  parent.rename(old/'old-parent');parent.mkdir();called=[]
  with self.assertRaises(Exception):x.produce(lambda w:called.append(1))
  self.assertEqual(called,[]);self.assertEqual(s.used(),p.charge)
 def test_directory_symlink_is_not_accepted(self):
  x,s,d,p=self.setup();other=self.root/'other';other.mkdir();x.root.symlink_to(other,target_is_directory=True)
  with self.assertRaises(Exception):x.produce(self.body(p))
  self.assertEqual(list(other.iterdir()),[]);self.assertEqual(s.used(),p.charge)
 def test_budget_rejects_before_any_allocation(self):
  from cluster_p1.byte_spool import Book
  p=self.plan();b=Book(self.root/'small.db',p.charge-1);self.addCleanup(b.close)
  with patch.object(os,'posix_fallocate') as allocate:
   with self.assertRaises(Exception):b.reserve('r',p,'source')
  allocate.assert_not_called();self.assertEqual(b.used(),0)
 def test_legacy_manifest_remains_readable_without_allocation_claim(self):
  import json
  from cluster_p1.byte_spool import Spool,canonical
  x,s,d,p=self.setup();x.produce(self.body(p))
  manifest=json.loads(s.record('r')['manifest']);manifest.pop('directory_identity')
  with s.tx() as c:c.execute('UPDATE reservation SET manifest=? WHERE id=?',(canonical(manifest),'r'))
  rebuilt=Spool(x.root,s,d,'r',p)
  self.assertEqual(len(b''.join(f.block for f in rebuilt.frames())),p.total)
 def test_allocation_and_retirement_failure_preserve_primary_error(self):
  x,s,d,p=self.setup()
  with patch.object(os,'posix_fallocate',side_effect=OSError('allocation failed')),patch.object(s,'isolate',side_effect=RuntimeError('retirement failed')):
   with self.assertRaises(BaseExceptionGroup) as caught:x.produce(self.body(p))
  def leaves(error):
   return [str(error)] if not isinstance(error,BaseExceptionGroup) else [s for e in error.exceptions for s in leaves(e)]
  self.assertIn('allocation failed',leaves(caught.exception));self.assertIn('retirement failed',leaves(caught.exception))
  self.assertTrue(x.retired);self.assertEqual(s.used(),p.charge);self.assertTrue(x.path.exists())
if __name__=='__main__':unittest.main()
