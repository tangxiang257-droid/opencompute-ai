"""Exercise real runtime adapter methods with a fake Torch API; no import Torch."""
import unittest,struct
from types import SimpleNamespace
from cluster_p1.device_completion import _TorchRuntime
class Tensor:
 def __init__(self,values,device='cuda:0',dtype='f32',shape=None):self.values=values;self.device=SimpleNamespace(type=device.split(':')[0],name=device);self.device_name=device;self.dtype=dtype;self.shape=shape or (1,1,len(values));self.contiguous=True
 def is_contiguous(self):return self.contiguous
 def numel(self):return len(self.values)
 def storage_offset(self):return 0
 def untyped_storage(self):return SimpleNamespace(nbytes=lambda:len(self.values)*4)
 def detach(self):return self
 def to(self,**kwargs):
  assert kwargs==dict(device='cpu',non_blocking=False,copy=True)
  return Tensor(self.values,'cpu')
 def reshape(self,*args):return self
 def __getitem__(self,i):return SimpleNamespace(item=lambda:self.values[i])
class Device:
 def __init__(self,name):self.name=name
 def __eq__(self,other):return getattr(other,'name',None)==self.name
class Tests(unittest.TestCase):
 def runtime(self):
  r=object.__new__(_TorchRuntime);r.retained=[];self.synced=[]
  r.torch=SimpleNamespace(Tensor=Tensor,float32='f32',device=Device,__version__='pinned',version=SimpleNamespace(cuda='12'),cuda=SimpleNamespace(is_available=lambda:True,device_count=lambda:1,current_stream=lambda d:SimpleNamespace(cuda_stream=7),synchronize=self.synced.append))
  return r
 def test_copy_encode_and_fence_api(self):
  r=self.runtime();r.check('cuda:0','pinned');self.assertEqual(r.current_stream('cuda:0'),7);r.synchronize('cuda:0');self.assertEqual(self.synced,['cuda:0'])
  t=Tensor([1.5,-2.]);h=r.copy(t,'cuda:0',2);self.assertEqual(r.encode(h,2),struct.pack('<ff',1.5,-2.));self.assertEqual(r.retained,[t,h])
 def test_version_cuda_and_index_reject(self):
  r=self.runtime()
  for device,version in [('cuda:0','wrong'),('cuda:1','pinned')]:
   with self.assertRaises(Exception):r.check(device,version)
  r.torch.version.cuda=None
  with self.assertRaises(Exception):r.check('cuda:0','pinned')
 def test_tensor_format_reject(self):
  for change in [('dtype','f16'),('shape',(2,)),('contiguous',False),('device',SimpleNamespace(type='cpu',name='cpu')),('device',SimpleNamespace(type='cuda',name='cuda:1'))]:
   r=self.runtime();t=Tensor([1,2]);setattr(t,*change)
   with self.assertRaises(Exception):r.copy(t,'cuda:0',2)
 def test_copy_failure_retains_source(self):
  r=self.runtime();t=Tensor([1,2]);t.to=lambda **kw:(_ for _ in ()).throw(RuntimeError('copy'))
  with self.assertRaises(RuntimeError):r.copy(t,'cuda:0',2)
  self.assertEqual(r.retained,[t])
 def test_hidden_large_backing_reject(self):
  r=self.runtime();t=Tensor([1,2]);t.untyped_storage=lambda:SimpleNamespace(nbytes=lambda:10000000)
  with self.assertRaises(Exception):r.copy(t,'cuda:0',2)
if __name__=='__main__':unittest.main()
