import base64,hashlib,importlib.abc,importlib.util,json,pathlib,struct,sys,unittest
ROOT=pathlib.Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT/'work'))
class Block(importlib.abc.MetaPathFinder):
 def find_spec(self,name,*args):
  if name.split('.')[0] in {'torch','transformers','safetensors','tensorflow','public_pool'} or name.endswith('.worker'):raise AssertionError('offline import denied')
def guard(event,args):
 if event in ('socket.bind','socket.connect','socket.getaddrinfo','subprocess.Popen','os.system'):raise AssertionError('offline event denied')
sys.meta_path.insert(0,Block());sys.addaudithook(guard)
class Tests(unittest.TestCase):
 def setUp(self):
  self.assertIsNotNone(importlib.util.find_spec('prefill_comparison'),'bridge API missing')
  from prefill_comparison import observe_prefill_difference
  self.f=observe_prefill_difference
 def bytes(self,values):return struct.pack('<'+'f'*len(values),*values)
 def package(self,blocks=((1,2),(3,4,5,6))):
  tiles=[];start=0
  for i,v in enumerate(blocks):
   b=self.bytes(v);count=len(v)//2
   tiles.append(dict(tile=i,start=start,count=count,shape=[1,count,2],bytes=len(b),sha256=hashlib.sha256(b).hexdigest(),data_base64=base64.b64encode(b).decode()));start+=count
  return dict(contract='prefill_logits_v1',dtype='float32-le',axes=['batch','token','vocab'],total_bytes=sum(t['bytes'] for t in tiles),tiles=tiles,package='opencompute-result-download-v1',manifest_digest='a'*64,computation_verified=False,execution_allowed=False)
 def raw(self,p):return json.dumps(p).encode()
 def runcompare(self,p=None,reference=None,shape=(1,3,2),atol=0,rtol=0):
  return self.f(self.raw(self.package() if p is None else p),self.bytes([1,2,3,4,5,6]) if reference is None else reference,reference_shape=shape,atol=atol,rtol=rtol)
 def test_full_equal(self):
  self.assertEqual(self.runcompare(),dict(element_count=6,violation_count=0,max_abs_error=0.,within_tolerance=True))
 def test_last_tile_difference(self):
  r=self.runcompare(reference=self.bytes([1,2,3,4,5,7]));self.assertEqual(r['element_count'],6);self.assertEqual(r['violation_count'],1);self.assertFalse(r['within_tolerance']);self.assertEqual(r['max_abs_error'],1)
 def test_wrong_full_shape(self):
  for shape in [(1,1,2),(3,1,2),(1,2,3),(True,3,2),[1,3,2]]:
   with self.assertRaises(ValueError):self.runcompare(shape=shape)
  with self.assertRaises(ValueError):self.runcompare(reference=self.bytes([5,6]))
 def test_bad_tiles(self):
  for change in ['reverse','gap','omit']:
   p=self.package()
   if change=='reverse':p['tiles'].reverse()
   elif change=='gap':p['tiles'][1]['start']=2
   else:p['tiles'].pop()
   with self.assertRaises(ValueError):self.runcompare(p)
 def test_self_consistent_short_shape(self):
  with self.assertRaises(ValueError):self.runcompare(self.package(((1,2),)))
 def test_nonfinite_everywhere(self):
  for n in [float('nan'),float('inf'),-float('inf')]:
   with self.assertRaises(ValueError):self.runcompare(self.package(((1,2),(3,4,5,n))))
   with self.assertRaises(ValueError):self.runcompare(reference=self.bytes([1,2,3,4,5,n]))
 def test_explicit_tolerance(self):
  with self.assertRaises(TypeError):self.f(self.raw(self.package()),self.bytes([1,2,3,4,5,6]),reference_shape=(1,3,2))
  for n in [True,-1,float('nan'),float('inf'),10**1000]:
   with self.assertRaises(ValueError):self.runcompare(atol=n)
   with self.assertRaises(ValueError):self.runcompare(rtol=n)
 def test_reference_types_limits(self):
  for r in [b'',bytearray(b'1234'),'1234',b'0'*(4194304+1)]:
   with self.assertRaises(ValueError):self.runcompare(reference=r)
 def test_malformed_fixed_error(self):
  for raw in [b'{}',b'null',b'\xff',bytearray(b'{}')]:
   with self.assertRaises(ValueError) as c:self.f(raw,b'1234',reference_shape=(1,1,1),atol=0,rtol=0)
   self.assertEqual(str(c.exception),'invalid prefill observation input')
 def test_explicit_direction(self):
  p=self.package(((2,2),));r=self.runcompare(p,reference=self.bytes([1,1]),shape=(1,1,2),rtol=.5)
  self.assertEqual(r['violation_count'],2)
 def test_exact_payload_limit(self):
  data=b'\0'*(4*1024*1024);count=len(data)//8;p=self.package(((0,0),))
  p['total_bytes']=len(data);p['tiles'][0].update(count=count,shape=[1,count,2],bytes=len(data),sha256=hashlib.sha256(data).hexdigest(),data_base64=base64.b64encode(data).decode())
  r=self.f(self.raw(p),data,reference_shape=(1,count,2),atol=0,rtol=0)
  self.assertEqual(r['element_count'],1048576);self.assertTrue(r['within_tolerance'])
if __name__=='__main__':unittest.main()
