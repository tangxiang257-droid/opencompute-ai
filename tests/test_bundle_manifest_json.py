import hashlib,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from cluster_v031 import checkpoints as c

class Tests(unittest.TestCase):
 def run_case(self,raw,meta):
  with tempfile.TemporaryDirectory() as d:
   p=Path(d);(p/'manifest.json').write_bytes(raw)
   (p/'rank0_io.pt').write_bytes(b'ordinary-io');(p/'rank_0.pt').write_bytes(b'ordinary-rank')
   return c.inspect_bundle(p,meta)
 def meta(self):return dict(format='opencompute-qwen2-tp-v1',workers=1,hidden=4,heads=2,kv_heads=2,head_dim=2,layers=2,positions=8,inner=4,vocab=3,rms_eps=1e-6,rope_theta=10000.,tied_embeddings=False,source='local')
 def test_duplicate_workers_rejected(self):
  meta=self.meta();raw=json.dumps(meta).replace('"workers": 1','"workers": 2, "workers": 1').encode()
  with self.assertRaises(ValueError):self.run_case(raw,meta)
 def test_nested_escaped_duplicate_rejected(self):
  meta=self.meta();meta['extra']={'x':1}
  raw=json.dumps(meta).replace('"x": 1','"x": 2, "\\u0078": 1').encode()
  with self.assertRaises(ValueError):self.run_case(raw,meta)
 def test_nonfinite_extra_rejected(self):
  for value in ('Infinity','1e999'):
   meta=self.meta();meta['extra']=float('inf');raw=json.dumps(meta).replace('Infinity',value).encode()
   with self.assertRaises(ValueError):self.run_case(raw,meta)
 def test_second_read_growth_bounded(self):
  meta=self.meta();raw=json.dumps(meta).encode();real=c.inspect_file
  with tempfile.TemporaryDirectory() as d:
   p=Path(d);(p/'manifest.json').write_bytes(raw);(p/'rank0_io.pt').write_bytes(b'x');(p/'rank_0.pt').write_bytes(b'y')
   def inspect(path,**kw):
    result=real(path,**kw)
    if path.name=='rank_0.pt':(p/'manifest.json').write_bytes(raw+b' '*(c.MAX_MANIFEST_BYTES+1))
    return result
   with patch.object(c,'inspect_file',side_effect=inspect),self.assertRaises(ValueError):c.inspect_bundle(p,meta)
 def test_replaced_manifest_digest_matches_parsed_bytes(self):
  meta=self.meta();new=json.dumps(meta).encode();old=new+b' '
  real=c.inspect_file
  with tempfile.TemporaryDirectory() as d:
   p=Path(d);(p/'manifest.json').write_bytes(old);(p/'rank0_io.pt').write_bytes(b'x');(p/'rank_0.pt').write_bytes(b'y')
   def inspect(path,**kw):
    result=real(path,**kw)
    if path.name=='rank_0.pt':
     replacement=p/'replacement';replacement.write_bytes(new);replacement.replace(p/'manifest.json')
    return result
   with patch.object(c,'inspect_file',side_effect=inspect):result=c.inspect_bundle(p,meta)
   self.assertEqual(result['manifest.json']['sha256'],hashlib.sha256(new).hexdigest())
   self.assertEqual(result['manifest.json']['size'],len(new))
 def test_historical_digest_rejects_replaced_manifest(self):
  meta=self.meta();raw=json.dumps(meta).encode();real=c.inspect_file
  with tempfile.TemporaryDirectory() as d:
   p=Path(d);(p/'manifest.json').write_bytes(raw);(p/'rank0_io.pt').write_bytes(b'x');(p/'rank_0.pt').write_bytes(b'y')
   expected=c.inspect_bundle(p,meta)
   def inspect(path,**kw):
    result=real(path,**kw)
    if path.name=='rank_0.pt':(p/'manifest.json').write_bytes(raw+b' ')
    return result
   with patch.object(c,'inspect_file',side_effect=inspect),self.assertRaises(ValueError):c.inspect_bundle(p,meta,expected=expected)
 def test_valid_noncanonical_and_original_digest(self):
  meta=self.meta();meta['extra']={'unicode':'中文'}
  raw=(' \n'+json.dumps(meta,indent=3,ensure_ascii=False)+'\n ').encode()
  result=self.run_case(raw,meta)
  self.assertEqual(result['manifest.json']['sha256'],hashlib.sha256(raw).hexdigest())
 def test_bad_utf8_bom_object(self):
  meta=self.meta()
  for raw in (b'\xff',b'\xef\xbb\xbf'+json.dumps(meta).encode(),b'[]'):
   with self.assertRaises(ValueError):self.run_case(raw,meta)

if __name__=='__main__':unittest.main()
