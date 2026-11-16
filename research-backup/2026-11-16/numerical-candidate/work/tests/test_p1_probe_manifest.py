"""Pure JSON/ordinary fixed bytes. No DB, socket, certificate, model or executor."""
import copy,hashlib,json,struct,unittest,sys,importlib.abc
from unittest.mock import patch
from cluster_p1.contracts import Task,Session,Stage
from cluster_p1.tile_contract_v2 import TileV2
try:
 from cluster_p1.receipt_probe_manifest import validate_probe_manifest,fixture_frames,ProbeManifestRejected
except ImportError:validate_probe_manifest=fixture_frames=ProbeManifestRejected=None

def _offline_guard(event,args):
 if event in ('socket.bind','socket.connect','socket.getaddrinfo','subprocess.Popen','os.system'):
  raise AssertionError('offline manifest tests prohibit '+event)
class _NoFramework(importlib.abc.MetaPathFinder):
 def find_spec(self,name,*args):
  if name.split('.')[0] in {'torch','transformers','safetensors','public_pool'}:
   raise AssertionError('offline manifest test prohibits framework import')
sys.addaudithook(_offline_guard);sys.meta_path.insert(0,_NoFramework())

def probe_fixture():
 h='a'*64;nonce='1'*32
 task=Task(task_id='t',owner_id='owner',model_hash=h,total_layers=3,payload_ref='fixed-probe-bytes-not-model-output')
 session=Session(session_id=nonce,task_id='t',owner_id='owner',epoch=1,model_hash=h,layout_hash=h,expected_tokens=5,selected_nodes=('a','b'))
 stages=(Stage(session_id=nonce,stage_id=0,layer_start=0,layer_end=1,model_hash=h,layout_hash=h,assigned_node='a'),Stage(session_id=nonce,stage_id=1,layer_start=1,layer_end=3,model_hash=h,layout_hash=h,assigned_node='b'))
 tiles=[]
 for tid,start,n in [(0,0,2),(1,2,2),(2,4,1)]:
  for stage in (0,1):
   inp={'role':'token_ids','dtype':'int64','shape':(1,n)} if stage==0 else {'role':'hidden_states','dtype':'float32','shape':(1,n,24)}
   out={'role':'hidden_states','dtype':'float32','shape':(1,n,24)} if stage==0 else {'role':'full_logits','dtype':'float32','shape':(1,n,32)}
   tiles.append(TileV2(schema_version=2,task_id='t',owner_id='owner',session_id=nonce,epoch=1,model_hash=h,layout_hash=h,stage_id=stage,tile_id=tid,token_start=start,token_count=n,input=inp,output=out).model_dump(mode='json'))
 return dict(schema_version=1,task=task.model_dump(mode='json'),session=session.model_dump(mode='json'),stages=[x.model_dump(mode='json') for x in stages],tiles=tiles,config_sha256='c'*64,sender_node='a',receiver_node='b',sender_certificate_sha256=hashlib.sha256(b'cert-a').hexdigest(),receiver_certificate_sha256=hashlib.sha256(b'cert-b').hexdigest(),expected_server_name='receiver.test',fixture_sha256=hashlib.sha256(struct.pack('<f',1.0)*120).hexdigest(),budget=dict(hidden_size=24,vocab_size=32,token_count=5,tile_count=3,deliveries=4,receipts=3,unique_payload_bytes=480,transmitted_payload_bytes=672,capacity_bytes=65536,capacity_tiles=1))

fixture_manifest=probe_fixture

def encode_manifest(data):return json.dumps(data,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def validate(data):
 raw=encode_manifest(data);return validate_probe_manifest(raw,trusted_manifest_sha256=hashlib.sha256(raw).hexdigest())

class Tests(unittest.TestCase):
 def setUp(self):
  self.assertIsNotNone(validate_probe_manifest,'probe manifest interface missing');self.data=fixture_manifest()
 def reject(self,d=None):
  with self.assertRaises(ValueError):validate(self.data if d is None else d)
 def test_fixture_and_complete_immutable_records(self):
  m=validate(self.data);frames=fixture_frames(m)
  self.assertEqual([len(x.payload) for x in frames],[192,192,96]);self.assertEqual(sum(len(x.payload) for x in frames),480)
  self.assertEqual(sum(len(x.payload) for x in (*frames,frames[0])),672)
  self.assertEqual(hashlib.sha256(b''.join(x.payload for x in frames)).hexdigest(),m.fixture_sha256)
  self.assertTrue(all(x.payload==struct.pack('<f',1.0)*(len(x.payload)//4) for x in frames))
  self.assertEqual([x.layout_version for x in frames],[2,2,2])
  with self.assertRaises(ValueError):m.sender_node='other'
 def test_distinct_role_certificate_fingerprints(self):
  self.data['receiver_certificate_sha256']=self.data['sender_certificate_sha256'];self.reject()
 def test_fixture_complete_frame_budget(self):
  m=validate(self.data)
  with patch('cluster_p1.receipt_probe_manifest.Envelope.encode',return_value=b'x'*65537):
   with self.assertRaises(ValueError):fixture_frames(m)
 def test_wrong_trusted_hash(self):
  with self.assertRaises(ValueError):validate_probe_manifest(encode_manifest(self.data),trusted_manifest_sha256='0'*64)
 def test_no_self_declared_trust(self):self.data['trusted_manifest_sha256']='a'*64;self.reject()
 def test_missing_full_task_default(self):del self.data['task']['privacy_level'];self.reject()
 def test_missing_full_resource_limit(self):del self.data['task']['resource_limits']['cpu_fraction'];self.reject()
 def test_unknown_nested_field(self):self.data['tiles'][0]['input']['private_key']='NO';self.reject()
 def test_boolean_version(self):self.data['schema_version']=True;self.reject()
 def test_boolean_budget(self):self.data['budget']['capacity_tiles']=True;self.reject()
 def test_string_number(self):self.data['session']['epoch']='1';self.reject()
 def test_float_integer(self):self.data['session']['epoch']=1.0;self.reject()
 def test_duplicate_key(self):
  raw=encode_manifest(self.data).replace(b'"schema_version":1',b'"schema_version":1,"schema_version":1',1)
  with self.assertRaises(ValueError):validate_probe_manifest(raw,trusted_manifest_sha256=hashlib.sha256(raw).hexdigest())
 def test_nonfinite(self):
  raw=encode_manifest(self.data).replace(b'"cost_cap":0.0',b'"cost_cap":NaN')
  with self.assertRaises(ValueError):validate_probe_manifest(raw,trusted_manifest_sha256=hashlib.sha256(raw).hexdigest())
 def test_layout_mismatch(self):self.data['tiles'][1]['layout_hash']='b'*64;self.reject()
 def test_fixture_hash_mismatch(self):self.data['fixture_sha256']='d'*64;self.reject()
 def test_fixed_tile_sizes(self):
  self.data['tiles'][0]['token_count']=1;self.reject()
 def test_wrong_node(self):self.data['receiver_node']='other';self.reject()
 def test_reversed_selected_nodes(self):self.data['session']['selected_nodes'].reverse();self.reject()
 def test_nonce_length(self):self.data['session']['session_id']='s';self.reject()
 def test_unsafe_server_name(self):
  for value in ('https://probe.test.invalid','probe.test.invalid/path','-bad.invalid','UPPER.invalid','bad name'):
   self.data['expected_server_name']=value;self.reject()
 def test_nonqueued_task(self):self.data['task']['state']='running';self.reject()
 def test_nonpreparing_session(self):self.data['session']['state']='running';self.reject()
 def test_fixed_budget(self):self.data['budget']['capacity_tiles']=8;self.reject()
 def test_bytes_bound(self):
  raw=b' '*65537
  with self.assertRaises(ValueError):validate_probe_manifest(raw,trusted_manifest_sha256=hashlib.sha256(raw).hexdigest())
 def test_private_paths_not_allowed(self):self.data['journal_path']='/tmp/unsafe';self.reject()
if __name__=='__main__':unittest.main(verbosity=2)
