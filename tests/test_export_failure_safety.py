import ast,builtins,json,shutil,tempfile,types,unittest
from pathlib import Path
from unittest.mock import patch
import ast, math
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
class Tensor:
 def __init__(self,shape): self.shape=tuple(shape)
 def detach(self): return self
 def to(self,**kw): return self
 def contiguous(self): return self
 def clone(self): return Tensor(self.shape)
 def __getitem__(self,k): return Tensor(self.shape)
def extract(file,name,env):
 tree=ast.parse((ROOT/file).read_text())
 node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name==name)
 exec(compile(ast.Module(body=[node],type_ignores=[]),file,'exec'),env)
 return env[name]
manifest_from_config=extract('compute/qwen2_shards.py','manifest_from_config',{'math':math,'FORMAT':'opencompute-qwen2-tp-v1'})
expected_shapes=extract('compute/qwen2_safetensors.py','_expected_shapes',{})
def fixture():
 c=dict(model_type='qwen2',hidden_size=4,num_attention_heads=2,num_key_value_heads=2,num_hidden_layers=2,intermediate_size=4,vocab_size=3,max_position_embeddings=8,tie_word_embeddings=False)
 return c,{k:Tensor(v) for k,v in expected_shapes(manifest_from_config(c,1)).items()}

NOTE='Export failed; output may remain and was not automatically cleaned. 导出失败：可能残留，未自动清理。'

def function(file,name,env):
 tree=ast.parse((ROOT/file).read_text());node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name==name)
 safe=dict(vars(builtins))
 def importer(module,*a,**kw):
  if module=='cluster_v031.checkpoints':return types.SimpleNamespace(inspect_bundle=lambda *a:None)
  if module=='compute.qwen2_chunked':return types.SimpleNamespace(export_chunked=env['chunked'])
  if module=='shutil':return shutil
  raise AssertionError(module)
 safe['__import__']=importer;env.update(__builtins__=safe,Path=Path,json=json,shutil=shutil)
 exec(compile(ast.Module(body=[node],type_ignores=[]),str(file),'exec'),env);return env[name]

def chunked_env(row):
 env=dict(manifest_from_config=manifest_from_config,FORMAT='opencompute-qwen2-tp-v2',MAX_FILE_BYTES=512000000,
  torch=types.SimpleNamespace(Tensor=Tensor,float32='float32',bfloat16='bfloat16',zeros=lambda n:Tensor((n,))),
  all_files=lambda m:[],chunks=lambda m:[(0,m['vocab'])],_get_rows=row,
  _write_tensor=lambda p,*a:p.write_bytes(b'fake'),save_file=lambda obj,p:Path(p).write_bytes(b'fake'))
 return function('compute/qwen2_chunked.py','export_chunked',env)

def local_env(producer):
 env={'export_state':producer,'chunked':producer,'_inspect_directory':lambda *a,**k:({'weights':{},'plan':{'exportable_under_current_limit':True,'chunked_v2_exportable_by_size':True}},fixture()[0]),'_json_file':lambda *a:fixture()[0],'MAX_CONFIG_BYTES':10000}
 return function('compute/qwen2_safetensors.py','export_local',env)

class Tests(unittest.TestCase):
 def failure(self,kind,mode,cancel=False):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);src=root/'src';src.mkdir();out=root/'out';original=KeyboardInterrupt('cancel') if cancel else RuntimeError('producer failure')
   cause=ValueError('underlying cause');original.__cause__=cause
   def mutate():
    if mode=='none':
     if out.exists():out.rmdir()
    else:
     out.mkdir(exist_ok=True);(out/'partial').write_bytes(b'partial')
     if mode=='replace':out.rename(root/'moved');out.mkdir()
     if mode in ('replace','stranger'):(out/'keep-user').write_bytes(b'user')
    raise original
   def producer(*a,**k):return mutate()
   actual_chunk=chunked_env(lambda *a:mutate())
   if kind=='chunk':run=lambda:actual_chunk({},fixture()[0],out,1)
   elif kind=='nested':run=lambda:local_env(actual_chunk)(src,out,1,chunked=True)
   else:run=lambda:local_env(producer)(src,out,1)
   with patch.object(shutil,'rmtree',side_effect=AssertionError('recursive deletion forbidden')):
    try:run()
    except BaseException as caught:self.assertIs(caught,original)
    else:self.fail('producer did not fail')
   self.assertIs(original.__cause__,cause)
   frames=[];tb=original.__traceback__
   while tb is not None:frames.append(tb.tb_frame.f_code.co_name);tb=tb.tb_next
   self.assertIn('mutate',frames)
   if mode in ('replace','stranger'):self.assertEqual((out/'keep-user').read_bytes(),b'user')
   if mode=='replace':self.assertEqual((root/'moved/partial').read_bytes(),b'partial')
   if mode=='partial':self.assertEqual((out/'partial').read_bytes(),b'partial')
   self.assertIn(NOTE,original.__notes__)
 def test_local_replacement_red(self):self.failure('local','replace')
 def test_chunk_replacement_red(self):self.failure('chunk','replace')
 def test_remaining_failure_matrix(self):
  for kind in ('local','chunk','nested'):
   for mode in ('none','partial','stranger','replace'):
    with self.subTest(kind=kind,mode=mode):self.failure(kind,mode)
 def test_cancel_keeps_original(self):
  for kind in ('local','chunk','nested'):self.failure(kind,'stranger',True)
 def test_no_rmtree_in_real_functions(self):
  for file,name in [('compute/qwen2_safetensors.py','export_local'),('compute/qwen2_chunked.py','export_chunked')]:
   tree=ast.parse((ROOT/file).read_text());node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name==name)
   self.assertFalse(any(isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr=='rmtree' for n in ast.walk(node)))
 def test_success_ordinary_fakes(self):
  c,state=fixture();chunk=chunked_env(lambda state,key,start,stop,shape:Tensor((stop-start,shape[1])))
  with tempfile.TemporaryDirectory() as d:
   result=chunk(state,c,Path(d)/'out',1)
   self.assertEqual(json.loads((Path(d)/'out/manifest.json').read_text()),result)
   src=Path(d)/'src';src.mkdir();meta={'ok':True}
   def producer(*a,**k):return meta
   self.assertIs(local_env(producer)(src,Path(d)/'other',1),meta)

if __name__=='__main__':unittest.main()
