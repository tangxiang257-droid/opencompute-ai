import ast
import builtins
from copy import deepcopy
import json
from pathlib import Path
import tempfile
import types
import unittest

SOURCE=Path(__file__).resolve().parents[1]/'compute/qwen2_safetensors.py'

def functions():
    tree=ast.parse(SOURCE.read_text())
    nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in
           ('export_local','inspect_directory','_inspect_directory')]
    env={'Path':Path,'deepcopy':deepcopy,'MAX_CONFIG_BYTES':32768}
    safe=dict(vars(builtins))
    def importer(name,*args,**kwargs):
        if name=='safetensors':return types.SimpleNamespace(safe_open=lambda *a,**k:None)
        if name=='cluster_v031.checkpoints':return types.SimpleNamespace(inspect_bundle=lambda *a:None)
        if name=='compute.qwen2_chunked':return types.SimpleNamespace(export_chunked=env['export_state'])
        raise AssertionError('unexpected import '+name)
    safe['__import__']=importer;env['__builtins__']=safe
    exec(compile(ast.Module(body=nodes,type_ignores=[]),str(SOURCE),'exec'),env)
    return env

class Tests(unittest.TestCase):
    def test_source_change_does_not_replace_checked_config(self):
        for chunked in (False,True):
            with tempfile.TemporaryDirectory() as temp:
                root=Path(temp)/'source';root.mkdir();path=root/'config.json'
                original={'revision':'checked','nested':{'v':[1]}}
                path.write_text(json.dumps(original));seen=[];env=functions()
                result={'manifest':{},'plan':{'chunked_v2_exportable_by_size':True,'exportable_under_current_limit':True},
                        'input_files':[],'tensor_count':0,'weights':object()}
                def inspected(*a,**kw):
                    config=json.loads(path.read_text());path.write_text('{"revision":"changed"}')
                    return result,deepcopy(config)
                env['_inspect_directory']=inspected
                env['inspect_directory']=lambda *a,**kw:inspected()[0]
                env['_json_file']=lambda *a:json.loads(path.read_text())
                env['export_state']=lambda w,c,*a,**kw:seen.append(c) or {}
                env['export_local'](root,Path(temp)/'output',1,chunked=chunked)
                self.assertEqual(seen,[original])
    def test_public_result_unchanged(self):
        env=functions();expected={'manifest':{},'plan':{},'input_files':[],'tensor_count':0,'weights':object()}
        env['_inspect_directory']=lambda *a,**kw:(expected,{'private':'config'})
        self.assertIs(env['inspect_directory']('unused',1),expected)
    def test_real_helper_config_independent_and_private(self):
        import re
        with tempfile.TemporaryDirectory() as temp:
            env=functions();source={'nested':{'values':[1]}}
            (Path(temp)/'model.safetensors.index.json').write_text('{}')
            env.update(MAX_INDEX_BYTES=4000000,MAX_WEIGHT_FILES=128,MAX_WEIGHT_FILE_BYTES=100,
                       SAFE_SHARD_NAME=re.compile(r'part.safetensors'),
                       _json_file=lambda path,limit:source if path.name=='config.json' else {'weight_map':{'x':'part.safetensors'}},
                       manifest_from_config=lambda c,*a,**k:{},storage_plan=lambda *a,**k:{},
                       _regular=lambda *a:None,
                       _expected_shapes=lambda m:{'x':(1,)},_optional_bias_shapes=lambda m:{},
                       LazySafetensors=lambda *a:object())
            inspected,config=env['_inspect_directory'](temp,1,verify_headers=False)
            self.assertEqual(set(inspected),{'manifest','plan','input_files','tensor_count','weights'})
            source['nested']['values'].append(2)
            self.assertEqual(config,{'nested':{'values':[1]}})
            config['nested']['values'].append(3)
            self.assertEqual(source,{'nested':{'values':[1,2]}})
    def test_no_export_reread_and_public_cli_shape(self):
        tree=ast.parse(SOURCE.read_text())
        export=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='export_local')
        calls={n.func.id for n in ast.walk(export) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name)}
        self.assertNotIn('_json_file',calls);self.assertIn('_inspect_directory',calls)
        facade=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='inspect_directory')
        self.assertEqual([a.arg for a in facade.args.kwonlyargs],['verify_headers','expected_sha256','storage_dtype'])
        main=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='main')
        self.assertIn('inspect_directory(',ast.unparse(main))
        self.assertNotIn('_inspect_directory(',ast.unparse(main))

    def test_failure_no_export(self):
        env=functions();calls=[]
        def failed(*a,**kw):raise ValueError('inspection failed')
        env['_inspect_directory']=failed;env['inspect_directory']=failed
        env['export_state']=lambda *a,**kw:calls.append(1)
        with self.assertRaises(ValueError):env['export_local']('unused','unused',1)
        self.assertEqual(calls,[])

if __name__=='__main__':unittest.main()
