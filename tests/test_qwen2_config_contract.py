"""Execute ONLY the manifest function AST, never the Torch-bearing module."""
import ast
import copy
import math
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
PATH=ROOT/'compute/qwen2_shards.py'
def load_manifest():
    tree=ast.parse(PATH.read_text())
    function=next(x for x in tree.body if isinstance(x,ast.FunctionDef) and x.name=='manifest_from_config')
    assert not function.decorator_list
    namespace={'math':math,'FORMAT':'opencompute-qwen2-tp-v1'}
    if (ROOT/'compute/qwen2_config_contract.py').exists():
        from compute.qwen2_config_contract import manifest_from_config
        namespace['_parse_manifest']=manifest_from_config
    exec(compile(ast.Module(body=[function],type_ignores=[]),str(PATH),'exec'),namespace)
    return namespace['manifest_from_config']
def config():return dict(model_type='qwen2',hidden_size=8,num_attention_heads=2,num_key_value_heads=1,num_hidden_layers=2,intermediate_size=16,vocab_size=16,max_position_embeddings=32)
# Frozen from the pre-change real manifest function; 12 valid configurations.
VALID_CASES = [({'hidden_size': 16,
   'intermediate_size': 16,
   'max_position_embeddings': 32,
   'model_type': 'qwen2',
   'num_attention_heads': 8,
   'num_hidden_layers': 3,
   'num_key_value_heads': 4,
   'rope_scaling': None,
   'tie_word_embeddings': False,
   'vocab_size': 17},
  1,
  {'format': 'opencompute-qwen2-tp-v1',
   'head_dim': 2,
   'heads': 8,
   'hidden': 16,
   'inner': 16,
   'kv_heads': 4,
   'layers': 3,
   'positions': 32,
   'rms_eps': 1e-06,
   'rope_theta': 10000.0,
   'source': 'local',
   'tied_embeddings': False,
   'vocab': 17,
   'workers': 1}),
 ({'hidden_size': 16,
   'intermediate_size': 16,
   'max_position_embeddings': 32,
   'num_attention_heads': 8,
   'num_hidden_layers': 3,
   'rope_scaling': None,
   'tie_word_embeddings': False,
   'vocab_size': 17},
  1,
  {'format': 'opencompute-qwen2-tp-v1',
   'head_dim': 2,
   'heads': 8,
   'hidden': 16,
   'inner': 16,
   'kv_heads': 8,
   'layers': 3,
   'positions': 32,
   'rms_eps': 1e-06,
   'rope_theta': 10000.0,
   'source': 'local',
   'tied_embeddings': False,
   'vocab': 17,
   'workers': 1}),
 ({'hidden_size': 16,
   'intermediate_size': 16,
   'max_position_embeddings': 32,
   'model_type': 'qwen2',
   'num_attention_heads': 8,
   'num_hidden_layers': 3,
   'num_key_value_heads': 4,
   'rope_scaling': None,
   'tie_word_embeddings': True,
   'vocab_size': 17},
  1,
  {'format': 'opencompute-qwen2-tp-v1',
   'head_dim': 2,
   'heads': 8,
   'hidden': 16,
   'inner': 16,
   'kv_heads': 4,
   'layers': 3,
   'positions': 32,
   'rms_eps': 1e-06,
   'rope_theta': 10000.0,
   'source': 'local',
   'tied_embeddings': True,
   'vocab': 17,
   'workers': 1}),
 ({'hidden_size': 16,
   'intermediate_size': 16,
   'max_position_embeddings': 32,
   'num_attention_heads': 8,
   'num_hidden_layers': 3,
   'rope_scaling': None,
   'tie_word_embeddings': True,
   'vocab_size': 17},
  1,
  {'format': 'opencompute-qwen2-tp-v1',
   'head_dim': 2,
   'heads': 8,
   'hidden': 16,
   'inner': 16,
   'kv_heads': 8,
   'layers': 3,
   'positions': 32,
   'rms_eps': 1e-06,
   'rope_theta': 10000.0,
   'source': 'local',
   'tied_embeddings': True,
   'vocab': 17,
   'workers': 1}),
 ({'hidden_size': 16,
   'intermediate_size': 16,
   'max_position_embeddings': 32,
   'model_type': 'qwen2',
   'num_attention_heads': 8,
   'num_hidden_layers': 3,
   'num_key_value_heads': 4,
   'rope_scaling': {'type': 'default'},
   'tie_word_embeddings': False,
   'vocab_size': 17},
  2,
  {'format': 'opencompute-qwen2-tp-v1',
   'head_dim': 2,
   'heads': 8,
   'hidden': 16,
   'inner': 16,
   'kv_heads': 4,
   'layers': 3,
   'positions': 32,
   'rms_eps': 1e-06,
   'rope_theta': 10000.0,
   'source': 'local',
   'tied_embeddings': False,
   'vocab': 17,
   'workers': 2}),
 ({'hidden_size': 16,
   'intermediate_size': 16,
   'max_position_embeddings': 32,
   'num_attention_heads': 8,
   'num_hidden_layers': 3,
   'rope_scaling': {'type': 'default'},
   'tie_word_embeddings': False,
   'vocab_size': 17},
  2,
  {'format': 'opencompute-qwen2-tp-v1',
   'head_dim': 2,
   'heads': 8,
   'hidden': 16,
   'inner': 16,
   'kv_heads': 8,
   'layers': 3,
   'positions': 32,
   'rms_eps': 1e-06,
   'rope_theta': 10000.0,
   'source': 'local',
   'tied_embeddings': False,
   'vocab': 17,
   'workers': 2}),
 ({'hidden_size': 16,
   'intermediate_size': 16,
   'max_position_embeddings': 32,
   'model_type': 'qwen2',
   'num_attention_heads': 8,
   'num_hidden_layers': 3,
   'num_key_value_heads': 4,
   'rope_scaling': {'type': 'default'},
   'tie_word_embeddings': True,
   'vocab_size': 17},
  2,
  {'format': 'opencompute-qwen2-tp-v1',
   'head_dim': 2,
   'heads': 8,
   'hidden': 16,
   'inner': 16,
   'kv_heads': 4,
   'layers': 3,
   'positions': 32,
   'rms_eps': 1e-06,
   'rope_theta': 10000.0,
   'source': 'local',
   'tied_embeddings': True,
   'vocab': 17,
   'workers': 2}),
 ({'hidden_size': 16,
   'intermediate_size': 16,
   'max_position_embeddings': 32,
   'num_attention_heads': 8,
   'num_hidden_layers': 3,
   'rope_scaling': {'type': 'default'},
   'tie_word_embeddings': True,
   'vocab_size': 17},
  2,
  {'format': 'opencompute-qwen2-tp-v1',
   'head_dim': 2,
   'heads': 8,
   'hidden': 16,
   'inner': 16,
   'kv_heads': 8,
   'layers': 3,
   'positions': 32,
   'rms_eps': 1e-06,
   'rope_theta': 10000.0,
   'source': 'local',
   'tied_embeddings': True,
   'vocab': 17,
   'workers': 2}),
 ({'hidden_size': 16,
   'intermediate_size': 16,
   'max_position_embeddings': 32,
   'model_type': 'qwen2',
   'num_attention_heads': 8,
   'num_hidden_layers': 3,
   'num_key_value_heads': 4,
   'rope_scaling': {'rope_type': 'default'},
   'tie_word_embeddings': False,
   'vocab_size': 17},
  4,
  {'format': 'opencompute-qwen2-tp-v1',
   'head_dim': 2,
   'heads': 8,
   'hidden': 16,
   'inner': 16,
   'kv_heads': 4,
   'layers': 3,
   'positions': 32,
   'rms_eps': 1e-06,
   'rope_theta': 10000.0,
   'source': 'local',
   'tied_embeddings': False,
   'vocab': 17,
   'workers': 4}),
 ({'hidden_size': 16,
   'intermediate_size': 16,
   'max_position_embeddings': 32,
   'num_attention_heads': 8,
   'num_hidden_layers': 3,
   'rope_scaling': {'rope_type': 'default'},
   'tie_word_embeddings': False,
   'vocab_size': 17},
  4,
  {'format': 'opencompute-qwen2-tp-v1',
   'head_dim': 2,
   'heads': 8,
   'hidden': 16,
   'inner': 16,
   'kv_heads': 8,
   'layers': 3,
   'positions': 32,
   'rms_eps': 1e-06,
   'rope_theta': 10000.0,
   'source': 'local',
   'tied_embeddings': False,
   'vocab': 17,
   'workers': 4}),
 ({'hidden_size': 16,
   'intermediate_size': 16,
   'max_position_embeddings': 32,
   'model_type': 'qwen2',
   'num_attention_heads': 8,
   'num_hidden_layers': 3,
   'num_key_value_heads': 4,
   'rope_scaling': {'rope_type': 'default'},
   'tie_word_embeddings': True,
   'vocab_size': 17},
  4,
  {'format': 'opencompute-qwen2-tp-v1',
   'head_dim': 2,
   'heads': 8,
   'hidden': 16,
   'inner': 16,
   'kv_heads': 4,
   'layers': 3,
   'positions': 32,
   'rms_eps': 1e-06,
   'rope_theta': 10000.0,
   'source': 'local',
   'tied_embeddings': True,
   'vocab': 17,
   'workers': 4}),
 ({'hidden_size': 16,
   'intermediate_size': 16,
   'max_position_embeddings': 32,
   'num_attention_heads': 8,
   'num_hidden_layers': 3,
   'rope_scaling': {'rope_type': 'default'},
   'tie_word_embeddings': True,
   'vocab_size': 17},
  4,
  {'format': 'opencompute-qwen2-tp-v1',
   'head_dim': 2,
   'heads': 8,
   'hidden': 16,
   'inner': 16,
   'kv_heads': 8,
   'layers': 3,
   'positions': 32,
   'rms_eps': 1e-06,
   'rope_theta': 10000.0,
   'source': 'local',
   'tied_embeddings': True,
   'vocab': 17,
   'workers': 4})]
class Tests(unittest.TestCase):
    def setUp(self):self.parse=load_manifest()
    def test_original_valid_configuration_matrix(self):
        for config_value,workers,expected in VALID_CASES:
            with self.subTest(workers=workers,tied=config_value['tie_word_embeddings']):
                self.assertEqual(self.parse(copy.deepcopy(config_value),workers),expected)

    def test_invalid_dimensions(self):
        for key,value in (('hidden_size',8.9),('num_key_value_heads',True),('head_dim',0),('head_dim',False),('num_hidden_layers','2')):
            with self.subTest(key=key,value=value):
                c=config();c[key]=value
                with self.assertRaises(ValueError):self.parse(c,1)
    def test_invalid_switches(self):
        for key,value in (('use_sliding_window',0),('use_sliding_window',None),('tie_word_embeddings',1),('tie_word_embeddings',0.0)):
            with self.subTest(key=key,value=value):
                c=config();c[key]=value
                with self.assertRaises(ValueError):self.parse(c,1)
    def test_layer_types(self):
        for value in (['sliding_attention','full_attention'],['unknown','full_attention'],['full_attention'],None,'full_attention'):
            with self.subTest(value=value):
                c=config();c.update(layer_types=value,use_sliding_window=False)
                with self.assertRaises(ValueError):self.parse(c,1)
    def test_valid_defaults_exact(self):
        c=config();before=copy.deepcopy(c)
        expected=dict(format='opencompute-qwen2-tp-v1',source='local',hidden=8,heads=2,kv_heads=1,head_dim=4,layers=2,inner=16,vocab=16,positions=32,workers=1,rms_eps=1e-6,rope_theta=10000.0,tied_embeddings=False)
        self.assertEqual(self.parse(c,1),expected);self.assertEqual(c,before)
        c.update(layer_types=['full_attention']*2,use_sliding_window=False,sliding_window=4096,max_window_layers=28,rope_scaling={'rope_type':'default'})
        self.assertEqual(self.parse(c,1),expected)
    def test_rope_and_sliding_stay_rejected(self):
        for key,value in (('use_sliding_window',True),('rope_scaling',{'type':'linear','factor':2.0})):
            c=config();c[key]=value
            with self.assertRaises(ValueError):self.parse(c,1)
    def test_compatible_optional_defaults(self):
        c=config();del c['model_type'];del c['num_key_value_heads']
        c.update(rope_scaling=None,tie_word_embeddings=True,head_dim=4)
        m=self.parse(c,2)
        self.assertEqual((m['kv_heads'],m['workers'],m['tied_embeddings']),(2,2,True))
        c['rope_scaling']={'type':'default'}
        self.assertEqual(self.parse(c,2),m)
    def test_workers_numbers_and_no_io(self):
        for worker in (True,1.0,0):
            with self.assertRaises(ValueError):self.parse(config(),worker)
        for key,value in (('rope_theta',True),('rms_norm_eps','0.000001'),('head_dim',None),('num_attention_heads',0)):
            c=config();c[key]=value
            with self.assertRaises(ValueError):self.parse(c,1)
        with patch('builtins.open',side_effect=AssertionError('I/O')),patch('os.open',side_effect=AssertionError('I/O')),patch('socket.socket',side_effect=AssertionError('network')):
            self.parse(config(),1)
    def test_static_actual_loader_seam(self):
        tree=ast.parse(PATH.read_text())
        imports=[n for n in tree.body if isinstance(n,ast.ImportFrom) and n.module=='compute.qwen2_config_contract']
        self.assertEqual([(a.name,a.asname) for a in imports[0].names],[('manifest_from_config','_parse_manifest')])
        function=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='manifest_from_config')
        self.assertEqual(ast.unparse(function.body[-1]),'return _parse_manifest(config, workers, source)')
        for filename in ('qwen2_safetensors.py','qwen2_reference.py','qwen2_chunked.py'):
            t=ast.parse((ROOT/'compute'/filename).read_text())
            self.assertTrue(any(isinstance(n,ast.ImportFrom) and n.module=='compute.qwen2_shards' and any(a.name=='manifest_from_config' for a in n.names) for n in t.body))
    def test_huge_real_parameters_are_value_errors(self):
        for key in ('rope_theta','rms_norm_eps'):
            with self.subTest(key=key):
                c=config();c[key]=10**1000
                with self.assertRaises(ValueError):self.parse(c,1)
if __name__=='__main__':unittest.main(verbosity=2)
