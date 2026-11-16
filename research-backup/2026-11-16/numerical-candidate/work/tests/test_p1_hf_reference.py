"""Independent, download-free Transformers architecture oracle."""
import os
import hashlib
import json
import tempfile
from pathlib import Path
import unittest
import torch
import importlib
from safetensors.torch import save_file
from compute.qwen2_tiled_stage import TiledQwen2Stage

def local_transformers_qwen2():
    try:
        module = importlib.import_module('transformers')
        return module.Qwen2Config, module.Qwen2ForCausalLM
    except ImportError as exc:
        if os.environ.get('P1_REQUIRE_HF') == '1':
            raise RuntimeError('required Transformers Qwen2 oracle unavailable') from exc
        raise unittest.SkipTest('optional Transformers absent; dedicated P1 CI requires it') from exc


class HFReference(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.Config, cls.Model = local_transformers_qwen2()

    @torch.inference_mode()
    def test_boundary_logits_tiles_and_hf_cached_decode(self):
        torch.manual_seed(71)
        cfg=self.Config(vocab_size=41,hidden_size=24,intermediate_size=48,num_hidden_layers=3,num_attention_heads=6,num_key_value_heads=2,max_position_embeddings=32,attention_dropout=0.,tie_word_embeddings=False)
        cfg._attn_implementation='eager'
        model=self.Model(cfg).eval()
        state=model.state_dict(); config=cfg.to_dict()
        ids=torch.tensor([[1,5,9,2,7,3,8]])
        expected=model(ids,output_hidden_states=True,use_cache=True)
        stages=[TiledQwen2Stage(state,config,stage_id=i,split_layer=1,device='cpu') for i in (0,1)]
        for cuts in ([7],[2,2,2,1]):
            for s in stages:s.reset()
            hs=[];ys=[];start=0
            for n in cuts:
                h=stages[0].prefill(ids[:,start:start+n],token_start=start,stage_id=0)
                hs.append(h);ys.append(stages[1].prefill(h,token_start=start,stage_id=1));start+=n
            torch.testing.assert_close(torch.cat(hs,1),expected.hidden_states[1],atol=3e-5,rtol=3e-5)
            torch.testing.assert_close(torch.cat(ys,1),expected.logits,atol=3e-5,rtol=3e-5)
            cache=expected.past_key_values
            for pos,token in enumerate((4,6,10),7):
                x=torch.tensor([[token]])
                oracle=model(x,past_key_values=cache,use_cache=True);cache=oracle.past_key_values
                h=stages[0].decode(x,token_start=pos,stage_id=0)
                result=stages[1].decode(h,token_start=pos,stage_id=1)
                torch.testing.assert_close(result,oracle.logits,atol=3e-5,rtol=3e-5)
            expected=model(ids,output_hidden_states=True,use_cache=True)
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'config.json').write_text(json.dumps(config))
            save_file({k:v.contiguous() for k,v in state.items()},str(root/'model.safetensors'))
            config_pin=hashlib.sha256((root/'config.json').read_bytes()).hexdigest()
            pin=hashlib.sha256((root/'model.safetensors').read_bytes()).hexdigest()
            a=TiledQwen2Stage.from_safetensors(root,expected_config_sha256=config_pin,expected_sha256=pin,stage_id=0,split_layer=1,device='cpu')
            b=TiledQwen2Stage.from_safetensors(root,expected_config_sha256=config_pin,expected_sha256=pin,stage_id=1,split_layer=1,device='cpu')
            self.assertEqual(len(a.layers),1)
            self.assertEqual(len(b.layers),2)
            torch.testing.assert_close(a.prefill(ids,token_start=0,stage_id=0),expected.hidden_states[1],atol=3e-5,rtol=3e-5)
            a.reset()
            boundary=a.prefill(ids,token_start=0,stage_id=0)
            torch.testing.assert_close(b.prefill(boundary,token_start=0,stage_id=1),expected.logits,atol=3e-5,rtol=3e-5)
            with self.assertRaises(ValueError):TiledQwen2Stage.from_safetensors(root,expected_config_sha256=config_pin,expected_sha256='0'*64,stage_id=0,split_layer=1,device='cpu')
            changed=dict(config);changed['rope_theta']=20000.0
            (root/'config.json').write_text(json.dumps(changed))
            with self.assertRaises(ValueError):TiledQwen2Stage.from_safetensors(root,expected_config_sha256=config_pin,expected_sha256=pin,stage_id=0,split_layer=1,device='cpu')
            (root/'config.json').write_text(json.dumps(config))
            with (root/'model.safetensors').open('ab') as stream:stream.write(b'tamper')
            with self.assertRaises(ValueError):TiledQwen2Stage.from_safetensors(root,expected_config_sha256=config_pin,expected_sha256=pin,stage_id=1,split_layer=1,device='cpu')


class HFDependencyContract(unittest.TestCase):
    def test_absence_skips_only_when_not_required(self):
        from unittest.mock import patch
        with patch('importlib.import_module', side_effect=ImportError('absent')):
            with patch.dict(os.environ, {'P1_REQUIRE_HF': '0'}):
                with self.assertRaises(unittest.SkipTest):
                    local_transformers_qwen2()
            with patch.dict(os.environ, {'P1_REQUIRE_HF': '1'}):
                with self.assertRaises(RuntimeError):
                    local_transformers_qwen2()
