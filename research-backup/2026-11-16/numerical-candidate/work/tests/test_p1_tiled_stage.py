"""Tile/cache contracts; mutations must occur only after successful compute."""
import unittest
import torch
from compute.qwen2_shards import synthetic_state
from compute.qwen2_tiled_stage import TiledQwen2Stage

class TileContracts(unittest.TestCase):
    def setUp(self):
        self.state, self.config = synthetic_state(layers=3, positions=8)
        self.a = TiledQwen2Stage(self.state, self.config, stage_id=0, split_layer=1, device='cpu')
        self.b = TiledQwen2Stage(self.state, self.config, stage_id=1, split_layer=1, device='cpu')

    def test_joint_tiles_decode_reset_and_no_future(self):
        ids = torch.tensor([[1,2,3,4,5]])
        def run(a,b,x,start):
            h=a.prefill(x, token_start=start, stage_id=0)
            return b.prefill(h, token_start=start, stage_id=1)
        full=run(self.a,self.b,ids,0)
        self.a.reset(); self.b.reset()
        pieces=[run(self.a,self.b,ids[:,:2],0), run(self.a,self.b,ids[:,2:4],2),run(self.a,self.b,ids[:,4:],4)]
        torch.testing.assert_close(torch.cat(pieces,1),full,atol=2e-6,rtol=2e-6)
        self.assertEqual(self.a.cache_length,5)
        h=self.a.decode(torch.tensor([[6]]),token_start=5,stage_id=0)
        out=self.b.decode(h,token_start=5,stage_id=1)
        self.a.reset();self.b.reset()
        expected=run(self.a,self.b,torch.tensor([[1,2,3,4,5,6]]),0)
        torch.testing.assert_close(out,expected[:,-1:],atol=2e-6,rtol=2e-6)
        self.a.reset();self.b.reset()
        altered=run(self.a,self.b,torch.tensor([[1,2,3,9,9]]),0)
        torch.testing.assert_close(altered[:,:3],full[:,:3])

    def test_invalid_calls_preserve_cache(self):
        self.a.prefill(torch.tensor([[1,2]]),token_start=0,stage_id=0)
        before=[(k.clone(),v.clone()) for k,v in self.a.caches]
        cases=[(torch.tensor([[3]]),0,0),(torch.tensor([[3]]),3,0),(torch.tensor([[3]]),2,1),(torch.tensor([[99]]),2,0),(torch.ones(1,1),2,0),(torch.ones(2,1,dtype=torch.long),2,0),(torch.ones(1,7,dtype=torch.long),2,0)]
        for value,start,stage in cases:
            with self.assertRaises(ValueError): self.a.prefill(value,token_start=start,stage_id=stage)
            self.assertEqual(self.a.cache_length,2)
            for old,new in zip(before,self.a.caches):
                for x,y in zip(old,new):torch.testing.assert_close(x,y)
        with self.assertRaises(ValueError): self.b.prefill(torch.ones(1,2,24,dtype=torch.float64),token_start=0,stage_id=1)
        with self.assertRaises(ValueError): self.a.decode(torch.tensor([[1,2]]),token_start=2,stage_id=0)

    def test_metadata_and_device(self):
        for stage,split in [(2,1),(0,0),(1,3)]:
            with self.assertRaises(ValueError):TiledQwen2Stage(self.state,self.config,stage_id=stage,split_layer=split,device='cpu')
        bad=dict(self.state);bad['model.layers.0.self_attn.q_proj.weight']=torch.zeros(1)
        with self.assertRaises(ValueError):TiledQwen2Stage(bad,self.config,stage_id=0,split_layer=1,device='cpu')
        if not torch.cuda.is_available():
            with self.assertRaises(ValueError):TiledQwen2Stage(self.state,self.config,stage_id=0,split_layer=1,device='cuda')

    @unittest.skipUnless(torch.cuda.is_available(),'PENDING: real CUDA device unavailable')
    def test_real_cuda(self):
        a=TiledQwen2Stage(self.state,self.config,stage_id=0,split_layer=1,device='cuda')
        x=torch.tensor([[1,2]],device='cuda')
        torch.testing.assert_close(a.prefill(x,token_start=0,stage_id=0).cpu(),self.a.prefill(x.cpu(),token_start=0,stage_id=0),atol=3e-5,rtol=3e-5)

class OwnershipAndFailure(unittest.TestCase):
    def test_only_owned_weights_are_read_and_missing_weight_rejected(self):
        from collections.abc import Mapping
        state,config=synthetic_state(layers=4)
        class Tracking(Mapping):
            def __init__(self):self.read=[]
            def __iter__(self):return iter(state)
            def __len__(self):return len(state)
            def __getitem__(self,key):self.read.append(key);return state[key]
        for stage in (0,1):
            source=Tracking()
            TiledQwen2Stage(source,config,stage_id=stage,split_layer=2,device='cpu')
            peer=(2,3) if stage==0 else (0,1)
            self.assertFalse(any(key.startswith(tuple(f'model.layers.{i}.' for i in peer)) for key in source.read))
        del state['model.layers.0.self_attn.q_proj.weight']
        with self.assertRaises(ValueError):TiledQwen2Stage(state,config,stage_id=0,split_layer=2,device='cpu')

    def test_nonfinite_compute_does_not_commit(self):
        state,config=synthetic_state()
        stage=TiledQwen2Stage(state,config,stage_id=1,split_layer=1,device='cpu')
        stage.prefill(torch.ones(1,2,24),token_start=0,stage_id=1)
        before=[(k.clone(),v.clone()) for k,v in stage.caches]
        # Inject finite but overflowing final weights to exercise post-math failure.
        stage.norm.fill_(3e38)
        stage.head.fill_(3e38)
        with self.assertRaises(ValueError):stage.prefill(torch.ones(1,1,24),token_start=2,stage_id=1)
        self.assertEqual(stage.cache_length,2)
        for old,new in zip(before,stage.caches):
            for x,y in zip(old,new):torch.testing.assert_close(x,y)

    def test_cuda_rejects_non_nvidia_runtime(self):
        from unittest.mock import patch
        state,config=synthetic_state()
        with patch('torch.cuda.is_available',return_value=True), patch('torch.version.cuda',None):
            with self.assertRaises(ValueError):
                TiledQwen2Stage(state,config,stage_id=0,split_layer=1,device='cuda')

    def test_pinned_loader_survives_source_replacement_after_inspection(self):
        import hashlib
        import json
        from pathlib import Path
        import tempfile
        from unittest.mock import patch
        from safetensors.torch import save_file
        from compute.qwen2_safetensors import inspect_directory
        state, config = synthetic_state()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_bytes = json.dumps(config).encode()
            (root / 'config.json').write_bytes(config_bytes)
            save_file(state, str(root / 'model.safetensors'))
            pin = hashlib.sha256((root / 'model.safetensors').read_bytes()).hexdigest()
            replacement = dict(state)
            replacement['model.embed_tokens.weight'] = torch.full_like(
                state['model.embed_tokens.weight'], 7.)
            save_file(replacement, str(root / 'replacement.safetensors'))
            inspected_paths = []
            def replace_original_after_inspection(path, *args, **kwargs):
                result = inspect_directory(path, *args, **kwargs)
                inspected_paths.append(Path(path))
                self.assertEqual(Path(path).stat().st_mode & 0o777, 0o700)
                (root / 'replacement.safetensors').replace(root / 'model.safetensors')
                return result
            with patch('compute.qwen2_tiled_stage.inspect_directory',
                       side_effect=replace_original_after_inspection):
                stage = TiledQwen2Stage.from_safetensors(
                    root, expected_sha256=pin,
                    expected_config_sha256=hashlib.sha256(config_bytes).hexdigest(),
                    stage_id=0, split_layer=1, device='cpu')
            torch.testing.assert_close(stage.embed, state['model.embed_tokens.weight'])
            self.assertNotEqual(inspected_paths[0], root)
            self.assertFalse(inspected_paths[0].exists(), 'private snapshot must be removed')
