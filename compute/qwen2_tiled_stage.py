"""Two contiguous Qwen2 stages with ordered, transactional tile-local KV state.

Inference-only FP32 adapter: stage 0 embeds; stage 1 applies final norm/head.
No networking, scheduler, checkpoint pickle loading or device fallback.
"""
import hashlib
import json
import os
import stat
import tempfile
from pathlib import Path

import torch
import torch.nn.functional as F

from compute.qwen2_core import forward_layers, rms_norm
from compute.qwen2_shards import manifest_from_config
from compute.qwen2_safetensors import (
    MAX_CONFIG_BYTES, MAX_WEIGHT_FILE_BYTES, _expected_shapes,
    _optional_bias_shapes, _regular,
    inspect_directory,
)


class TiledQwen2Stage:
    def __init__(self, state, config, *, stage_id, split_layer, device):
        self.meta = manifest_from_config(config, 1, source='tiled-stage')
        m = self.meta
        if (type(stage_id) is not int or stage_id not in (0, 1)
                or type(split_layer) is not int or not 0 < split_layer < m['layers']):
            raise ValueError('invalid two-stage identity or contiguous split')
        self.device = torch.device(device)
        if self.device.type not in ('cpu', 'cuda'):
            raise ValueError('only explicit CPU or CUDA execution is supported')
        if self.device.type == 'cuda':
            if (not torch.version.cuda or not torch.cuda.is_available()
                    or (self.device.index is not None
                        and self.device.index >= torch.cuda.device_count())):
                raise ValueError('requested CUDA device unavailable; no CPU fallback')
            if self.device.index is None:
                self.device = torch.device('cuda', torch.cuda.current_device())
        self.stage_id = stage_id
        self.layer_range = ((0, split_layer) if stage_id == 0
                            else (split_layer, m['layers']))
        shapes = {**_expected_shapes(m), **_optional_bias_shapes(m)}

        def get(key):
            try:
                value = state[key]
            except KeyError as exc:
                raise ValueError("missing Qwen2 weight: " + key) from exc
            if (not isinstance(value, torch.Tensor) or tuple(value.shape) != shapes[key]
                    or value.dtype not in (torch.float32, torch.float16, torch.bfloat16)
                    or not torch.isfinite(value).all()):
                raise ValueError('invalid Qwen2 weight: ' + key)
            return value.detach().to(device=self.device, dtype=torch.float32).clone()

        self.layers = []
        for i in range(*self.layer_range):
            p = f'model.layers.{i}.'
            layer = {
                'input_norm': get(p + 'input_layernorm.weight'),
                'post_norm': get(p + 'post_attention_layernorm.weight'),
            }
            for name in ('q', 'k', 'v'):
                key = p + f'self_attn.{name}_proj.bias'
                heads = m['heads'] if name == 'q' else m['kv_heads']
                width = heads * m['head_dim']
                layer[name + '_w'] = get(p + f'self_attn.{name}_proj.weight')
                layer[name + '_b'] = (get(key) if key in state else
                                     torch.zeros(width, device=self.device))
            layer['o_w'] = get(p + 'self_attn.o_proj.weight')
            for name in ('gate', 'up', 'down'):
                layer[name + '_w'] = get(p + f'mlp.{name}_proj.weight')
            self.layers.append(layer)
        self.embed = get('model.embed_tokens.weight') if stage_id == 0 else None
        self.norm = get('model.norm.weight') if stage_id == 1 else None
        head_key = ('model.embed_tokens.weight' if m['tied_embeddings']
                    else 'lm_head.weight')
        self.head = get(head_key) if stage_id == 1 else None
        self.reset()

    @classmethod
    def from_safetensors(cls, root, *, expected_sha256, expected_config_sha256,
                         stage_id, split_layer, device):
        """Load only owned tensors from trusted config/weight SHA256 pins.

        Config and weights are loaded from private verified byte snapshots.
        Header inspection remains CPU-only; no peer layers transfer to GPU.
        Cold load requires temporary disk space for one complete weight file.
        """
        for pin in (expected_sha256, expected_config_sha256):
            if (not isinstance(pin, str) or len(pin) != 64
                    or any(ch not in '0123456789abcdef' for ch in pin)):
                raise ValueError('trusted lowercase config and weight SHA256 required')
        root = Path(root).resolve(strict=True)
        with _regular(root / 'config.json', MAX_CONFIG_BYTES).open('rb') as stream:
            config_bytes = stream.read(MAX_CONFIG_BYTES + 1)
        if len(config_bytes) > MAX_CONFIG_BYTES:
            raise ValueError('config grew beyond permitted size')
        if hashlib.sha256(config_bytes).hexdigest() != expected_config_sha256:
            raise ValueError('source config SHA256 mismatch')
        config = json.loads(config_bytes)
        if not isinstance(config, dict):
            raise ValueError('model config must be an object')
        if ((root / 'model.safetensors.index.json').exists()
                or (root / 'model.safetensors.index.json').is_symlink()):
            raise ValueError('pinned stage loader requires a single weight file')
        source = _regular(root / 'model.safetensors', MAX_WEIGHT_FILE_BYTES)
        # Hash the actual bytes written to a private snapshot, not a source
        # pathname that the lazy reader would later reopen. A replacement or
        # in-place write in the source can only affect bytes subject to this pin.
        with tempfile.TemporaryDirectory(prefix='oc-qwen2-pinned-') as tmp:
            snapshot = Path(tmp)
            (snapshot / 'config.json').write_bytes(config_bytes)
            try:
                fd = os.open(source, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
            except OSError as exc:
                raise ValueError('cannot open regular pinned weight file') from exc
            digest = hashlib.sha256()
            total = 0
            with os.fdopen(fd, 'rb') as original:
                info = os.fstat(original.fileno())
                if (not stat.S_ISREG(info.st_mode)
                        or not 0 < info.st_size <= MAX_WEIGHT_FILE_BYTES):
                    raise ValueError('unsafe pinned weight file')
                with (snapshot / 'model.safetensors').open('xb') as copied:
                    while block := original.read(8 * 1024 * 1024):
                        total += len(block)
                        if total > MAX_WEIGHT_FILE_BYTES:
                            raise ValueError('weight file grew beyond permitted size')
                        copied.write(block)
                        digest.update(block)
            if digest.hexdigest() != expected_sha256:
                raise ValueError('source Safetensors SHA256 mismatch')
            # TemporaryDirectory is mode 0700; private files become read-only
            # before inspector and stage loading use this verified byte image.
            for name in ('config.json', 'model.safetensors'):
                (snapshot / name).chmod(0o400)
            from safetensors import SafetensorError
            try:
                inspected = inspect_directory(
                    snapshot, 1, expected_sha256=expected_sha256)
            except SafetensorError as exc:
                raise ValueError('invalid local Safetensors') from exc
            expected_meta = manifest_from_config(config, 1, source=snapshot.name)
            if inspected['manifest'] != expected_meta:
                raise ValueError('config changed during inspection')
            # Construction eagerly retains only owned tensor clones. No file
            # handles or lazy readers escape the private snapshot lifetime.
            return cls(inspected['weights'], config, stage_id=stage_id,
                       split_layer=split_layer, device=device)

    def reset(self):
        self.caches = [None] * len(self.layers)
        self.cache_length = 0
        self.batch_size = None

    @torch.inference_mode()
    def prefill(self, value, *, token_start, stage_id):
        m = self.meta
        if (type(stage_id) is not int or stage_id != self.stage_id
                or type(token_start) is not int or token_start != self.cache_length):
            raise ValueError('stage identity or token_start/order mismatch')
        if not isinstance(value, torch.Tensor) or value.device != self.device:
            raise ValueError('input must be a tensor on the explicit stage device')
        ndim = 2 if self.stage_id == 0 else 3
        if value.ndim != ndim or value.shape[0] < 1 or value.shape[1] < 1:
            raise ValueError('invalid tile shape')
        b, n = value.shape[:2]
        if ((self.batch_size is not None and b != self.batch_size)
                or token_start + n > m['positions']):
            raise ValueError('batch/context mismatch')
        if self.stage_id == 0:
            if (value.dtype != torch.long or bool((value < 0).any())
                    or bool((value >= m['vocab']).any())):
                raise ValueError('invalid input token dtype or vocabulary range')
            x = F.embedding(value, self.embed)
        else:
            if (value.dtype != torch.float32 or value.shape[2] != m['hidden']
                    or not bool(torch.isfinite(value).all())):
                raise ValueError('invalid hidden shape/dtype/values')
            x = value
        # Shared math uses the committed KV length as absolute RoPE position.
        # It constructs new KV tensors and does not mutate the old cache.
        x, caches = forward_layers(x, self.layers, self.caches, m)
        if self.stage_id == 1:
            x = F.linear(rms_norm(x, self.norm, m['rms_eps']), self.head)
        if not bool(torch.isfinite(x).all()):
            raise ValueError('non-finite tile result')
        self.caches = caches
        self.cache_length += n
        self.batch_size = b
        return x

    def decode(self, value, *, token_start, stage_id):
        if (not isinstance(value, torch.Tensor) or value.ndim < 2
                or value.shape[1] != 1):
            raise ValueError('decode requires exactly one token')
        return self.prefill(value, token_start=token_start, stage_id=stage_id)
