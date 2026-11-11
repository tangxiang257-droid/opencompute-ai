"""Shared float32 Qwen2 math for CPU and single-device CUDA inference.

Features: RoPE (non-interleaved rotate_half), GQA, RMSNorm, SwiGLU, causal
masking and per-rank KV caches. No fused/quantized kernels or sliding window.
"""
import math
import torch
import torch.nn.functional as F


def rms_norm(x, weight, eps):
    return x * torch.rsqrt(torch.mean(x * x, dim=-1, keepdim=True) + eps) * weight


def rope(query, key, position_ids, theta):
    dim = query.shape[-1]
    if dim % 2:
        raise ValueError("RoPE head dimension must be even")
    inv = 1.0 / (theta ** (torch.arange(0, dim, 2, dtype=torch.float32, device=query.device) / dim))
    freqs = position_ids.to(torch.float32).unsqueeze(-1) * inv
    embedding = torch.cat((freqs, freqs), dim=-1)
    cos = embedding.cos()[None, None, :, :]
    sin = embedding.sin()[None, None, :, :]
    def half_rotate(value):
        return torch.cat((-value[..., dim // 2:], value[..., :dim // 2]), dim=-1)
    return ((query * cos) + half_rotate(query) * sin,
            (key * cos) + half_rotate(key) * sin)


def attention(x, layer, cache, meta):
    b, n, hidden = x.shape
    local_heads = meta["heads"] // meta["workers"]
    local_kv = meta["kv_heads"] // meta["workers"]
    dim = meta["head_dim"]
    def projection(name, groups):
        value = F.linear(x, layer[name + "_w"], layer[name + "_b"])
        return value.view(b, n, groups, dim).transpose(1, 2)
    q = projection("q", local_heads)
    k = projection("k", local_kv)
    v = projection("v", local_kv)
    past = cache[0].shape[2] if cache is not None else 0
    positions = torch.arange(past, past + n, device=x.device)
    q, k = rope(q, k, positions, meta["rope_theta"])
    if cache is not None:
        k = torch.cat((cache[0], k), dim=2)
        v = torch.cat((cache[1], v), dim=2)
    rep = local_heads // local_kv
    expanded_k = k.repeat_interleave(rep, dim=1)
    expanded_v = v.repeat_interleave(rep, dim=1)
    score = (q @ expanded_k.transpose(2, 3)) / math.sqrt(dim)
    key_positions = torch.arange(k.shape[2], device=x.device)
    mask = key_positions[None, :] > positions[:, None]
    score = score.masked_fill(mask[None, None, :, :], float("-inf"))
    probs = torch.softmax(score, dim=-1, dtype=torch.float32).to(q.dtype)
    out = probs @ expanded_v
    out = out.transpose(1, 2).contiguous().view(b, n, local_heads * dim)
    return F.linear(out, layer["o_w"]), (k, v)


def forward_layers(x, layers, caches, meta, reduce_sum=None):
    next_cache = []
    for layer, cache in zip(layers, caches):
        norm = rms_norm(x, layer["input_norm"], meta["rms_eps"])
        partial, updated = attention(norm, layer, cache, meta)
        if reduce_sum is not None:
            reduce_sum(partial)
        x = x + partial
        norm = rms_norm(x, layer["post_norm"], meta["rms_eps"])
        mlp = F.silu(F.linear(norm, layer["gate_w"])) * F.linear(norm, layer["up_w"])
        partial = F.linear(mlp, layer["down_w"])
        if reduce_sum is not None:
            reduce_sum(partial)
        x = x + partial
        next_cache.append(updated)
    return x, next_cache
