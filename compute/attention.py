"""CPU tensor-parallel causal attention with rank-local KV cache.

Each rank owns disjoint attention heads and an output projection row shard.
The collective sums projected outputs. Synthetic weights are deterministic.
"""
import argparse
import json
import os
import time
from datetime import timedelta
from pathlib import Path

import torch
import torch.distributed as dist

from compute.mlp import matrix


def prepare_attention(directory, hidden=32, heads=4, workers=2):
    if min(hidden, heads, workers) < 1 or hidden % heads or heads % workers:
        raise ValueError("hidden divisible by heads; heads divisible by workers")
    path = Path(directory)
    path.mkdir(parents=True, exist_ok=True)
    local = hidden // workers
    for rank in range(workers):
        begin = rank * local
        weights = {
            "q": matrix(hidden, local, col_offset=begin),
            "k": matrix(hidden, local, col_offset=begin + 10000),
            "v": matrix(hidden, local, col_offset=begin + 20000),
            "o": matrix(local, hidden, row_offset=begin + 30000),
        }
        torch.save(weights, path / f"rank_{rank}.pt")
    meta = {"hidden": hidden, "heads": heads, "workers": workers}
    (path / "manifest.json").write_text(json.dumps(meta))
    return meta


def local_attention(x, weights, heads, past=None):
    """x [batch, tokens, hidden]; cache tensors [batch, heads, tokens, head_dim]."""
    batch, tokens, hidden = x.shape
    local_width = weights["q"].shape[1]
    head_dim = local_width // heads
    def project(key):
        return (x @ weights[key]).reshape(batch, tokens, heads, head_dim).transpose(1, 2)
    q, k, v = project("q"), project("k"), project("v")
    past_len = 0
    if past is not None:
        pk, pv = past
        past_len = pk.shape[2]
        k, v = torch.cat((pk, k), dim=2), torch.cat((pv, v), dim=2)
    scores = q @ k.transpose(-2, -1) / (head_dim ** 0.5)
    query_positions = torch.arange(past_len, past_len + tokens, device=x.device)
    key_positions = torch.arange(past_len + tokens, device=x.device)
    mask = key_positions[None, :] > query_positions[:, None]
    scores = scores.masked_fill(mask[None, None, :, :], float("-inf"))
    attention = torch.softmax(scores, dim=-1) @ v
    merged = attention.transpose(1, 2).contiguous().reshape(batch, tokens, local_width)
    return merged @ weights["o"], (k, v)


def load_shard(directory, rank, hidden, heads, workers):
    local = hidden // workers
    weights = torch.load(Path(directory) / f"rank_{rank}.pt",
                         map_location="cpu", weights_only=True)
    for name in ("q", "k", "v"):
        if weights[name].shape != (hidden, local) or weights[name].dtype != torch.float32:
            raise ValueError("invalid projection shard")
    if weights["o"].shape != (local, hidden) or weights["o"].dtype != torch.float32:
        raise ValueError("invalid output projection shard")
    return weights


def reference(x, hidden, heads, workers, past=None):
    """Streaming single-process reference, reconstructing no full weight matrix."""
    result = torch.zeros_like(x)
    caches = []
    for rank in range(workers):
        begin = rank * (hidden // workers)
        local = hidden // workers
        w = {"q": matrix(hidden, local, col_offset=begin),
             "k": matrix(hidden, local, col_offset=begin + 10000),
             "v": matrix(hidden, local, col_offset=begin + 20000),
             "o": matrix(local, hidden, row_offset=begin + 30000)}
        output, cache = local_attention(x, w, heads // workers,
                                        None if past is None else past[rank])
        result += output
        caches.append(cache)
    return result, caches


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--shards", required=True)
    p.add_argument("--batch", type=int, default=2)
    p.add_argument("--prompt-tokens", type=int, default=4)
    p.add_argument("--decode-steps", type=int, default=3)
    args = p.parse_args()
    if min(args.batch, args.prompt_tokens, args.decode_steps) < 1:
        p.error("all dimensions must be positive")
    rank, world = int(os.environ["RANK"]), int(os.environ["WORLD_SIZE"])
    torch.set_num_threads(1)
    dist.init_process_group("gloo", timeout=timedelta(seconds=120))
    try:
        root = Path(args.shards)
        meta = json.loads((root / "manifest.json").read_text())
        hidden, heads = meta["hidden"], meta["heads"]
        if meta["workers"] != world or heads % world:
            raise ValueError("worker count mismatch")
        weights = load_shard(root, rank, hidden, heads, world)
        torch.manual_seed(2202)
        sequence = torch.randn(args.batch, args.prompt_tokens + args.decode_steps, hidden)
        cache = None
        reference_cache = None
        max_error = 0.0
        total_compute = total_collective = 0.0
        for step in range(args.decode_steps + 1):
            x = sequence[:, :args.prompt_tokens] if step == 0 else sequence[:, args.prompt_tokens + step - 1:args.prompt_tokens + step]
            dist.barrier()
            start = time.perf_counter()
            partial, cache = local_attention(x, weights, heads // world, cache)
            middle = time.perf_counter()
            dist.all_reduce(partial, op=dist.ReduceOp.SUM)
            end = time.perf_counter()
            total_compute += middle - start
            total_collective += end - middle
            if rank == 0:
                expected, reference_cache = reference(x, hidden, heads, world, reference_cache)
                max_error = max(max_error, (partial - expected).abs().max().item())
        stats = torch.tensor([total_compute, total_collective], dtype=torch.float64)
        dist.all_reduce(stats, op=dist.ReduceOp.MAX)
        if rank == 0:
            local_bytes = sum(t.numel() * t.element_size() for t in weights.values())
            print(json.dumps({
                "model": "causal_attention_tp", "workers": world,
                "hidden": hidden, "heads": heads, "batch": args.batch,
                "prompt_tokens": args.prompt_tokens, "decode_steps": args.decode_steps,
                "correct": max_error < 1e-4, "max_abs_error": max_error,
                "local_weight_bytes": local_bytes,
                "full_weight_bytes": local_bytes * world,
                "local_kv_cache_bytes": sum(t.numel() * t.element_size() for t in cache),
                "compute_ms_upper_bound": round(stats[0].item() * 1000, 4),
                "collective_ms_upper_bound": round(stats[1].item() * 1000, 4)
            }), flush=True)
            if max_error >= 1e-4:
                raise RuntimeError("attention mismatch")
    finally:
        dist.destroy_process_group()


if __name__ == "__main__":
    main()
