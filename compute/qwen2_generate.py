"""Distributed CPU Qwen2 greedy decoding from rank-local tensor shards.

Uses one Gloo process per shard. All ranks own a fraction of Q/K/V and MLP,
rank 0 owns the full vocabulary head. For trusted localhost research only.
"""
import argparse
import json
import os
import time
from pathlib import Path

import torch
import torch.distributed as dist
import torch.nn.functional as F

from compute.qwen2_core import forward_layers, rms_norm
from compute.qwen2_shards import FORMAT
from compute.qwen2_chunked import FORMAT as CHUNK_FORMAT, ChunkedIO, load_chunked_rank, rank_files
from compute.rendezvous import init_gloo
from compute.qwen2_trace import CollectiveProfiler, profile_rank_stats, interpret_rank_stats


def read_manifest(folder):
    m = json.loads((Path(folder) / "manifest.json").read_text(encoding="utf-8"))
    if m.get("format") not in (FORMAT, CHUNK_FORMAT):
        raise ValueError("unsupported Qwen2 shard manifest")
    from cluster_v031.checkpoints import validate_manifest
    validate_manifest(m)
    return m


def load_rank(folder, rank, m, *, layer_cache_mib=0, io_cache_mib=0):
    if m["format"] == CHUNK_FORMAT:
        return load_chunked_rank(folder, rank, m,
                                 layer_cache_mib=layer_cache_mib,
                                 io_cache_mib=io_cache_mib)
    folder = Path(folder)
    hidden = m["hidden"]
    q_width = m["head_dim"] * m["heads"] // m["workers"]
    kv_width = m["head_dim"] * m["kv_heads"] // m["workers"]
    inner = m["inner"] // m["workers"]
    expected = {
        "input_norm": (hidden,), "post_norm": (hidden,),
        "q_w": (q_width, hidden), "q_b": (q_width,),
        "k_w": (kv_width, hidden), "k_b": (kv_width,),
        "v_w": (kv_width, hidden), "v_b": (kv_width,),
        "o_w": (hidden, q_width),
        "gate_w": (inner, hidden), "up_w": (inner, hidden),
        "down_w": (hidden, inner),
    }
    shards = torch.load(folder / f"rank_{rank}.pt", map_location="cpu",
                        weights_only=True)
    if not isinstance(shards, list) or len(shards) != m["layers"]:
        raise ValueError("invalid Qwen2 layer count")
    for index, layer in enumerate(shards):
        if not isinstance(layer, dict) or set(layer) != set(expected):
            raise ValueError(f"layer {index}: invalid parameter keys")
        for key, shape in expected.items():
            tensor = layer[key]
            if (not isinstance(tensor, torch.Tensor)
                    or tuple(tensor.shape) != shape
                    or tensor.dtype != torch.float32
                    or not torch.isfinite(tensor).all().item()):
                raise ValueError(f"layer {index}: invalid tensor {key}")
    io = None
    if rank == 0:
        io = torch.load(folder / "rank0_io.pt", map_location="cpu",
                        weights_only=True)
        expected_io = {"embed": (m["vocab"], hidden),
                       "lm_head": (m["vocab"], hidden),
                       "norm": (hidden,)}
        if not isinstance(io, dict) or set(io) != set(expected_io):
            raise ValueError("invalid Qwen2 vocabulary/head keys")
        for key, shape in expected_io.items():
            tensor = io[key]
            if (not isinstance(tensor, torch.Tensor)
                    or tuple(tensor.shape) != shape
                    or tensor.dtype != torch.float32
                    or not torch.isfinite(tensor).all().item()):
                raise ValueError(f"invalid Qwen2 IO tensor {key}")
    return shards, io


@torch.inference_mode()
def generate(folder, ids, max_new_tokens, rank, world, return_logits=False,
             preflight_error=None, layer_cache_mib=0, io_cache_mib=0,
             profile=False):
    preflight_started_ns = time.perf_counter_ns()
    error = preflight_error
    m, layers, io = None, None, None
    if error is None:
        try:
            m = read_manifest(folder)
            if (m["workers"] != world or not 0 <= rank < world
                    or not 1 <= max_new_tokens <= 256):
                raise ValueError("distributed Qwen2 world or token count mismatch")
            if rank == 0:
                if (not ids or any(type(i) is not int or i < 0 or i >= m["vocab"]
                                   for i in ids)):
                    raise ValueError("invalid Qwen2 prompt IDs")
                if len(ids) + max_new_tokens - 1 > m["positions"]:
                    raise ValueError("Qwen2 context length exceeded")
            layers, io = load_rank(
                folder, rank, m, layer_cache_mib=layer_cache_mib,
                io_cache_mib=io_cache_mib)
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
    ready = torch.tensor([0 if error else 1], dtype=torch.int32)
    dist.all_reduce(ready, op=dist.ReduceOp.MIN)
    if int(ready.item()) != 1:
        raise ValueError(f"Qwen2 rank preflight failed: {error or 'peer rank failed validation'}")
    preflight_us = (time.perf_counter_ns() - preflight_started_ns) // 1000
    trace = None

    def broadcast(tensor):
        if trace is None:
            return dist.broadcast(tensor, src=0)
        return trace.broadcast(tensor, src=0)

    def reduce_sum(tensor):
        if trace is None:
            return dist.all_reduce(tensor, op=dist.ReduceOp.SUM)
        return trace.all_reduce(tensor, op=dist.ReduceOp.SUM)

    length = torch.tensor([len(ids) if rank == 0 else 0], dtype=torch.int64)
    broadcast(length)
    prompt_length = int(length.item())
    if not 1 <= prompt_length <= m["positions"] - max_new_tokens + 1:
        raise ValueError("invalid synchronized Qwen2 context")
    # Exclude the one-off prompt-length coordination from decode profiling.
    trace = CollectiveProfiler() if profile else None
    pending = list(ids) if rank == 0 else []
    caches = [None] * m["layers"]
    generated = []
    all_logits = []
    start = time.perf_counter()
    prefill_us = 0
    for step in range(max_new_tokens):
        step_started_ns = time.perf_counter_ns()
        sequence_length = prompt_length if step == 0 else 1
        if rank == 0:
            token = torch.tensor(pending, dtype=torch.long)
            x = (io.embed(token) if isinstance(io, ChunkedIO)
                 else F.embedding(token, io["embed"])).unsqueeze(0).contiguous()
        else:
            x = torch.empty((1, sequence_length, m["hidden"]), dtype=torch.float32)
        broadcast(x)
        x, caches = forward_layers(x, layers, caches, m, reduce_sum=reduce_sum)
        if rank == 0:
            last = rms_norm(x[:, -1, :],
                            io.norm if isinstance(io, ChunkedIO) else io["norm"],
                            m["rms_eps"])
            logits = (io.logits(last)[0] if isinstance(io, ChunkedIO)
                      else F.linear(last, io["lm_head"])[0])
            next_id = int(torch.argmax(logits).item())
            generated.append(next_id)
            if return_logits:
                all_logits.append(logits.tolist())
            pending = [next_id]
        if step == 0:
            prefill_us = (time.perf_counter_ns() - step_started_ns) // 1000
    elapsed = time.perf_counter() - start
    def tensor_bytes(value):
        if isinstance(value, torch.Tensor):
            return value.numel() * value.element_size()
        if isinstance(value, dict):
            return sum(tensor_bytes(v) for v in value.values())
        if isinstance(value, (list, tuple)):
            return sum(tensor_bytes(v) for v in value)
        return 0
    # The managed Worker expects a per-rank weight metric just as the GPT-2
    # runner reports. All ranks must join this collective before rank 0 exits.
    weight_bytes = (sum((Path(folder) / name).stat().st_size
                    for name in rank_files(m, rank) if name != "manifest.json")
                    if m["format"] == CHUNK_FORMAT
                    else tensor_bytes(layers) + tensor_bytes(io))
    size = torch.tensor([weight_bytes], dtype=torch.int64)
    gathered = [torch.zeros_like(size) for _ in range(world)]
    dist.all_gather(gathered, size)
    profiled_ranks = None
    if profile:
        try:
            import psutil
            rss_sample = psutil.Process().memory_info().rss
        except (ImportError, OSError):
            rss_sample = 0
        record = profile_rank_stats(
            preflight_us=int(preflight_us),
            decode_us=int(elapsed * 1_000_000),
            prefill_us=int(prefill_us),
            tokens=max_new_tokens,
            profiler=trace,
            rss_sample_bytes=int(rss_sample),
        )
        local = torch.tensor(record, dtype=torch.int64)
        observed = [torch.zeros_like(local) for _ in range(world)]
        # Profiling does not include this final reporting collective.
        dist.all_gather(observed, local)
        if rank == 0:
            profiled_ranks = [interpret_rank_stats(item.tolist())
                              for item in observed]
    if rank == 0:
        return {
            "engine": "opencompute-qwen2-tp-v0.4.1",
            "source": m["source"], "workers": world,
            "prompt_ids": ids, "generated_ids": generated,
            "tokens_per_second_rank0": round(max_new_tokens / max(1e-9, elapsed), 3),
            "elapsed_seconds_rank0": round(elapsed, 5),
            "weight_bytes_per_rank": [int(x.item()) for x in gathered],
            **({"profile": {
                "schema": "opencompute-qwen2-tp-profile-v1",
                "ranks": profiled_ranks,
                "scope": "trusted same-host Gloo; tensor bytes not actual network traffic",
                "decode_includes": "prefill and greedy steps; excludes model preflight",
            }} if profile else {}),
            **({"step_logits": all_logits} if return_logits else {}),
        }
    return None


def main():
    parser = argparse.ArgumentParser(description="Qwen2 CPU Gloo greedy decoder")
    parser.add_argument("--shards", required=True)
    parser.add_argument("--prompt-ids", default="1,2,3")
    parser.add_argument("--max-new-tokens", type=int, default=3)
    parser.add_argument("--report-logits", action="store_true")
    parser.add_argument("--profile", action="store_true",
                        help="collect per-rank timing and logical collective payload")
    parser.add_argument("--layer-cache-mib", type=int, default=0)
    parser.add_argument("--io-cache-mib", type=int, default=0)
    args = parser.parse_args()
    if not 0 <= args.layer_cache_mib <= 512 or not 0 <= args.io_cache_mib <= 512:
        parser.error("cache budgets must be in 0..512 MiB")
    torch.set_num_threads(1)
    rank, world = int(os.environ["RANK"]), int(os.environ["WORLD_SIZE"])
    store = init_gloo(timeout_seconds=120)
    try:
        ids, input_error = [], None
        if rank == 0:
            try:
                ids = [int(item.strip()) for item in args.prompt_ids.split(",")]
            except Exception as exc:
                input_error = f"invalid prompt format: {type(exc).__name__}: {exc}"
        outcome = generate(Path(args.shards), ids, args.max_new_tokens,
                           rank, world, args.report_logits, input_error,
                           layer_cache_mib=args.layer_cache_mib,
                           io_cache_mib=args.io_cache_mib,
                           profile=args.profile)
        if rank == 0:
            print(json.dumps(outcome, ensure_ascii=False), flush=True)
    finally:
        dist.destroy_process_group()
        del store


if __name__ == "__main__":
    main()
