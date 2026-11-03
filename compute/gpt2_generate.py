"""Greedy autoregressive GPT-2 inference with rank-local model and KV shards.

The full model is never loaded in worker processes. Rank zero alone holds
vocabulary/position embeddings and tied LM-head weights; hidden activations
are broadcast to peers. Each Transformer block uses two All-Reduce operations.
"""
import argparse
import json
import os
import resource
import time
from datetime import timedelta
from pathlib import Path

import torch
import torch.distributed as dist
import torch.nn.functional as F


def read_manifest(path: Path) -> dict:
    manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("format") != "opencompute-gpt2-tp-v1":
        raise ValueError("unsupported weight shard format")
    return manifest


def load_rank(path: Path, rank: int, config: dict):
    layers = torch.load(path / f"rank_{rank}.pt", map_location="cpu", weights_only=True)
    hidden = config["hidden"]
    local = hidden // config["workers"]
    inner_local = config["inner"] // config["workers"]
    if not isinstance(layers, list) or len(layers) != config["layers"]:
        raise ValueError("incorrect layer count")
    expected = {
        "ln1_w": (hidden,), "ln1_b": (hidden,), "ln2_w": (hidden,), "ln2_b": (hidden,),
        "attn_out_b": (hidden,), "mlp_out_b": (hidden,),
        "q_w": (hidden, local), "q_b": (local,),
        "k_w": (hidden, local), "k_b": (local,),
        "v_w": (hidden, local), "v_b": (local,),
        "attn_out_w": (local, hidden),
        "mlp_fc_w": (hidden, inner_local), "mlp_fc_b": (inner_local,),
        "mlp_out_w": (inner_local, hidden),
    }
    for i, layer in enumerate(layers):
        if set(layer) != set(expected):
            raise ValueError(f"layer {i}: incorrect parameter names")
        for name, shape in expected.items():
            tensor = layer[name]
            if not isinstance(tensor, torch.Tensor) or tuple(tensor.shape) != shape or tensor.dtype != torch.float32:
                raise ValueError(f"layer {i}: invalid {name}")
    io = None
    if rank == 0:
        io = torch.load(path / "rank0_io.pt", map_location="cpu", weights_only=True)
        shapes = {"wte": (config["vocab"], hidden),
                  "wpe": (config["positions"], hidden),
                  "ln_f_w": (hidden,), "ln_f_b": (hidden,)}
        for key, shape in shapes.items():
            if key not in io or tuple(io[key].shape) != shape or io[key].dtype != torch.float32:
                raise ValueError(f"invalid rank-zero IO: {key}")
    return layers, io


def local_attention(x, layer: dict, cache, local_heads: int):
    batch, tokens, hidden = x.shape
    width = layer["q_w"].shape[1]
    dimension = width // local_heads
    def projected(name):
        v = x @ layer[name + "_w"] + layer[name + "_b"]
        return v.reshape(batch, tokens, local_heads, dimension).transpose(1, 2)
    q, k, v = projected("q"), projected("k"), projected("v")
    past_tokens = 0
    if cache is not None:
        past_k, past_v = cache
        past_tokens = past_k.shape[2]
        k = torch.cat((past_k, k), dim=2)
        v = torch.cat((past_v, v), dim=2)
    score = (q @ k.transpose(-2, -1)) * (dimension ** -0.5)
    query_idx = torch.arange(past_tokens, past_tokens + tokens)
    key_idx = torch.arange(past_tokens + tokens)
    score.masked_fill_(key_idx[None, :] > query_idx[:, None], float("-inf"))
    attn = F.softmax(score, dim=-1) @ v
    merged = attn.transpose(1, 2).contiguous().view(batch, tokens, width)
    return merged @ layer["attn_out_w"], (k, v)


def forward_step(x, layers, caches, meta):
    """Run each shard on each rank; collective sums before adding shared biases."""
    new_caches = []
    local_heads = meta["heads"] // meta["workers"]
    for layer, cache in zip(layers, caches):
        normalized = F.layer_norm(x, (meta["hidden"],), layer["ln1_w"],
                                  layer["ln1_b"], meta["layer_norm_epsilon"])
        partial, new_cache = local_attention(normalized, layer, cache, local_heads)
        dist.all_reduce(partial, op=dist.ReduceOp.SUM)
        x = x + partial + layer["attn_out_b"]
        normalized = F.layer_norm(x, (meta["hidden"],), layer["ln2_w"],
                                  layer["ln2_b"], meta["layer_norm_epsilon"])
        partial = F.gelu(normalized @ layer["mlp_fc_w"] + layer["mlp_fc_b"],
                         approximate="tanh") @ layer["mlp_out_w"]
        dist.all_reduce(partial, op=dist.ReduceOp.SUM)
        x = x + partial + layer["mlp_out_b"]
        new_caches.append(new_cache)
    return x, new_caches


def parameter_bytes(value):
    if isinstance(value, torch.Tensor):
        return value.numel() * value.element_size()
    if isinstance(value, dict):
        return sum(parameter_bytes(v) for v in value.values())
    if isinstance(value, list):
        return sum(parameter_bytes(v) for v in value)
    return 0


@torch.inference_mode()
def generate(path: Path, prompt_ids: list[int], max_new_tokens: int, rank: int, world: int,
             return_logits=False):
    meta = read_manifest(path)
    if world != meta["workers"] or meta["heads"] % world:
        raise ValueError("number of distributed workers must match exported shards")
    if not 1 <= max_new_tokens <= 256:
        raise ValueError("max_new_tokens must be 1..256")
    if rank == 0:
        if not prompt_ids or any(not 0 <= item < meta["vocab"] for item in prompt_ids):
            raise ValueError("prompt must contain valid token IDs")
        if len(prompt_ids) + max_new_tokens - 1 > meta["positions"]:
            raise ValueError("generation exceeds model position embeddings")
    layers, io = load_rank(path, rank, meta)
    count = torch.tensor([len(prompt_ids) if rank == 0 else 0], dtype=torch.int64)
    dist.broadcast(count, src=0)
    prefill_len = int(count.item())
    if prefill_len < 1 or prefill_len + max_new_tokens - 1 > meta["positions"]:
        raise ValueError("prompt length exceeds model context")
    caches = [None] * meta["layers"]
    output_ids = []
    logits_samples = []
    pending = prompt_ids if rank == 0 else []
    total_start = time.perf_counter()
    for iteration in range(max_new_tokens):
        position = 0 if iteration == 0 else prefill_len + iteration - 1
        length = prefill_len if iteration == 0 else 1
        if rank == 0:
            idx = torch.tensor(pending, dtype=torch.long)
            offsets = torch.arange(position, position + length)
            x = (io["wte"][idx] + io["wpe"][offsets]).unsqueeze(0).contiguous()
        else:
            x = torch.empty(1, length, meta["hidden"], dtype=torch.float32)
        dist.broadcast(x, src=0)
        x, caches = forward_step(x, layers, caches, meta)
        if rank == 0:
            x = F.layer_norm(x, (meta["hidden"],), io["ln_f_w"], io["ln_f_b"],
                             meta["layer_norm_epsilon"])
            logits = x[0, -1] @ io["wte"].T
            next_token = int(torch.argmax(logits).item())
            if return_logits:
                logits_samples.append(logits.tolist())
        else:
            next_token = 0
        synced = torch.tensor([next_token], dtype=torch.int64)
        dist.broadcast(synced, src=0)
        next_token = int(synced.item())
        output_ids.append(next_token)
        pending = [next_token] if rank == 0 else []
    elapsed = time.perf_counter() - total_start
    kv_bytes = sum(parameter_bytes(k) + parameter_bytes(v) for k, v in caches)
    values = torch.tensor([parameter_bytes(layers) + parameter_bytes(io), kv_bytes,
                          resource.getrusage(resource.RUSAGE_SELF).ru_maxrss], dtype=torch.int64)
    gathered = [torch.zeros_like(values) for _ in range(world)]
    dist.all_gather(gathered, values)
    if rank == 0:
        return {
            "engine": "opencompute-gpt2-tp-v0.2", "source": meta["source"],
            "workers": world, "prompt_ids": prompt_ids, "generated_ids": output_ids,
            "processed_tokens_per_layer": prefill_len + max_new_tokens - 1,
            "max_new_tokens": max_new_tokens, "elapsed_seconds_rank0": round(elapsed, 5),
            "tokens_per_second_rank0": round(max_new_tokens / max(elapsed, 1e-9), 3),
            "weight_bytes_per_rank": [int(v[0]) for v in gathered],
            "kv_cache_bytes_per_rank": [int(v[1]) for v in gathered],
            "peak_rss_kib_per_rank_linux": [int(v[2]) for v in gathered],
            **({"step_logits": logits_samples} if return_logits else {}),
        }
    return None


def main():
    parser = argparse.ArgumentParser(description="CPU distributed greedy GPT-2 decode")
    parser.add_argument("--shards", required=True)
    parser.add_argument("--prompt-ids", default="1,2,3")
    parser.add_argument("--prompt-text", help="Encode using exported tokenizer (requires transformers)")
    parser.add_argument("--max-new-tokens", type=int, default=5)
    parser.add_argument("--report-logits", action="store_true", help="Debug only; potentially large JSON")
    args = parser.parse_args()
    torch.set_num_threads(1)
    rank, world = int(os.environ["RANK"]), int(os.environ["WORLD_SIZE"])
    dist.init_process_group("gloo", timeout=timedelta(seconds=120))
    try:
        ids = []
        tokenizer = None
        if rank == 0:
            if args.prompt_text is not None:
                try:
                    from transformers import AutoTokenizer
                except ImportError as exc:
                    raise RuntimeError("pip install transformers to use --prompt-text") from exc
                tokenizer = AutoTokenizer.from_pretrained(Path(args.shards) / "tokenizer", local_files_only=True)
                ids = tokenizer.encode(args.prompt_text, add_special_tokens=False)
            else:
                ids = [int(v.strip()) for v in args.prompt_ids.split(",")]
        result = generate(Path(args.shards), ids, args.max_new_tokens, rank, world,
                          args.report_logits)
        if rank == 0:
            if tokenizer is not None:
                result["generated_text"] = tokenizer.decode(result["generated_ids"], skip_special_tokens=False)
            print(json.dumps(result, ensure_ascii=False), flush=True)
    finally:
        dist.destroy_process_group()


if __name__ == "__main__":
    main()