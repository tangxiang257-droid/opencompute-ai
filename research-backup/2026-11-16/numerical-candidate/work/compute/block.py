"""Synthetic transformer block: pre-norm causal TP attention + TP GELU MLP.

Both sublayers perform independent local work then one all-reduce each.
This is not a pretrained language model or token sampler.
"""
import argparse
import json
import os
import tempfile
from pathlib import Path
import torch
import torch.distributed as dist

from compute.attention import prepare_attention, load_shard, local_attention, reference
from compute.mlp import prepare_mlp


def local_block(x, attn_weights, mlp_weights, local_heads, cache):
    normalized = torch.nn.functional.layer_norm(x, (x.shape[-1],))
    attn_partial, cache = local_attention(normalized, attn_weights, local_heads, cache)
    dist.all_reduce(attn_partial)
    residual = x + attn_partial
    normalized = torch.nn.functional.layer_norm(residual, (residual.shape[-1],))
    mlp_partial = torch.nn.functional.gelu(normalized @ mlp_weights["w1"]) @ mlp_weights["w2"]
    dist.all_reduce(mlp_partial)
    return residual + mlp_partial, cache


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--hidden", type=int, default=32)
    p.add_argument("--heads", type=int, default=4)
    p.add_argument("--expansion", type=int, default=64)
    p.add_argument("--tokens", type=int, default=4)
    args = p.parse_args()
    rank, world = int(os.environ["RANK"]), int(os.environ["WORLD_SIZE"])
    if args.hidden < 1 or args.heads < 1 or args.expansion < 1 or args.tokens < 1 or args.hidden % args.heads or args.heads % world or args.expansion % world:
        p.error("invalid tensor-parallel dimensions")
    torch.set_num_threads(1)
    dist.init_process_group("gloo")
    try:
        with tempfile.TemporaryDirectory() as tmp:
            attn_dir, mlp_dir = str(Path(tmp) / "attn"), str(Path(tmp) / "mlp")
            # Single-host prototype; each process independently prepares deterministic
            # shard files in its own temporary directory.
            prepare_attention(attn_dir, args.hidden, args.heads, world)
            prepare_mlp(mlp_dir, args.hidden, args.expansion, world)
            aw = load_shard(attn_dir, rank, args.hidden, args.heads, world)
            mw = torch.load(Path(mlp_dir) / f"rank_{rank}.pt", weights_only=True)
            torch.manual_seed(919)
            x = torch.randn(2, args.tokens, args.hidden)
            output, _ = local_block(x, aw, mw, args.heads // world, None)
            if rank == 0:
                normalized = torch.nn.functional.layer_norm(x, (args.hidden,))
                attn, _ = reference(normalized, args.hidden, args.heads, world)
                residual = x + attn
                normalized = torch.nn.functional.layer_norm(residual, (args.hidden,))
                mlp_sum = torch.zeros_like(residual)
                from compute.mlp import matrix
                width = args.expansion // world
                for i in range(world):
                    w1 = matrix(args.hidden, width, col_offset=i * width)
                    w2 = matrix(width, args.hidden, row_offset=i * width + 100000)
                    mlp_sum += torch.nn.functional.gelu(normalized @ w1) @ w2
                expected = residual + mlp_sum
                error = (output - expected).abs().max().item()
                print(json.dumps({"model": "transformer_block_tp", "workers": world,
                                  "max_abs_error": error, "correct": error < 1e-4}), flush=True)
                if error >= 1e-4:
                    raise RuntimeError("block output mismatch")
    finally:
        dist.destroy_process_group()


if __name__ == "__main__":
    main()
