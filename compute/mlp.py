"""Two-layer GELU MLP with column/row tensor parallelism."""
import argparse
import json
import os
import time
from datetime import timedelta
from pathlib import Path
import torch
import torch.distributed as dist

def matrix(rows, cols, row_offset=0, col_offset=0):
    r = torch.arange(row_offset, row_offset + rows, dtype=torch.float64)[:, None]
    c = torch.arange(col_offset, col_offset + cols, dtype=torch.float64)[None, :]
    return (torch.sin(r * .013 + c * .017) * .1).float()

def prepare_mlp(path, hidden, expansion, world):
    if min(hidden, expansion, world) < 1 or expansion % world:
        raise ValueError("expansion must be divisible by positive world size")
    root = Path(path)
    root.mkdir(parents=True, exist_ok=True)
    width = expansion // world
    for rank in range(world):
        w1 = matrix(hidden, width, col_offset=rank * width)
        w2 = matrix(width, hidden, row_offset=rank * width + 100000)
        torch.save({"w1": w1, "w2": w2}, root / f"rank_{rank}.pt")
    (root / "manifest.json").write_text(json.dumps({"hidden": hidden, "expansion": expansion, "world": world}))

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--shards", required=True)
    p.add_argument("--batch", type=int, default=8)
    p.add_argument("--iterations", type=int, default=5)
    p.add_argument("--warmup", type=int, default=2)
    p.add_argument("--latency-ms", type=float, default=0)
    a = p.parse_args()
    if a.batch < 1 or a.iterations < 1 or a.warmup < 0 or a.latency_ms < 0:
        p.error("invalid benchmark parameters")
    rank, world = int(os.environ["RANK"]), int(os.environ["WORLD_SIZE"])
    torch.set_num_threads(1)
    dist.init_process_group("gloo", timeout=timedelta(seconds=120))
    try:
        root = Path(a.shards)
        meta = json.loads((root / "manifest.json").read_text())
        hidden, expansion = meta["hidden"], meta["expansion"]
        if world != meta["world"] or expansion % world:
            raise ValueError("manifest does not match process group")
        width = expansion // world
        weights = torch.load(root / f"rank_{rank}.pt", weights_only=True, map_location="cpu")
        w1, w2 = weights["w1"], weights["w2"]
        if w1.shape != (hidden, width) or w2.shape != (width, hidden):
            raise ValueError("incorrect shard shapes")
        torch.manual_seed(1729)
        x = torch.randn(a.batch, hidden)
        expected = None
        if rank == 0:
            expected = torch.zeros(a.batch, hidden)
            for i in range(world):
                first = matrix(hidden, width, col_offset=i * width)
                second = matrix(width, hidden, row_offset=i * width + 100000)
                expected += torch.nn.functional.gelu(x @ first) @ second
                del first, second
        samples, error = [], 0.
        for step in range(a.warmup + a.iterations):
            dist.barrier()
            start = time.perf_counter()
            partial = torch.nn.functional.gelu(x @ w1) @ w2
            computed = time.perf_counter()
            if a.latency_ms:
                time.sleep(a.latency_ms / 1000)
            dist.all_reduce(partial, op=dist.ReduceOp.SUM)
            finish = time.perf_counter()
            if rank == 0:
                error = max(error, (partial - expected).abs().max().item())
            if step >= a.warmup:
                samples.append([computed-start, finish-computed, finish-start])
        local = torch.tensor(samples, dtype=torch.float64)
        gathered = [torch.empty_like(local) for _ in range(world)]
        dist.all_gather(gathered, local)
        if rank == 0:
            elapsed = torch.stack(gathered).max(0).values
            print(json.dumps({
                "model": "gelu_mlp", "workers": world, "hidden": hidden,
                "expansion": expansion, "batch": a.batch, "latency_ms": a.latency_ms,
                "correct": error < 1e-3, "max_abs_error": error,
                "mean_compute_ms_upper_bound": round(elapsed[:,0].mean().item()*1000,4),
                "mean_communication_ms_upper_bound": round(elapsed[:,1].mean().item()*1000,4),
                "mean_total_ms": round(elapsed[:,2].mean().item()*1000,4),
                "local_weight_bytes": (w1.numel()+w2.numel())*4,
                "full_weight_bytes": 2*hidden*expansion*4,
                "full_weights_loaded": False}), flush=True)
            if error >= 1e-3:
                raise RuntimeError("MLP mismatch")
    finally:
        dist.destroy_process_group()

if __name__ == "__main__":
    main()
