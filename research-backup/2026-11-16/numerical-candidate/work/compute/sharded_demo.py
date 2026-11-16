"""Distributed row-parallel matrix multiplication, loading only rank-local weights."""
import argparse
import json
import os
import resource
import time
from datetime import timedelta
from pathlib import Path

import torch
import torch.distributed as dist

try:
    from compute.shards import weight_rows
except ModuleNotFoundError:
    from shards import weight_rows


def run():
    p = argparse.ArgumentParser()
    p.add_argument("--shards", required=True)
    p.add_argument("--batch", type=int, default=16)
    p.add_argument("--iterations", type=int, default=10)
    p.add_argument("--warmup", type=int, default=3)
    args = p.parse_args()
    if args.batch < 1 or args.iterations < 1 or args.warmup < 0:
        p.error("batch and iterations must be positive; warmup must be nonnegative")
    rank, world = int(os.environ["RANK"]), int(os.environ["WORLD_SIZE"])
    torch.set_num_threads(1)
    dist.init_process_group("gloo", timeout=timedelta(seconds=120))
    try:
        root = Path(args.shards)
        manifest = json.loads((root / "manifest.json").read_text())
        hidden = manifest["hidden"]
        if manifest["workers"] != world:
            raise ValueError("manifest worker count differs from process group")
        shard_rows = hidden // world
        # weights_only=True avoids untrusted pickle object deserialization.
        local_w = torch.load(root / f"rank_{rank}.pt", map_location="cpu", weights_only=True)
        if local_w.shape != (shard_rows, hidden) or local_w.dtype != torch.float32:
            raise ValueError("invalid local weight shard")
        torch.manual_seed(1234)
        x = torch.randn(args.batch, hidden)
        x_local = x[:, rank * shard_rows:(rank + 1) * shard_rows].contiguous()
        del x
        # The reference is evaluated in streaming row blocks on rank zero.
        # No worker ever loads or reconstructs the full weight matrix.
        expected = None
        if rank == 0:
            torch.manual_seed(1234)
            ref_x = torch.randn(args.batch, hidden)
            expected = torch.zeros(args.batch, hidden)
            for start in range(0, hidden, shard_rows):
                stop = start + shard_rows
                expected += ref_x[:, start:stop] @ weight_rows(start, stop, hidden)
            del ref_x
        samples = []
        error = 0.0
        for step in range(args.warmup + args.iterations):
            dist.barrier()
            start = time.perf_counter()
            partial = x_local @ local_w
            computed = time.perf_counter()
            dist.all_reduce(partial, op=dist.ReduceOp.SUM)
            finished = time.perf_counter()
            if rank == 0:
                error = max(error, (partial - expected).abs().max().item())
            if step >= args.warmup:
                samples.append([computed - start, finished - computed, finished - start])
        local = torch.tensor(samples, dtype=torch.float64)
        all_samples = [torch.empty_like(local) for _ in range(world)]
        dist.all_gather(all_samples, local)
        # ru_maxrss is KiB on Linux (GitHub Actions / Docker target).
        # It measures peak process RSS, including runtime and buffers, not just weights.
        rss_kib = torch.tensor([resource.getrusage(resource.RUSAGE_SELF).ru_maxrss], dtype=torch.int64)
        all_rss = [torch.zeros_like(rss_kib) for _ in range(world)]
        dist.all_gather(all_rss, rss_kib)
        if rank == 0:
            per_step = torch.stack(all_samples).max(dim=0).values
            report = {
                "workers": world, "hidden": hidden, "batch": args.batch,
                "iterations": args.iterations, "warmup": args.warmup,
                "max_abs_error": error, "correct": error < 1e-3,
                "mean_compute_ms_upper_bound": round(per_step[:, 0].mean().item() * 1000, 4),
                "mean_collective_ms_upper_bound": round(per_step[:, 1].mean().item() * 1000, 4),
                "mean_total_ms": round(per_step[:, 2].mean().item() * 1000, 4),
                "weight_elements_per_worker": local_w.numel(),
                "full_weight_elements": hidden * hidden,
                "full_weight_materialized": False,
                "peak_worker_rss_kib": [int(v.item()) for v in all_rss],
                "theoretical_local_weight_bytes": local_w.numel() * local_w.element_size(),
                "note": "Rank-local disk shards; reference computed in streaming blocks on rank 0."
            }
            print(json.dumps(report), flush=True)
            if not report["correct"]:
                raise RuntimeError("incorrect distributed output")
    finally:
        dist.destroy_process_group()


if __name__ == "__main__":
    run()
