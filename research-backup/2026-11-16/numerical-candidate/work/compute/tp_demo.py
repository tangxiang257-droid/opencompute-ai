"""CPU tensor parallel demo: each rank owns only its weight shard."""
import argparse
import json
import os
import time
from datetime import timedelta

import torch
import torch.distributed as dist


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--hidden", type=int, default=512)
    parser.add_argument("--batch", type=int, default=16)
    parser.add_argument("--iterations", type=int, default=5)
    args = parser.parse_args()
    rank = int(os.environ["RANK"])
    world = int(os.environ["WORLD_SIZE"])
    if args.hidden <= 0 or args.batch <= 0 or args.iterations <= 0 or args.hidden % world:
        parser.error("positive sizes required; hidden must be divisible by WORLD_SIZE")
    torch.set_num_threads(1)
    dist.init_process_group("gloo", timeout=timedelta(seconds=120))
    try:
        torch.manual_seed(2026)
        x = torch.randn(args.batch, args.hidden)
        # Each rank independently regenerates deterministic reference weights for
        # validation only. Production should load only the local shard from disk.
        w = torch.randn(args.hidden, args.hidden)
        shard = args.hidden // world
        local_w = w[:, rank * shard:(rank + 1) * shard].clone()
        # Row parallel: partition X and W along the contracted dimension.
        x_local = x[:, rank * shard:(rank + 1) * shard].contiguous()
        row_w = w[rank * shard:(rank + 1) * shard, :].clone()
        del local_w
        expected = x @ w if rank == 0 else None
        del w
        times = []
        error = 0.0
        for _ in range(args.iterations):
            dist.barrier()
            start = time.perf_counter()
            partial = x_local @ row_w
            compute_end = time.perf_counter()
            dist.all_reduce(partial, op=dist.ReduceOp.SUM)
            end = time.perf_counter()
            if rank == 0:
                error = max(error, (partial - expected).abs().max().item())
            times.append((compute_end - start, end - compute_end, end - start))
        local = torch.tensor(times, dtype=torch.float64)
        gathered = [torch.empty_like(local) for _ in range(world)]
        dist.all_gather(gathered, local)
        if rank == 0:
            # Per iteration wall clock is determined by the slowest rank.
            elapsed = torch.stack(gathered).max(dim=0).values
            report = {
                "workers": world,
                "hidden": args.hidden,
                "batch": args.batch,
                "iterations": args.iterations,
                "max_abs_error": error,
                "correct": error < 1e-3,
                "mean_compute_ms_upper_bound": round(elapsed[:, 0].mean().item() * 1000, 3),
                "mean_collective_ms_upper_bound": round(elapsed[:, 1].mean().item() * 1000, 3),
                "mean_total_ms": round(elapsed[:, 2].mean().item() * 1000, 3),
                "weight_elements_per_worker": row_w.numel(),
                "full_weight_elements": args.hidden * args.hidden,
                "note": "Reference weights are temporarily regenerated on all ranks for validation; this is not a memory-capacity benchmark."
            }
            print(json.dumps(report, ensure_ascii=False), flush=True)
            if not report["correct"]:
                raise RuntimeError("Distributed output differs from reference")
    finally:
        dist.destroy_process_group()


if __name__ == "__main__":
    main()
