"""Measure actual Gloo collective wall times on the current host/network.

Run with torchrun. No synthetic penalty is injected. Tests loopback if local.
"""
import argparse
import json
import os
import statistics
import time
from datetime import timedelta

import torch
import torch.distributed as dist


def percentile95(items):
    ordered = sorted(items)
    return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * 0.95 + 0.5))]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--payloads-bytes", default="8,1024,65536")
    p.add_argument("--iterations", type=int, default=12)
    p.add_argument("--warmup", type=int, default=3)
    args = p.parse_args()
    payloads = [int(v) for v in args.payloads_bytes.split(",")]
    if not payloads or any(v <= 0 or v % 4 for v in payloads) or args.iterations < 1 or args.warmup < 0:
        p.error("payload bytes must be positive multiples of 4; iterations positive")
    torch.set_num_threads(1)
    dist.init_process_group("gloo", timeout=timedelta(seconds=120))
    rank, world = dist.get_rank(), dist.get_world_size()
    reports = []
    try:
        for size in payloads:
            data = torch.ones(size // 4, dtype=torch.float32)
            for kind in ("broadcast", "all_reduce"):
                measurements = []
                for step in range(args.iterations + args.warmup):
                    dist.barrier()
                    start = time.perf_counter()
                    if kind == "broadcast":
                        dist.broadcast(data, src=0)
                    else:
                        dist.all_reduce(data)
                        data.fill_(1)
                    elapsed = time.perf_counter() - start
                    if step >= args.warmup:
                        measurements.append(elapsed)
                local = torch.tensor(measurements, dtype=torch.float64)
                gathered = [torch.empty_like(local) for _ in range(world)]
                dist.all_gather(gathered, local)
                if rank == 0:
                    worst_per_iteration = torch.stack(gathered).max(0).values.tolist()
                    reports.append({
                        "workers": world, "kind": kind, "payload_bytes": size,
                        "median_ms": round(statistics.median(worst_per_iteration) * 1000, 5),
                        "p95_ms": round(percentile95(worst_per_iteration) * 1000, 5),
                        "measurement": "actual Gloo wall time, no artificial delay",
                        "host_mode": "localhost" if os.environ.get("LOCAL_WORLD_SIZE") == str(world) else "possibly-multi-host",
                    })
        if rank == 0:
            print(json.dumps({"probe": "gloo_cpu_collectives", "results": reports}), flush=True)
    finally:
        dist.destroy_process_group()


if __name__ == "__main__":
    main()
