"""Repeatable CPU benchmarks for one, two and four workers."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

from compute.shards import prepare


def benchmark(workers=(1, 2, 4), hidden=512, batch=16, iterations=10):
    if any(hidden % n for n in workers):
        raise ValueError("hidden must be divisible by all worker counts")
    results = []
    with tempfile.TemporaryDirectory(prefix="opencompute-") as tmp:
        for n in workers:
            directory = Path(tmp) / str(n)
            prepare(str(directory), hidden, n)
            cmd = [sys.executable, "-m", "torch.distributed.run", "--standalone",
                   f"--nproc-per-node={n}", "-m", "compute.sharded_demo",
                   "--shards", str(directory), "--batch", str(batch),
                   "--iterations", str(iterations)]
            completed = subprocess.run(cmd, capture_output=True, text=True,
                                       timeout=180, env={**os.environ, "OMP_NUM_THREADS": "1"})
            if completed.returncode:
                raise RuntimeError(completed.stderr[-3000:])
            parsed = None
            for line in reversed(completed.stdout.splitlines()):
                try:
                    parsed = json.loads(line)
                    break
                except json.JSONDecodeError:
                    continue
            if parsed is None or not parsed["correct"]:
                raise RuntimeError("missing or incorrect benchmark output")
            results.append(parsed)
    baseline = results[0]["mean_total_ms"]
    for result in results:
        result["speedup_vs_one_worker"] = round(baseline / result["mean_total_ms"], 4)
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--hidden", type=int, default=512)
    parser.add_argument("--batch", type=int, default=16)
    parser.add_argument("--iterations", type=int, default=10)
    args = parser.parse_args()
    print(json.dumps(benchmark(hidden=args.hidden, batch=args.batch,
                               iterations=args.iterations), indent=2))
