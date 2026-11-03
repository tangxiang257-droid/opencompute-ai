"""Measure greedy distributed decode using identical synthetic GPT-2 shapes.

A single-host CPU microbenchmark, not a multi-machine speedup claim.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

from compute.gpt2_shards import export_state, synthetic_state


def benchmark(workers=(1, 2, 4), hidden=32, heads=4, layers=2,
              prompt_ids=(1, 2, 3, 4), new_tokens=8):
    if not workers or any(n <= 0 or heads % n for n in workers):
        raise ValueError("worker counts must divide attention heads")
    state, config = synthetic_state(hidden=hidden, heads=heads, layers=layers)
    measurements = []
    with tempfile.TemporaryDirectory(prefix="opencompute-generate-") as temp:
        for n in workers:
            directory = Path(temp) / f"workers-{n}"
            export_state(state, config, directory, n, "synthetic-benchmark")
            cmd = [sys.executable, "-m", "torch.distributed.run", "--standalone",
                   f"--nproc-per-node={n}", "-m", "compute.gpt2_generate",
                   "--shards", str(directory), "--prompt-ids",
                   ",".join(map(str, prompt_ids)), "--max-new-tokens", str(new_tokens)]
            completed = subprocess.run(cmd, capture_output=True, text=True, timeout=180,
                                       env={**os.environ, "OMP_NUM_THREADS": "1"})
            if completed.returncode:
                raise RuntimeError(completed.stderr[-4000:])
            reports = [json.loads(line) for line in completed.stdout.splitlines()
                       if line.startswith('{"engine":')]
            if len(reports) != 1:
                raise RuntimeError("generation report missing or duplicated")
            measurements.append(reports[0])
    baseline = measurements[0]["tokens_per_second_rank0"]
    for record in measurements:
        record["throughput_relative_to_first"] = round(
            record["tokens_per_second_rank0"] / max(baseline, 1e-9), 3)
    return measurements


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", default="1,2,4")
    parser.add_argument("--hidden", type=int, default=32)
    parser.add_argument("--heads", type=int, default=4)
    parser.add_argument("--layers", type=int, default=2)
    parser.add_argument("--new-tokens", type=int, default=8)
    args = parser.parse_args()
    records = benchmark(tuple(int(x) for x in args.workers.split(",")),
                        args.hidden, args.heads, args.layers,
                        new_tokens=args.new_tokens)
    print(json.dumps(records, indent=2))


if __name__ == "__main__":
    main()