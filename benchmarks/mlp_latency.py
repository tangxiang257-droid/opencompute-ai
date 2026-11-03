"""Measure sensitivity to artificial pre-collective delays (not bandwidth emulation)."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from compute.mlp import prepare_mlp

def sweep(workers=2, hidden=256, expansion=512, batch=8,
          latency_values=(0, 1, 5, 20), iterations=5):
    if workers < 1 or hidden < 1 or expansion < 1 or batch < 1 or iterations < 1:
        raise ValueError("invalid benchmark dimensions")
    results = []
    with tempfile.TemporaryDirectory(prefix="oc-mlp-") as tmp:
        root = Path(tmp)
        prepare_mlp(root, hidden, expansion, workers)
        for latency in latency_values:
            if latency < 0:
                raise ValueError("latency must be nonnegative")
            cmd = [sys.executable, "-m", "torch.distributed.run", "--standalone",
                   f"--nproc-per-node={workers}", "-m", "compute.mlp",
                   "--shards", str(root), "--batch", str(batch),
                   "--iterations", str(iterations), "--latency-ms", str(latency)]
            result = subprocess.run(cmd, capture_output=True, text=True,
                                    timeout=180, env={**os.environ, "OMP_NUM_THREADS": "1"})
            if result.returncode:
                raise RuntimeError(result.stderr[-3000:])
            reports = [json.loads(line) for line in result.stdout.splitlines()
                       if line.startswith('{"model":')]
            if len(reports) != 1 or not reports[0]["correct"]:
                raise RuntimeError("missing or invalid MLP report")
            results.append(reports[0])
    return results

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--workers", type=int, default=2)
    p.add_argument("--hidden", type=int, default=256)
    p.add_argument("--expansion", type=int, default=512)
    p.add_argument("--batch", type=int, default=8)
    p.add_argument("--iterations", type=int, default=5)
    args = p.parse_args()
    print(json.dumps(sweep(args.workers, args.hidden, args.expansion,
                           args.batch, iterations=args.iterations), indent=2))
