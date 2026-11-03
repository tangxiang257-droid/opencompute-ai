"""Single-host TP decode with *added* modeled WAN delays (not traffic shaping)."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

from compute.gpt2_shards import export_state, synthetic_state


def sweep(workers=2, hidden=32, heads=4, layers=2, new_tokens=4,
          latency_values=(0, 1, 5), bandwidth_values=(0, 100, 10)):
    if workers < 1 or heads % workers or min(hidden, heads, layers, new_tokens) < 1:
        raise ValueError("invalid model dimensions")
    if any(n < 0 for n in latency_values) or any(n < 0 for n in bandwidth_values):
        raise ValueError("latencies/bandwidths must be nonnegative")
    state, config = synthetic_state(hidden=hidden, heads=heads, layers=layers)
    results = []
    with tempfile.TemporaryDirectory(prefix="oc-net-sweep-") as temp:
        root = Path(temp)
        export_state(state, config, root, workers, "synthetic-network-sweep")
        for latency in latency_values:
            for bandwidth in bandwidth_values:
                command = [sys.executable, "-m", "torch.distributed.run", "--standalone",
                           f"--nproc-per-node={workers}", "-m", "compute.gpt2_generate",
                           "--shards", str(root), "--prompt-ids", "1,2,3,4",
                           "--max-new-tokens", str(new_tokens),
                           "--net-latency-ms", str(latency),
                           "--net-bandwidth-mbps", str(bandwidth)]
                completed = subprocess.run(command, capture_output=True, text=True,
                                           timeout=180, env={**os.environ, "OMP_NUM_THREADS": "1"})
                if completed.returncode:
                    raise RuntimeError(completed.stderr[-3000:])
                reports = [json.loads(line) for line in completed.stdout.splitlines()
                           if line.startswith('{"engine":')]
                if len(reports) != 1:
                    raise RuntimeError("missing network sweep report")
                results.append(reports[0])
    return results


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--workers", type=int, default=2)
    p.add_argument("--hidden", type=int, default=32)
    p.add_argument("--heads", type=int, default=4)
    p.add_argument("--layers", type=int, default=2)
    p.add_argument("--new-tokens", type=int, default=4)
    p.add_argument("--latencies-ms", default="0,1,5")
    p.add_argument("--bandwidths-mbps", default="0,100,10")
    a = p.parse_args()
    print(json.dumps(sweep(a.workers, a.hidden, a.heads, a.layers,
                           a.new_tokens,
                           tuple(float(v) for v in a.latencies_ms.split(",")),
                           tuple(float(v) for v in a.bandwidths_mbps.split(","))),
                     ensure_ascii=False, indent=2))
