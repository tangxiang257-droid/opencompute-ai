"""A/B test removed token-ID broadcasts (actual timings, optional synthetic network wait)."""
import argparse
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import tempfile

from compute.gpt2_shards import export_state, synthetic_state


def compare(workers=2, hidden=32, heads=4, layers=2, new_tokens=8,
            latency_ms=0.0, bandwidth_mbps=0.0, repeats=3):
    if workers < 1 or heads % workers or repeats < 1:
        raise ValueError("invalid workers or repeats")
    state, config = synthetic_state(hidden=hidden, heads=heads, layers=layers)
    observations = {"legacy": [], "optimized": []}
    with tempfile.TemporaryDirectory(prefix="oc-ab-") as root:
        export_state(state, config, root, workers, "synthetic-ab")
        # Interleave legacy and optimized to reduce order effects.
        for _ in range(repeats):
            for variant in ("legacy", "optimized"):
                cmd = [sys.executable, "-m", "torch.distributed.run", "--standalone",
                       f"--nproc-per-node={workers}", "-m", "compute.gpt2_generate",
                       "--shards", root, "--prompt-ids", "1,2,3,4",
                       "--max-new-tokens", str(new_tokens),
                       "--net-latency-ms", str(latency_ms),
                       "--net-bandwidth-mbps", str(bandwidth_mbps)]
                if variant == "legacy":
                    cmd.append("--legacy-token-broadcast")
                process = subprocess.run(cmd, capture_output=True, text=True,
                                         timeout=180, env={**os.environ, "OMP_NUM_THREADS": "1"})
                if process.returncode:
                    raise RuntimeError(process.stderr[-3000:])
                reports = [json.loads(line) for line in process.stdout.splitlines()
                           if line.startswith('{"engine":')]
                if len(reports) != 1:
                    raise RuntimeError("missing A/B report")
                observations[variant].append(reports[0])
    old = observations["legacy"]
    new = observations["optimized"]
    if any(a["generated_ids"] != b["generated_ids"] for a, b in zip(old, new)):
        raise AssertionError("optimized token generation differs from legacy")
    def summary(records):
        return {
            "median_rank0_elapsed_s": round(statistics.median(
                r["elapsed_seconds_rank0"] for r in records), 6),
            "broadcasts_per_run": records[0]["network_profile"]["calls"]["broadcast"],
            "all_reduces_per_run": records[0]["network_profile"]["calls"]["all_reduce"],
            "median_injected_wait_s": round(statistics.median(
                r["network_profile"]["injected_seconds"] for r in records), 6),
        }
    return {
        "workers": workers, "new_tokens": new_tokens, "repeats": repeats,
        "added_latency_ms": latency_ms, "added_bandwidth_mbps": bandwidth_mbps,
        "legacy": summary(old), "optimized": summary(new),
        "warning": "Local single-host timing; optional delay is simulated and additional to Gloo.",
    }


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--workers", type=int, default=2)
    p.add_argument("--hidden", type=int, default=32)
    p.add_argument("--heads", type=int, default=4)
    p.add_argument("--layers", type=int, default=2)
    p.add_argument("--new-tokens", type=int, default=8)
    p.add_argument("--latency-ms", type=float, default=0)
    p.add_argument("--bandwidth-mbps", type=float, default=0)
    p.add_argument("--repeats", type=int, default=3)
    a = p.parse_args()
    print(json.dumps(compare(a.workers, a.hidden, a.heads, a.layers, a.new_tokens,
                             a.latency_ms, a.bandwidth_mbps, a.repeats), indent=2))
