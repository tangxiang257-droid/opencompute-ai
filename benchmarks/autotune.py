"""Capacity-filtered, measured CPU tensor-parallel worker-count tuner.

Measures each *feasible* worker count on a single host with the same synthetic
GPT-2 weights. Not an estimator for heterogeneous devices or internet peers.
"""
import argparse
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import tempfile

from compute.gpt2_shards import export_state, synthetic_state
from compute.topology_planner import plan


def best_observed(records):
    successful = [row for row in records if row.get("measured_median_seconds") is not None]
    return min(successful, key=lambda row: (
        row["measured_median_seconds"], row["workers"])) if successful else None


def tune(*, devices=4, hidden=32, heads=4, layers=2, prompt_ids=(1, 2, 3),
         new_tokens=4, memory_mib=512, runtime_reserve_mib=256,
         latency_ms=0.0, bandwidth_mbps=100, simulate_network=False,
         repeats=3):
    if repeats <= 0 or len(prompt_ids) == 0:
        raise ValueError("repeats and prompt must be positive")
    state, config = synthetic_state(hidden=hidden, heads=heads, layers=layers)
    manifest = {"hidden": hidden, "heads": heads, "layers": layers,
                "positions": config["n_positions"], "inner": config["n_inner"],
                "vocab": config["vocab_size"]}
    sizing = plan(manifest, devices=devices, memory_mib=memory_mib,
                  runtime_reserve_mib=runtime_reserve_mib,
                  prompt_tokens=len(prompt_ids), new_tokens=new_tokens,
                  latency_ms=latency_ms, bandwidth_mbps=bandwidth_mbps)
    measurements = []
    with tempfile.TemporaryDirectory(prefix="opencompute-tune-") as tmp:
        for candidate in sizing["candidates"]:
            if not candidate["feasible"]:
                continue
            workers = candidate["workers"]
            folder = Path(tmp) / f"tp-{workers}"
            export_state(state, config, folder, workers, "synthetic-autotune")
            timings, calls = [], None
            for _ in range(repeats):
                cmd = [sys.executable, "-m", "torch.distributed.run", "--standalone",
                       f"--nproc-per-node={workers}", "-m", "compute.gpt2_generate",
                       "--shards", str(folder), "--prompt-ids",
                       ",".join(map(str, prompt_ids)),
                       "--max-new-tokens", str(new_tokens)]
                if simulate_network:
                    cmd += ["--net-latency-ms", str(latency_ms),
                            "--net-bandwidth-mbps", str(bandwidth_mbps)]
                result = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=180,
                    env={**os.environ, "OMP_NUM_THREADS": "1"})
                if result.returncode:
                    raise RuntimeError(f"TP={workers}: " + result.stderr[-2500:])
                reports = [json.loads(line) for line in result.stdout.splitlines()
                           if line.startswith('{"engine":')]
                if len(reports) != 1:
                    raise RuntimeError(f"TP={workers}: missing result")
                timings.append(reports[0]["elapsed_seconds_rank0"])
                calls = reports[0]["network_profile"]["calls"]
            measurements.append({
                "workers": workers,
                "measured_median_seconds": round(statistics.median(timings), 6),
                "measured_min_seconds": min(timings),
                "measured_max_seconds": max(timings),
                "collective_calls": calls,
                "estimated_rank0_memory_mib": candidate["estimated_rank0_memory_mib"],
            })
    fastest = best_observed(measurements)
    return {
        "capacity_first_workers": sizing["selected_workers"],
        "fastest_observed_workers_on_test_host": fastest["workers"] if fastest else None,
        "measurements": measurements,
        "sizing": sizing,
        "measurement_scope": "single-host CPU with synthetic GPT-2 weights",
        "injected_network_penalty": simulate_network,
        "warning": "Not transferable to other devices; measured timings have noise."
    }


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--devices", type=int, default=4)
    p.add_argument("--hidden", type=int, default=32)
    p.add_argument("--heads", type=int, default=4)
    p.add_argument("--layers", type=int, default=2)
    p.add_argument("--new-tokens", type=int, default=4)
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--memory-mib", type=float, default=512)
    p.add_argument("--runtime-reserve-mib", type=float, default=256)
    p.add_argument("--latency-ms", type=float, default=0)
    p.add_argument("--bandwidth-mbps", type=float, default=100)
    p.add_argument("--simulate-network", action="store_true")
    a = p.parse_args()
    print(json.dumps(tune(devices=a.devices, hidden=a.hidden, heads=a.heads,
                          layers=a.layers, new_tokens=a.new_tokens,
                          repeats=a.repeats, memory_mib=a.memory_mib,
                          runtime_reserve_mib=a.runtime_reserve_mib,
                          latency_ms=a.latency_ms, bandwidth_mbps=a.bandwidth_mbps,
                          simulate_network=a.simulate_network), indent=2))
