"""Communication-aware GPT-2 TP placement *estimator*, not a measured speed predictor.

Reports exact float32 exported parameter sizes and logical KV cache size for
the current GPT-2 shard layout. RAM headroom is an operator-provided reserve.
"""
import argparse
import json
import math
from pathlib import Path


BYTES_PER_ELEMENT = 4
MIB = 1024 * 1024


def _positive_int(name, value):
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")


def plan(config, *, devices=4, memory_mib=1024, runtime_reserve_mib=256,
         prompt_tokens=16, new_tokens=16, latency_ms=1.0,
         bandwidth_mbps=100.0):
    """Choose smallest feasible tensor-parallel group; no compute-speed claim."""
    for key in ("hidden", "heads", "layers", "positions", "inner", "vocab"):
        _positive_int(key, config.get(key))
    for key, value in (("devices", devices), ("prompt_tokens", prompt_tokens),
                       ("new_tokens", new_tokens)):
        _positive_int(key, value)
    for name, value in (("memory_mib", memory_mib),
                        ("runtime_reserve_mib", runtime_reserve_mib),
                        ("latency_ms", latency_ms), ("bandwidth_mbps", bandwidth_mbps)):
        if not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
            raise ValueError(f"{name} must be a finite nonnegative number")
    if memory_mib <= 0 or bandwidth_mbps <= 0:
        raise ValueError("memory_mib and bandwidth_mbps must be positive")
    if config["hidden"] % config["heads"]:
        raise ValueError("model hidden must divide evenly into attention heads")
    if prompt_tokens + new_tokens - 1 > config["positions"]:
        raise ValueError("requested sequence exceeds model context")
    h, l, inner, vocab, pos = (config[x] for x in
                              ("hidden", "layers", "inner", "vocab", "positions"))
    steps = new_tokens
    seq_len = prompt_tokens + new_tokens - 1
    evaluations = []
    for workers in range(1, devices + 1):
        if config["heads"] % workers or inner % workers:
            evaluations.append({"workers": workers, "feasible": False,
                                "reason": "heads/MLP not divisible by workers"})
            continue
        shared_layer_elements = 6 * h
        local_layer_elements = (4 * h * h + 2 * h * inner + 3 * h + inner) // workers
        layer_bytes = l * (shared_layer_elements + local_layer_elements) * BYTES_PER_ELEMENT
        rank0_io_bytes = (vocab * h + pos * h + 2 * h) * BYTES_PER_ELEMENT
        cache_bytes = 2 * l * seq_len * (h // workers) * BYTES_PER_ELEMENT
        required_rank0 = layer_bytes + rank0_io_bytes + cache_bytes
        required_peer = layer_bytes + cache_bytes
        estimated_peak_mib = required_rank0 / MIB + runtime_reserve_mib
        feasible = estimated_peak_mib <= memory_mib
        call_count = 0 if workers == 1 else steps * (1 + 2 * l)
        # Each forward receives the prompt once and each subsequent token once.
        activation_bytes = (prompt_tokens + steps - 1) * h * BYTES_PER_ELEMENT
        if workers > 1:
            ring_factor = 2 * (workers - 1) / workers
            modeled_wire = activation_bytes * (1 + 2 * l * ring_factor)
            wait_ms = (call_count * latency_ms +
                       modeled_wire * 8 * 1000 / (bandwidth_mbps * 1e6))
        else:
            modeled_wire, wait_ms = 0, 0.0
        evaluations.append({
            "workers": workers,
            "feasible": feasible,
            "reason": "estimated weight + KV + reserve fits" if feasible
                      else "rank-0 estimated memory exceeds device budget",
            "rank0_weights_bytes": layer_bytes + rank0_io_bytes,
            "peer_weights_bytes": layer_bytes,
            "kv_cache_bytes_per_rank": cache_bytes,
            "estimated_rank0_memory_mib": round(estimated_peak_mib, 4),
            "estimated_peer_memory_mib": round(
                required_peer / MIB + runtime_reserve_mib, 4),
            "network_collectives_per_generation": call_count,
            "modeled_wire_bytes_per_rank": round(modeled_wire),
            "modeled_added_network_wait_ms": round(wait_ms, 4),
        })
    allowed = [candidate for candidate in evaluations if candidate["feasible"]]
    # Without measured per-worker FLOPS and calibrated Gloo timing, smallest TP
    # group is the defensible capacity-first heuristic (minimum collectives).
    selected = min(allowed, key=lambda c: (c["workers"], c["modeled_added_network_wait_ms"])) if allowed else None
    return {
        "selection_policy": "smallest capacity-feasible TP group; NOT a speed predictor",
        "selected_workers": selected["workers"] if selected else None,
        "fits_any": bool(allowed),
        "candidates": evaluations,
        "assumptions": {
            "dtype": "float32", "memory_mib_per_device": memory_mib,
            "runtime_reserve_mib": runtime_reserve_mib,
            "latency_ms_per_collective": latency_ms,
            "assumed_bandwidth_mbps": bandwidth_mbps,
            "unmodeled": ["compute time", "activation peak", "allocator fragmentation",
                           "actual Gloo algorithm", "heterogeneous devices",
                           "checkpoint loading peak", "network contention"],
            "rank0_io_unsharded": True,
        },
    }


def main():
    p = argparse.ArgumentParser(description="Estimate suitable TP worker count")
    p.add_argument("--manifest", help="Exported GPT-2 manifest JSON")
    p.add_argument("--devices", type=int, default=4)
    p.add_argument("--memory-mib", type=float, default=1024)
    p.add_argument("--runtime-reserve-mib", type=float, default=256)
    p.add_argument("--prompt-tokens", type=int, default=16)
    p.add_argument("--new-tokens", type=int, default=16)
    p.add_argument("--latency-ms", type=float, default=1)
    p.add_argument("--bandwidth-mbps", type=float, default=100)
    a = p.parse_args()
    config = json.loads(Path(a.manifest).read_text()) if a.manifest else {
        "hidden": 768, "heads": 12, "layers": 12, "positions": 1024,
        "inner": 3072, "vocab": 50257,
    }
    report = plan(config, devices=a.devices, memory_mib=a.memory_mib,
                  runtime_reserve_mib=a.runtime_reserve_mib,
                  prompt_tokens=a.prompt_tokens, new_tokens=a.new_tokens,
                  latency_ms=a.latency_ms, bandwidth_mbps=a.bandwidth_mbps)
    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
