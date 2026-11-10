"""Repeated same-host Qwen2 TP benchmark and controlled one-rank A/B test.

Uses real torchrun processes and independent synthetic dense references. It
does *not* train a model, download pretrained weights, or measure WAN traffic.
Each trial starts a new process group; warmups only prime host/filesystem
caches and do not warm PyTorch within a persistent process.
"""
import argparse
import json
import math
import os
from pathlib import Path
import platform
import statistics
import tempfile

import torch

from benchmarks.qwen2_matrix_v045 import launch_rank_group
from compute.qwen2_chunked import export_chunked
from compute.qwen2_reference import generate_dense
from compute.qwen2_shards import synthetic_state
from compute.qwen2_validation import (
    summarize_profile, validate_generation_parity)
from compute.parallel_screen_v046 import screening


SIZES = {
    "tiny": {"hidden": 24, "heads": 6, "kv_heads": 2,
             "layers": 2, "inner": 48, "vocab": 64, "positions": 64},
    "small": {"hidden": 96, "heads": 8, "kv_heads": 2,
              "layers": 4, "inner": 256, "vocab": 128, "positions": 64},
}


def sample_summary(values, *, nonnegative=False):
    if (not isinstance(values, (list, tuple)) or not 2 <= len(values) <= 30
            or any(type(v) not in (int, float) or not math.isfinite(v)
                   or v < 0 or (not nonnegative and v == 0)
                   for v in values)):
        raise ValueError("need 2..30 finite positive trial samples")
    ordered = sorted(float(v) for v in values)
    def nearest_quantile(percent):
        return ordered[min(len(ordered)-1, max(0, math.ceil(len(ordered)*percent)-1))]
    median = statistics.median(ordered)
    return {
        "samples": len(values),
        "median": round(median, 7),
        "min": round(ordered[0], 7),
        "max": round(ordered[-1], 7),
        "p10_nearest_rank": round(nearest_quantile(.10), 7),
        "p90_nearest_rank": round(nearest_quantile(.90), 7),
        "stdev": round(statistics.stdev(ordered), 7),
        "individual": [round(v, 7) for v in values],
        "quantile_note": "Nearest-rank quantiles; small sample sizes lack confidence intervals.",
    }


def modes_for(world):
    if world == 1:
        return ("local_fastpath", "legacy_collective")
    if world == 2:
        return ("tensor_parallel",)
    raise ValueError("local Gloo benchmark supports only 1 or 2 ranks")


def repeated_benchmark(*, model_size="tiny", storage_dtype="float32",
                       repeats=3, warmups=1, max_new_tokens=3,
                       output=None, timeout=100):
    if model_size not in SIZES:
        raise ValueError("model_size must be tiny or small")
    if storage_dtype not in ("float32", "bfloat16"):
        raise ValueError("storage_dtype must be float32 or bfloat16")
    if type(repeats) is not int or not 2 <= repeats <= 10:
        raise ValueError("repeats must be 2..10")
    if type(warmups) is not int or not 0 <= warmups <= 2:
        raise ValueError("warmups must be 0..2")
    if type(max_new_tokens) is not int or not 1 <= max_new_tokens <= 8:
        raise ValueError("new tokens must be 1..8")
    if type(timeout) is not int or not 5 <= timeout <= 600:
        raise ValueError("invalid process-group timeout")
    if output is not None and (Path(output).is_symlink() or Path(output).exists()):
        raise ValueError("will not overwrite an existing report")
    torch.set_num_threads(1)
    params = SIZES[model_size]
    state, cfg = synthetic_state(**params, seed=4180)
    expected_state = (
        {k: value.to(torch.bfloat16).float()
         for k, value in state.items()}
        if storage_dtype == "bfloat16" else state)
    prompt = [1, 2, 3]
    expected = generate_dense(expected_state, cfg, prompt, max_new_tokens)
    record = {
        "schema": "opencompute-qwen2-repeated-v046",
        "workload": {
            "synthetic_model": model_size, "random_seed": 4180,
            "dimensions": params, "storage_dtype": storage_dtype,
            "prompt_ids": prompt, "generated_tokens": max_new_tokens,
        },
        "environment": {
            "platform": platform.platform(), "python": platform.python_version(),
            "torch": torch.__version__, "OMP_NUM_THREADS": "1",
            "processes": "new torchrun job for every measurement",
        },
        "warmups_per_mode": warmups,
        "repeats_per_mode": repeats,
        "trials": {},
        "summaries": {},
        "interpretation": (
            "Single host, synthetic model; speedup is empirical one-run-group "
            "comparison, not WAN or real pretrained model performance."),
    }
    with tempfile.TemporaryDirectory(prefix="opencompute-v046-") as tmp:
        folders = {}
        for workers in (1, 2):
            folder = Path(tmp) / f"rank-{workers}"
            meta = export_chunked(state, cfg, folder, workers,
                                  storage_dtype=storage_dtype, chunk_rows=11,
                                  source="synthetic-v046")
            folders[workers] = (folder, meta)
        configs = [(1, "local_fastpath"), (1, "legacy_collective"),
                   (2, "tensor_parallel")]
        samples = {name: [] for _, name in configs}
        # Rotate execution order so one mode does not always run later.
        for round_index in range(repeats + warmups):
            order = configs[round_index % len(configs):] + configs[:round_index % len(configs)]
            for workers, mode in order:
                folder, meta = folders[workers]
                result, wall = launch_rank_group(
                    folder, workers=workers, max_new_tokens=max_new_tokens,
                    prompt=prompt, timeout=timeout,
                    local_fastpath=(mode != "legacy_collective"))
                checked = validate_generation_parity(
                    expected, result, atol=1e-5, rtol=1e-5,
                    reference_name="dense-same-storage-precision",
                    candidate_name=mode)
                if not checked["passed"]:
                    raise RuntimeError(f"TP output mismatch in {mode} round {round_index}")
                trace = summarize_profile(result, layers=meta["layers"],
                                          prompt_length=len(prompt))
                if round_index < warmups:
                    continue
                samples[mode].append({
                    "round": round_index - warmups,
                    "decode_seconds": result["elapsed_seconds_rank0"],
                    "whole_torchrun_seconds": round(wall, 7),
                    "tokens_per_second": result["tokens_per_second_rank0"],
                    "all_rank_collective_fraction": [
                        r["collective_fraction"] for r in trace["ranks"]],
                    "all_rank_peak_rss_bytes_linux": [
                        r["peak_rss_bytes_linux"] for r in trace["ranks"]],
                    "all_rank_tensor_payload_bytes": [
                        r["all_reduce_tensor_bytes"] for r in trace["ranks"]],
                    "parity_passed": True,
                })
        for _, mode in configs:
            record["trials"][mode] = samples[mode]
            record["summaries"][mode] = {
                "decode_seconds": sample_summary(
                    [i["decode_seconds"] for i in samples[mode]]),
                "whole_torchrun_seconds": sample_summary(
                    [i["whole_torchrun_seconds"] for i in samples[mode]]),
                "tokens_per_second": sample_summary(
                    [i["tokens_per_second"] for i in samples[mode]]),
                "collective_fraction_max": sample_summary([
                    max(i["all_rank_collective_fraction"])
                    for i in samples[mode]], nonnegative=True),
            }
        baseline = record["summaries"]["legacy_collective"]["decode_seconds"]["median"]
        fast = record["summaries"]["local_fastpath"]["decode_seconds"]["median"]
        tp2 = record["summaries"]["tensor_parallel"]["decode_seconds"]["median"]
        record["comparisons"] = {
            "one_rank_fastpath_vs_legacy_median_speedup": round(baseline / fast, 5),
            "two_rank_vs_one_rank_fastpath_median_speedup": round(fast / tp2, 5),
            "comparison_warning": ("Ratios of separately measured sample medians; "
                                   "no causal claim, error bar or WAN extrapolation."),
        }
        # Feed measured weight file sizes to a separate *analytical* screening
        # with explicitly hypothetical local and low-bandwidth network inputs.
        # Do not pass measured CI timings as if they were real WAN timings.
        layer_bytes = sum(
            p.stat().st_size for p in folders[2][0].glob("rank_*_layer_*.safetensors"))
        io_bytes = sum(
            p.stat().st_size for p in folders[2][0].glob("rank0_*.safetensors")
            if "_layer_" not in p.name)
        record["parallel_options_illustrative_only"] = screening(
            hidden=params["hidden"], heads=params["heads"],
            kv_heads=params["kv_heads"], layers=params["layers"], workers=2,
            transformer_weight_bytes=layer_bytes,
            io_weight_bytes=io_bytes,
            memory_per_node_bytes=512 * 1024 * 1024,
            latency_ms=10, bandwidth_mbps=100,
            prompt_tokens=len(prompt), new_tokens=max_new_tokens)
    if output is not None:
        dest = Path(output)
        if dest.exists() or dest.is_symlink():
            raise ValueError("will not overwrite an existing report")
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(json.dumps(record, ensure_ascii=False, indent=2),
                        encoding="utf-8")
    return record


def main():
    parser = argparse.ArgumentParser(description="Repeated Qwen2 1/2 rank Gloo A/B performance")
    parser.add_argument("--model-size", choices=list(SIZES), default="tiny")
    parser.add_argument("--dtype", choices=("float32", "bfloat16"), default="float32")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--tokens", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=100)
    parser.add_argument("--output", help="new, non-existing JSON report path")
    args = parser.parse_args()
    report = repeated_benchmark(
        model_size=args.model_size, storage_dtype=args.dtype,
        repeats=args.repeats, warmups=args.warmups, max_new_tokens=args.tokens,
        output=args.output, timeout=args.timeout)
    print(json.dumps({
        "schema": report["schema"], "workload": report["workload"],
        "summaries": {
            mode: {k: data[k] for k in ("decode_seconds", "tokens_per_second",
                                       "collective_fraction_max")}
            for mode, data in report["summaries"].items()},
        "comparisons": report["comparisons"],
        "warning": report["interpretation"],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
