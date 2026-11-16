"""Offline Qwen2 v0.4.8 batched 2-stage PP versus sequential per-request PP.

All variants run the same two Gloo ranks, weights, request prompts, token
count and isolated KV caches. The only changed factor is whether stage0 can
queue nonblocking P2P while preparing other requests. Repeat per-mode and
rotate order; results do not represent cross-machine or serving throughput.
"""
import argparse
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import tempfile
import time

import torch

from compute.qwen2_chunked import export_chunked
from compute.qwen2_pipeline_v047 import partition_one_rank_v2
from compute.qwen2_reference import generate_dense
from compute.qwen2_shards import synthetic_state
from compute.qwen2_validation import validate_generation_parity


PROMPTS = [
    {"id": "alpha", "prompt_ids": [1, 2, 3]},
    {"id": "beta", "prompt_ids": [11, 4]},
    {"id": "gamma", "prompt_ids": [6, 7, 8, 9]},
    {"id": "delta", "prompt_ids": [15, 2, 17]},
]


def launch_group(bundle, requests_path, *, mode, max_new_tokens, timeout=90,
                 max_kv_mib=64):
    if mode not in ("sequential", "overlap"):
        raise ValueError("unsupported benchmark execution mode")
    argv = [
        sys.executable, "-m", "torch.distributed.run",
        "--standalone", "--nproc-per-node=2",
        "-m", "compute.qwen2_pipeline_batch_v048",
        "--bundle", str(Path(bundle).resolve(strict=True)),
        "--requests-json", str(Path(requests_path).resolve(strict=True)),
        "--mode", mode,
        "--max-new-tokens", str(max_new_tokens),
        "--max-kv-mib", str(max_kv_mib),
        "--report-logits",
    ]
    started = time.perf_counter()
    proc = subprocess.run(
        argv, capture_output=True, text=True,
        env={**os.environ, "OMP_NUM_THREADS": "1",
             "OPENCOMPUTE_EXTERNAL_STORE": "0"},
        timeout=timeout, check=False)
    elapsed = time.perf_counter() - started
    if proc.returncode:
        raise RuntimeError(
            "two-stage Qwen2 batched inference failed: " + proc.stderr[-2800:])
    results = []
    for line in proc.stdout.splitlines():
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if (isinstance(record, dict) and record.get("engine") ==
                "opencompute-qwen2-pipeline-batched-v0.4.8"):
            results.append(record)
    if len(results) != 1:
        raise RuntimeError("expected exactly one rank0 batched PP record")
    return results[0], elapsed


def run_comparison(*, request_count=3, repeats=2,
                   dtype="float32", new_tokens=3,
                   max_kv_mib=64, timeout=90, output=None):
    if (type(request_count) is not int or not 2 <= request_count <= 4
            or type(repeats) is not int or not 2 <= repeats <= 6
            or dtype not in ("float32", "bfloat16")
            or type(new_tokens) is not int or not 1 <= new_tokens <= 8
            or type(max_kv_mib) is not int or not 1 <= max_kv_mib <= 4096):
        raise ValueError("invalid multi-request benchmark dimensions")
    if output is not None and (Path(output).exists() or Path(output).is_symlink()):
        raise ValueError("benchmark report path already exists")
    torch.set_num_threads(1)
    state, config = synthetic_state(
        hidden=24, heads=6, kv_heads=2, layers=4, inner=48,
        vocab=64, positions=64, seed=4180)
    rounded = ({key: value.bfloat16().float() for key, value in state.items()}
               if dtype == "bfloat16" else state)
    requests = PROMPTS[:request_count]
    baseline = {
        req["id"]: generate_dense(
            rounded, config, req["prompt_ids"], new_tokens)
        for req in requests
    }
    report = {
        "schema": "opencompute-qwen2-multi-request-v048",
        "model": "synthetic-random-no-pretrained-model",
        "request_count": request_count, "requests": requests,
        "storage_precision": dtype, "new_tokens_per_request": new_tokens,
        "repeats_per_mode": repeats,
        "scope": ("single trusted Linux host with two Gloo CPU ranks; "
                  "no dynamic admissions, WAN, pretrained weights or GPUs"),
        "trials": {"sequential": [], "overlap": []},
    }
    with tempfile.TemporaryDirectory(prefix="oc-qwen2-v048-") as base:
        base = Path(base)
        src, bundle = base / "tp1", base / "pp"
        export_chunked(state, config, src, 1, chunk_rows=11,
                       storage_dtype=dtype, source="synthetic-batched-v048")
        partition_one_rank_v2(src, bundle)
        prompts = base / "requests.json"
        prompts.write_text(
            json.dumps({"requests": requests}), encoding="utf-8")
        # Alternate order to reduce one-sided host cache and CI CPU drift.
        for trial in range(repeats):
            modes = ("sequential", "overlap") if trial % 2 == 0 else (
                "overlap", "sequential")
            for mode in modes:
                result, process_seconds = launch_group(
                    bundle, prompts, mode=mode, max_new_tokens=new_tokens,
                    max_kv_mib=max_kv_mib, timeout=timeout)
                if result["request_count"] != request_count:
                    raise RuntimeError("Gloo request count mismatch")
                for actual in result["results"]:
                    reference = baseline.get(actual["id"])
                    if reference is None:
                        raise RuntimeError("unknown result/request identifier")
                    parity = validate_generation_parity(
                        reference, actual, atol=1e-5, rtol=1e-5,
                        reference_name="synthetic-dense",
                        candidate_name=f"pp-{mode}-{actual['id']}")
                    if not parity["passed"]:
                        raise RuntimeError(
                            f"independent request output mismatch: {mode}/{actual['id']}")
                if len(result["results"]) != request_count:
                    raise RuntimeError("missing or repeated batched PP result")
                if any(len(item["generated_ids"]) != new_tokens
                       for item in result["results"]):
                    raise RuntimeError("one request failed to complete")
                report["trials"][mode].append({
                    "trial": trial,
                    "decode_seconds": result["elapsed_seconds_rank0"],
                    "tokens_per_second": result["total_tokens_per_second_rank0"],
                    "whole_torchrun_seconds": round(process_seconds, 6),
                    "rank_profiles": result["rank_profiles"],
                    "peak_inflight_rank0": result["peak_inflight_rank0"],
                    "scheduling_opportunities_rank0":
                        result["scheduling_opportunities_rank0"],
                    "generated_by_id": {item["id"]: item["generated_ids"]
                                        for item in result["results"]},
                    "all_requests_parity_passed": True,
                })
    medians = {
        mode: {
            "median_decode_seconds": round(statistics.median(
                sample["decode_seconds"] for sample in entries), 7),
            "median_total_tokens_per_second": round(statistics.median(
                sample["tokens_per_second"] for sample in entries), 3),
            "min_decode_seconds": min(sample["decode_seconds"] for sample in entries),
            "max_decode_seconds": max(sample["decode_seconds"] for sample in entries),
        }
        for mode, entries in report["trials"].items()
    }
    baseline_time = medians["sequential"]["median_decode_seconds"]
    async_time = medians["overlap"]["median_decode_seconds"]
    report["summary"] = {
        **medians,
        "overlap_vs_sequential_median_speedup": round(
            baseline_time / async_time, 5),
        "warning": ("Only 2..6 runs/mode. Nonblocking P2P creates overlap "
                    "opportunities; do not assume physical overlap, stable "
                    "speedup or improved latency for individual requests."),
    }
    if output is not None:
        p = Path(output)
        if p.exists() or p.is_symlink():
            raise ValueError("refusing to overwrite an existing report")
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(report, ensure_ascii=False, indent=2),
                     encoding="utf-8")
    return report


def main():
    parser = argparse.ArgumentParser(description="v0.4.8 offline multi-request PP comparison")
    parser.add_argument("--requests", type=int, default=3)
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--dtype", choices=("float32", "bfloat16"),
                        default="float32")
    parser.add_argument("--new-tokens", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=90)
    parser.add_argument("--max-kv-mib", type=int, default=64)
    parser.add_argument("--output", help="new optional JSON performance report")
    a = parser.parse_args()
    if not 5 <= a.timeout <= 300:
        parser.error("--timeout must be 5..300")
    result = run_comparison(
        request_count=a.requests, repeats=a.repeats, dtype=a.dtype,
        new_tokens=a.new_tokens, timeout=a.timeout,
        max_kv_mib=a.max_kv_mib, output=a.output)
    print(json.dumps({
        "schema": result["schema"],
        "request_count": result["request_count"],
        "storage_precision": result["storage_precision"],
        "summary": result["summary"],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
