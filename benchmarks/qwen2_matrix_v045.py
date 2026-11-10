"""No-download Qwen2 numerical + performance matrix for trusted localhost.

Each case uses deterministic *synthetic* Qwen2 weights, an independent
(single-process) unsliced reference and real torchrun Gloo ranks. This gives
code-correctness and communication-overhead evidence, NOT extrapolated WAN
performance or evidence that a pretrained DeepSeek model works.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

import torch

from compute.qwen2_chunked import all_files, export_chunked
from compute.qwen2_reference import generate_dense
from compute.qwen2_shards import synthetic_state
from compute.qwen2_validation import (
    summarize_profile, validate_generation_parity)


def parse_choices(text, *, allowed):
    try:
        values = [str(item).strip() for item in text.split(",")]
    except AttributeError as exc:
        raise ValueError("expected comma-separated options") from exc
    if not values or len(set(values)) != len(values) or any(
            item not in allowed for item in values):
        raise ValueError("invalid or repeated matrix option")
    return values


def launch_rank_group(folder, *, workers, max_new_tokens, prompt,
                      timeout=100):
    if not 1 <= workers <= 2 or not 1 <= max_new_tokens <= 8:
        raise ValueError("benchmark supports 1-2 ranks and 1-8 tokens")
    cmd = [sys.executable, "-m", "torch.distributed.run", "--standalone",
           f"--nproc-per-node={workers}", "-m", "compute.qwen2_generate",
           "--shards", str(Path(folder).resolve(strict=True)),
           "--prompt-ids", ",".join(str(i) for i in prompt),
           "--max-new-tokens", str(max_new_tokens),
           "--report-logits", "--profile"]
    started = time.perf_counter()
    proc = subprocess.run(
        cmd, capture_output=True, text=True, timeout=timeout,
        env={**os.environ, "OMP_NUM_THREADS": "1",
             "OPENCOMPUTE_EXTERNAL_STORE": "0"}, check=False)
    total = time.perf_counter() - started
    if proc.returncode:
        raise RuntimeError("Gloo rank-group failed: " + proc.stderr[-1500:])
    records = []
    for line in proc.stdout.splitlines():
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if (isinstance(record, dict) and
                str(record.get("engine", "")).startswith("opencompute-qwen2-")):
            records.append(record)
    if len(records) != 1 or records[0].get("workers") != workers:
        raise RuntimeError("did not receive one rank0 Gloo inference result")
    return records[0], total


def run_matrix(*, worker_counts=(1, 2), precisions=("float32", "bfloat16"),
               max_new_tokens=2, persist_to=None, timeout=100):
    counts = tuple(worker_counts)
    dtypes = tuple(precisions)
    if (not counts or not dtypes or any(type(i) is not int or i not in (1, 2)
                                        for i in counts) or len(set(counts)) != len(counts)
            or any(i not in ("float32", "bfloat16") for i in dtypes)
            or len(set(dtypes)) != len(dtypes)
            or type(max_new_tokens) is not int or not 1 <= max_new_tokens <= 8):
        raise ValueError("invalid matrix dimensions")
    torch.set_num_threads(1)
    state, cfg = synthetic_state(hidden=24, heads=6, kv_heads=2, layers=2,
                                 inner=48, vocab=64, positions=64, seed=4180)
    prompt = [1, 2, 3]
    baseline_fp32 = generate_dense(state, cfg, prompt, max_new_tokens)
    result = {
        "schema": "opencompute-qwen2-validation-matrix-v1",
        "model": "synthetic-qwen2-no-trained-language-capability",
        "execution": "trusted local torchrun CPU Gloo",
        "warning": ("CPU/synthetic results are not WAN predictions, "
                    "do not imply real DeepSeek inference or cross-device speedup."),
        "prompt_ids": prompt,
        "cases": [],
    }
    with tempfile.TemporaryDirectory(prefix="opencompute-qwen2-v045-") as root:
        for dtype in dtypes:
            rounded_state = (
                {key: value.bfloat16().float()
                 for key, value in state.items()}
                if dtype == "bfloat16" else state)
            quantized_reference = generate_dense(
                rounded_state, cfg, prompt, max_new_tokens)
            for world in counts:
                folder = Path(root) / f"ranks-{world}-{dtype}"
                metadata = export_chunked(state, cfg, folder, world,
                                          chunk_rows=11,
                                          storage_dtype=dtype,
                                          source="synthetic-v045-no-download")
                report, wall_seconds = launch_rank_group(
                    folder, workers=world,
                    max_new_tokens=max_new_tokens, prompt=prompt, timeout=timeout)
                parity = validate_generation_parity(
                    quantized_reference, report, atol=1e-5, rtol=1e-5,
                    reference_name=f"synthetic-{dtype}-rounded-dense",
                    candidate_name=f"{world}-rank-qwen2-v2-gloo")
                if not parity["passed"]:
                    raise RuntimeError(f"{dtype} {world} ranks failed strict parity")
                original_drift = validate_generation_parity(
                    baseline_fp32, report, atol=0.04, rtol=0.02,
                    reference_name="synthetic-fp32", candidate_name=f"{dtype}-{world}-rank")
                profile = summarize_profile(
                    report, layers=metadata["layers"],
                    prompt_length=len(prompt))
                bytes_used = sum(
                    (folder / name).stat().st_size for name in all_files(metadata)
                    if name != "manifest.json")
                result["cases"].append({
                    "workers": world,
                    "stored_dtype": dtype,
                    "output_format": metadata["format"],
                    "storage_file_count": len(all_files(metadata)),
                    "stored_weight_bytes": bytes_used,
                    "rank0_generated_ids": report["generated_ids"],
                    "rank0_decode_seconds": report["elapsed_seconds_rank0"],
                    "rank0_tokens_per_second": report["tokens_per_second_rank0"],
                    "end_to_end_process_launch_seconds": round(wall_seconds, 6),
                    "exact_rounded_dense_parity": parity,
                    "original_fp32_reference_drift": original_drift,
                    "profile": profile,
                })
    for dtype in dtypes:
        baseline = next((c for c in result["cases"]
                         if c["workers"] == 1 and c["stored_dtype"] == dtype), None)
        if baseline:
            for case in result["cases"]:
                if case["stored_dtype"] == dtype:
                    case["speedup_vs_same_dtype_one_rank"] = round(
                        baseline["rank0_decode_seconds"] /
                        max(1e-9, case["rank0_decode_seconds"]), 5)
                    case["efficiency_vs_same_dtype_one_rank"] = round(
                        case["speedup_vs_same_dtype_one_rank"] / case["workers"], 5)
    if persist_to is not None:
        path = Path(persist_to)
        if path.exists():
            raise ValueError("refusing to overwrite an existing benchmark report")
        if path.is_symlink():
            raise ValueError("benchmark report cannot be a symlink")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(result, ensure_ascii=False, indent=2),
                        encoding="utf-8")
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="No-download Qwen2 synthetic 1/2-rank FP32/BF16 correctness + Gloo timing")
    parser.add_argument("--workers", default="1,2")
    parser.add_argument("--precisions", default="float32,bfloat16")
    parser.add_argument("--new-tokens", type=int, default=2)
    parser.add_argument("--timeout", type=int, default=100)
    parser.add_argument("--output", help="optional new JSON report path")
    args = parser.parse_args(argv)
    count = tuple(int(v) for v in parse_choices(args.workers, allowed=("1", "2")))
    types = tuple(parse_choices(args.precisions, allowed=("float32", "bfloat16")))
    if not 5 <= args.timeout <= 600:
        parser.error("--timeout must be 5..600 seconds")
    report = run_matrix(worker_counts=count, precisions=types,
                        max_new_tokens=args.new_tokens,
                        timeout=args.timeout, persist_to=args.output)
    # Print compact report; detailed records are available via --output.
    print(json.dumps({
        "schema": report["schema"],
        "warning": report["warning"],
        "cases": [{
            key: c[key] for key in (
                "workers", "stored_dtype", "stored_weight_bytes",
                "rank0_decode_seconds", "rank0_tokens_per_second",
                "speedup_vs_same_dtype_one_rank")
            if key in c
        } | {"passed": c["exact_rounded_dense_parity"]["passed"],
             "collective_fraction_max": max(
                 rank["collective_fraction"] for rank in c["profile"]["ranks"])}
        for c in report["cases"]],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
