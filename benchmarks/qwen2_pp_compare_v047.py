"""No-download Qwen2 TP1/TP2 versus genuine PP2 correctness + timing.

All cases use identical deterministic random weights, the same token prompt,
the same number of layers, and the same FP32/BF16 disk dtype. This benchmark
is a single localhost sample per case, NOT evidence of WAN speedup.
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

from benchmarks.qwen2_matrix_v045 import launch_rank_group
from compute.qwen2_chunked import export_chunked
from compute.qwen2_pipeline_v047 import partition_one_rank_v2
from compute.qwen2_reference import generate_dense
from compute.qwen2_shards import synthetic_state
from compute.qwen2_validation import validate_generation_parity


def launch_pp(bundle, prompt, max_new_tokens, timeout=90):
    cmd = [
        sys.executable, "-m", "torch.distributed.run",
        "--standalone", "--nproc-per-node=2",
        "-m", "compute.qwen2_pipeline_v047",
        "--bundle", str(Path(bundle).resolve(strict=True)),
        "--prompt-ids", ",".join(map(str, prompt)),
        "--max-new-tokens", str(max_new_tokens),
        "--report-logits",
    ]
    start = time.perf_counter()
    result = subprocess.run(
        cmd, capture_output=True, text=True, timeout=timeout,
        env={**os.environ, "OPENCOMPUTE_EXTERNAL_STORE": "0",
             "OMP_NUM_THREADS": "1"},
        check=False)
    seconds = time.perf_counter() - start
    if result.returncode:
        raise RuntimeError(
            "two-stage Gloo pipeline failed: " + result.stderr[-2200:])
    matches = []
    for line in result.stdout.splitlines():
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if (isinstance(item, dict) and item.get("engine") ==
                "opencompute-qwen2-pipeline-v0.4.7"):
            matches.append(item)
    if len(matches) != 1:
        raise RuntimeError("expected exactly one rank0 PP inference record")
    return matches[0], seconds


def benchmark(*, dtype="float32", new_tokens=3, timeout=90):
    if dtype not in ("float32", "bfloat16"):
        raise ValueError("unsupported storage dtype")
    if type(new_tokens) is not int or not 1 <= new_tokens <= 8:
        raise ValueError("invalid output length")
    torch.set_num_threads(1)
    state, config = synthetic_state(
        hidden=24, heads=6, kv_heads=2, layers=4, inner=48,
        vocab=64, positions=64, seed=4180)
    ref_state = ({k: v.bfloat16().float() for k, v in state.items()}
                 if dtype == "bfloat16" else state)
    prompt = [1, 2, 3]
    expected = generate_dense(ref_state, config, prompt, new_tokens)
    cases = []
    with tempfile.TemporaryDirectory(prefix="opencompute-qwen2-pp-v047-") as tmp:
        root = Path(tmp)
        full = root / "tp1"
        tp2 = root / "tp2"
        pp2 = root / "pp2"
        export_chunked(state, config, full, workers=1, chunk_rows=11,
                       storage_dtype=dtype, source="synthetic-v047")
        export_chunked(state, config, tp2, workers=2, chunk_rows=11,
                       storage_dtype=dtype, source="synthetic-v047")
        partition_one_rank_v2(full, pp2)
        for mode in ("tp1", "tp2", "pp2"):
            if mode == "pp2":
                record, whole = launch_pp(pp2, prompt, new_tokens, timeout)
            else:
                record, whole = launch_rank_group(
                    full if mode == "tp1" else tp2, workers=1 if mode == "tp1" else 2,
                    max_new_tokens=new_tokens, prompt=prompt, timeout=timeout)
            parity = validate_generation_parity(
                expected, record, atol=1e-5, rtol=1e-5,
                reference_name=f"synthetic-dense-{dtype}",
                candidate_name=mode)
            if not parity["passed"]:
                raise RuntimeError(f"{mode} does not match dense reference")
            case = {
                "mode": mode, "workers": 1 if mode == "tp1" else 2,
                "output_ids": record["generated_ids"],
                "decode_seconds_rank0": record["elapsed_seconds_rank0"],
                "tokens_per_second_rank0": record["tokens_per_second_rank0"],
                "whole_process_seconds": round(whole, 6),
                "parity_passed": parity["passed"],
                "max_abs_logit_delta": parity["max_abs_delta"],
            }
            if mode == "pp2":
                case["rank_profiles"] = record["rank_profiles"]
                case["stages"] = record["stages"]
            else:
                case["rank_profiles"] = record["profile"]["ranks"]
            cases.append(case)
    base = next(c for c in cases if c["mode"] == "tp1")
    for case in cases:
        case["speedup_vs_tp1"] = round(
            base["decode_seconds_rank0"] / case["decode_seconds_rank0"], 5)
    return {
        "schema": "opencompute-qwen2-pp-v047-comparison-v1",
        "model": "synthetic-qwen2-no-pretrained-language",
        "storage_dtype": dtype,
        "prompt_ids": prompt, "generated_tokens": new_tokens,
        "cases": cases,
        "limitations": (
            "One measurement per mode, trusted localhost CPU/Gloo. PP sends "
            "activations stage0->stage1->stage0 for every step. No overlapping "
            "request batches, no WAN testing, no DeepSeek pretrained model."),
    }


def main():
    parser = argparse.ArgumentParser(description="Compare actual Qwen2 TP1/TP2 and PP2")
    parser.add_argument("--dtype", choices=("float32", "bfloat16"),
                        default="float32")
    parser.add_argument("--new-tokens", type=int, default=3)
    parser.add_argument("--output", help="new JSON report; never overwrite")
    args = parser.parse_args()
    if args.output and (Path(args.output).is_symlink() or Path(args.output).exists()):
        parser.error("refusing to overwrite existing output")
    result = benchmark(dtype=args.dtype, new_tokens=args.new_tokens)
    if args.output:
        path = Path(args.output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(result, ensure_ascii=False, indent=2),
                        encoding="utf-8")
    print(json.dumps({
        "schema": result["schema"],
        "storage_dtype": result["storage_dtype"],
        "cases": [{
            k: c[k] for k in ("mode", "workers", "decode_seconds_rank0",
                             "tokens_per_second_rank0", "whole_process_seconds",
                             "speedup_vs_tp1", "parity_passed")
        } for c in result["cases"]],
        "limitations": result["limitations"],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
