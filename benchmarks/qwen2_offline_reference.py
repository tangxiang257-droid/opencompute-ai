"""Opt-in, OFFLINE Qwen2/DeepSeek tokenizer and dense-reference check.

This never downloads weights, executes remote Python code, or runs untrusted
checkpoint formats. HF reference requires explicit --compare-hf and enough RAM
for a full FP32 model (often far more than 1.5B weight bytes alone).
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

import torch

from compute.qwen2_generate import read_manifest
from compute.qwen2_safetensors import inspect_directory


def local_token_ids(directory, text, *, chat=False):
    from transformers import AutoTokenizer
    directory = Path(directory).resolve(strict=True)
    tokenizer = AutoTokenizer.from_pretrained(
        str(directory), local_files_only=True, trust_remote_code=False,
        use_fast=True)
    if chat:
        if not tokenizer.chat_template:
            raise ValueError("trusted tokenizer has no chat template")
        tokens = tokenizer.apply_chat_template(
            [{"role": "user", "content": text}],
            add_generation_prompt=True, tokenize=True)
    else:
        tokens = tokenizer.encode(text, add_special_tokens=False)
    if isinstance(tokens, torch.Tensor):
        tokens = tokens.flatten().tolist()
    if (not isinstance(tokens, list) or not 1 <= len(tokens) <= 4096
            or any(type(v) is not int or v < 0 for v in tokens)):
        raise ValueError("invalid tokenizer output or excessive prompt length")
    return tokenizer, tokens


def run_tp(shards, prompt_ids, steps, workers, *, timeout=180):
    command = [
        sys.executable, "-m", "torch.distributed.run", "--standalone",
        f"--nproc-per-node={workers}", "-m", "compute.qwen2_generate",
        "--shards", str(shards), "--prompt-ids",
        ",".join(str(i) for i in prompt_ids),
        "--max-new-tokens", str(steps), "--report-logits",
    ]
    result = subprocess.run(
        command, text=True, capture_output=True, timeout=timeout,
        env={**os.environ, "OMP_NUM_THREADS": "1",
             "OPENCOMPUTE_EXTERNAL_STORE": "0"})
    if result.returncode:
        raise RuntimeError("TP inference failed: " + result.stderr[-2000:])
    reports = []
    for line in result.stdout.splitlines():
        if line.startswith('{"engine": "opencompute-qwen2'):
            reports.append(json.loads(line))
    if len(reports) != 1:
        raise RuntimeError("expected one Qwen2 rank-zero JSON report")
    return reports[0]


def compare_logits(tp, reference, *, atol=0.04, rtol=0.02):
    tp_scores = torch.as_tensor(tp["step_logits"], dtype=torch.float32)
    ref_scores = torch.as_tensor(reference["step_logits"], dtype=torch.float32)
    if tp_scores.shape != ref_scores.shape or tp_scores.ndim != 2:
        raise ValueError("logits shape mismatch between TP and reference")
    delta = (tp_scores - ref_scores).abs()
    max_delta = float(delta.max().item())
    same_tokens = tp["generated_ids"] == reference["generated_ids"]
    numeric_ok = torch.allclose(tp_scores, ref_scores, atol=atol, rtol=rtol)
    return {
        "pass": bool(same_tokens and numeric_ok),
        "same_greedy_tokens": bool(same_tokens),
        "logits_allclose": bool(numeric_ok),
        "max_abs_logit_error": max_delta,
        "atol": atol, "rtol": rtol,
    }


@torch.inference_mode()
def dense_hf_reference(model_dir, token_ids, steps):
    """Explicit opt-in; full CPU FP32 HF model can consume significant RAM."""
    from transformers import Qwen2ForCausalLM
    model = Qwen2ForCausalLM.from_pretrained(
        str(model_dir), local_files_only=True, trust_remote_code=False,
        use_safetensors=True, torch_dtype=torch.float32).eval().cpu()
    inputs = list(token_ids)
    generated, scores = [], []
    for _ in range(steps):
        # Full-prefix decode is slow but avoids KV implementation drift.
        tensor = torch.tensor([inputs], dtype=torch.long)
        logits = model(input_ids=tensor, use_cache=False).logits[0, -1, :]
        next_id = int(torch.argmax(logits).item())
        generated.append(next_id)
        scores.append(logits.detach().cpu())
        inputs.append(next_id)
    return {"generated_ids": generated, "step_logits": torch.stack(scores)}


def main():
    parser = argparse.ArgumentParser(
        description="Trusted local DeepSeek/Qwen2 tokenizer and optional HF parity")
    parser.add_argument("--model-dir", required=True,
                        help="trusted local config, tokenizer and Safetensors weights")
    parser.add_argument("--shards", help="trusted already exported Qwen2 TP shards")
    parser.add_argument("--text", required=True, help="short user prompt to tokenize")
    parser.add_argument("--chat", action="store_true")
    parser.add_argument("--tokenize-only", action="store_true")
    parser.add_argument("--compare-hf", action="store_true",
                        help="opt into large CPU RAM use for dense FP32 reference")
    parser.add_argument("--max-new-tokens", type=int, default=2)
    parser.add_argument("--atol", type=float, default=0.04)
    parser.add_argument("--rtol", type=float, default=0.02)
    args = parser.parse_args()
    if not 1 <= args.max_new_tokens <= 16:
        parser.error("limit reference verification to 1..16 new tokens")
    if not 0 <= args.atol <= 1 or not 0 <= args.rtol <= 1:
        parser.error("numerical tolerances must be nonnegative and at most one")
    if args.tokenize_only and args.compare_hf:
        parser.error("--compare-hf requires actual TP inference")
    if not args.tokenize_only and not args.shards:
        parser.error("--shards is mandatory unless --tokenize-only is set")
    root = Path(args.model_dir).resolve(strict=True)
    # Real-weight source shape and filename checks before any optional HF load.
    if not args.tokenize_only:
        config = read_manifest(args.shards)
        inspect_directory(root, config["workers"])
    tokenizer, tokens = local_token_ids(root, args.text, chat=args.chat)
    output = {"prompt_ids": tokens, "chat_template": bool(args.chat)}
    if not args.tokenize_only:
        manifest = read_manifest(args.shards)
        if (len(tokens) + args.max_new_tokens - 1 > manifest["positions"]
                or any(token >= manifest["vocab"] for token in tokens)):
            raise ValueError("prompt exceeds local shard token/context limits")
        tp = run_tp(Path(args.shards), tokens, args.max_new_tokens,
                    manifest["workers"])
        output["tp"] = {
            "generated_ids": tp["generated_ids"],
            "decoded_output": tokenizer.decode(tp["generated_ids"]),
            "engine": tp["engine"],
            "weight_bytes_per_rank": tp["weight_bytes_per_rank"],
        }
        if args.compare_hf:
            reference = dense_hf_reference(root, tokens, args.max_new_tokens)
            output["parity"] = compare_logits(tp, reference,
                                              atol=args.atol, rtol=args.rtol)
            if not output["parity"]["pass"]:
                print(json.dumps(output, ensure_ascii=False, indent=2))
                raise SystemExit("dense reference does not match TP output")
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
