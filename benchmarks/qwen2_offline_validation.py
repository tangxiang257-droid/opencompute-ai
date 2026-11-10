"""Offline compatibility check: trusted local Qwen2 tokenizer + optional HF/TP parity.

This utility NEVER downloads a model or runs remote Python modules. It can
inspect tokenizer IDs cheaply; an explicit --compare-run enables the costly
CPU Hugging Face reference and localhost torchrun on already present weights.

For research/debugging only. Hugging Face reference model loading can require
many GB RAM and is intentionally disabled by default.
"""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import sys

import torch


def trusted_path(path):
    root = Path(path).expanduser().resolve(strict=True)
    if not root.is_dir() or not (root / "config.json").is_file():
        raise ValueError("expected an existing local model folder with config.json")
    return root


def local_tokenizer_ids(model_dir, text, *, chat=False):
    if not isinstance(text, str) or not text or len(text) > 4096:
        raise ValueError("prompt must have 1..4096 characters")
    try:
        from transformers import AutoTokenizer
    except ImportError as exc:
        raise RuntimeError(
            "install optional transformers for trusted local tokenizer checks") from exc
    tokenizer = AutoTokenizer.from_pretrained(
        str(trusted_path(model_dir)), local_files_only=True,
        trust_remote_code=False, use_fast=True)
    if chat:
        if not getattr(tokenizer, "chat_template", None):
            raise ValueError("this local tokenizer has no chat template")
        ids = tokenizer.apply_chat_template(
            [{"role": "user", "content": text}],
            add_generation_prompt=True, tokenize=True)
        if isinstance(ids, torch.Tensor):
            ids = ids.flatten().tolist()
    else:
        ids = tokenizer.encode(text, add_special_tokens=False)
    if not isinstance(ids, list) or not ids or len(ids) > 128:
        raise ValueError("tokenized prompt must contain 1..128 IDs")
    if any(type(v) is not int or v < 0 for v in ids):
        raise ValueError("tokenizer produced invalid IDs")
    return ids


def sha256_regular_file(path, *, max_bytes):
    """Local bounded read; SHA-256 pins bytes, not model authorship."""
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        raise ValueError("refusing missing/symlinked fingerprint source")
    size = path.stat().st_size
    if type(max_bytes) is not int or not 0 < size <= max_bytes:
        raise ValueError("source size exceeds fingerprint envelope")
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    if path.stat().st_size != size:
        raise ValueError("source changed during fingerprinting")
    return {"size": size, "sha256": digest.hexdigest()}


def compare_logits(reference, distributed, *, atol=0.005, rtol=0.001):
    """Compare tokens and *per-step* logits without hiding any bad step."""
    if (not isinstance(reference, dict) or not isinstance(distributed, dict)
            or not isinstance(reference.get("generated_ids"), list)
            or not isinstance(distributed.get("generated_ids"), list)):
        raise ValueError("missing generated token IDs")
    ref_logits = reference.get("step_logits")
    tp_logits = distributed.get("step_logits")
    if not isinstance(ref_logits, list) or not isinstance(tp_logits, list):
        raise ValueError("both inference runs must report step logits")
    if (len(ref_logits) != len(tp_logits)
            or len(ref_logits) != len(reference["generated_ids"])
            or not ref_logits):
        raise ValueError("missing or unequal number of inference steps")
    if (not all(isinstance(row, list) and row for row in ref_logits)
            or not all(isinstance(row, list) and row for row in tp_logits)):
        raise ValueError("malformed logit rows")
    if type(atol) not in (int, float) or not math.isfinite(atol) or atol < 0:
        raise ValueError("invalid absolute tolerance")
    if type(rtol) not in (int, float) or not math.isfinite(rtol) or rtol < 0:
        raise ValueError("invalid relative tolerance")
    max_abs = 0.0
    max_rel = 0.0
    values_match = True
    for a, b in zip(ref_logits, tp_logits):
        if len(a) != len(b):
            raise ValueError("vocabulary size mismatch")
        x = torch.tensor(a, dtype=torch.float64)
        y = torch.tensor(b, dtype=torch.float64)
        if not torch.isfinite(x).all() or not torch.isfinite(y).all():
            raise ValueError("non-finite reference/distributed logits")
        difference = (x - y).abs()
        max_abs = max(max_abs, float(difference.max().item()))
        max_rel = max(max_rel, float(
            (difference / x.abs().clamp_min(1e-12)).max().item()))
        values_match = values_match and bool(
            torch.allclose(x, y, atol=atol, rtol=rtol))
    tokens_match = reference["generated_ids"] == distributed["generated_ids"]
    return {
        "token_ids_match": tokens_match,
        "logits_within_tolerance": values_match,
        "passed": tokens_match and values_match,
        "max_abs_logit_delta": max_abs,
        "max_rel_logit_delta": max_rel,
        "steps": len(ref_logits),
        "atol": atol,
        "rtol": rtol,
    }


@torch.inference_mode()
def hf_reference(model_dir, ids, max_new_tokens):
    """Load ONLY locally installed Safetensors, using stock Transformers code."""
    try:
        from transformers import AutoModelForCausalLM
    except ImportError as exc:
        raise RuntimeError(
            "install optional transformers for offline HF reference") from exc
    model = AutoModelForCausalLM.from_pretrained(
        str(trusted_path(model_dir)), local_files_only=True,
        trust_remote_code=False, use_safetensors=True,
        torch_dtype=torch.float32)
    if getattr(model.config, "model_type", None) != "qwen2":
        raise ValueError("reference model is not a stock Qwen2 checkpoint")
    model.eval()
    current = torch.tensor([ids], dtype=torch.long)
    cache = None
    outputs = {"generated_ids": [], "step_logits": []}
    for _ in range(max_new_tokens):
        result = model(input_ids=current, past_key_values=cache,
                       use_cache=True)
        cache = result.past_key_values
        logits = result.logits[0, -1, :].float()
        token = int(logits.argmax().item())
        outputs["generated_ids"].append(token)
        outputs["step_logits"].append(logits.tolist())
        current = torch.tensor([[token]], dtype=torch.long)
    return outputs


def local_tp(shards, ids, max_new_tokens, workers, *, timeout=240):
    if type(workers) is not int or not 1 <= workers <= 4:
        raise ValueError("invalid number of TP ranks")
    if type(max_new_tokens) is not int or not 1 <= max_new_tokens <= 4:
        raise ValueError("verification allows 1..4 output tokens")
    root = Path(shards).resolve(strict=True)
    if not root.is_dir():
        raise ValueError("TP shard path is not a directory")
    argv = [sys.executable, "-m", "torch.distributed.run",
            "--standalone", f"--nproc-per-node={workers}",
            "-m", "compute.qwen2_generate", "--shards", str(root),
            "--prompt-ids", ",".join(str(i) for i in ids),
            "--max-new-tokens", str(max_new_tokens), "--report-logits"]
    env = {**os.environ, "OMP_NUM_THREADS": "1",
           "OPENCOMPUTE_EXTERNAL_STORE": "0"}
    result = subprocess.run(argv, capture_output=True, text=True,
                            check=False, timeout=timeout, env=env)
    if result.returncode:
        raise RuntimeError(
            "trusted localhost TP reference failed: " + result.stderr[-1000:])
    matches = []
    for line in result.stdout.splitlines():
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict) and record.get("engine", "").startswith(
                "opencompute-qwen2-"):
            matches.append(record)
    if len(matches) != 1:
        raise RuntimeError("expected exactly one rank0 Qwen2 output")
    return matches[0]


def run(model_dir, shards, prompt, *, max_new_tokens=2,
        workers=2, compare_run=False, atol=0.005, rtol=0.001,
        chat=False, fingerprint_weights=False):
    from compute.qwen2_safetensors import inspect_directory
    from compute.qwen2_generate import read_manifest
    if type(max_new_tokens) is not int or not 1 <= max_new_tokens <= 4:
        raise ValueError("verification allows 1..4 output tokens")
    original = inspect_directory(trusted_path(model_dir), workers)
    meta = read_manifest(shards)
    for key in ("format", "hidden", "heads", "kv_heads", "layers", "inner",
                "vocab", "positions", "head_dim", "workers", "rms_eps",
                "rope_theta", "tied_embeddings"):
        if key == "format":
            # The model source is the unsharded config; v1/v2 TP formats differ.
            continue
        if meta[key] != original["manifest"][key]:
            raise ValueError(f"source and shards mismatch on {key}")
    ids = local_tokenizer_ids(model_dir, prompt, chat=chat)
    if any(i >= meta["vocab"] for i in ids):
        raise ValueError("tokenizer produced out-of-vocabulary token")
    if len(ids) + max_new_tokens - 1 > meta["positions"]:
        raise ValueError("prompt exceeds model context length")
    summary = {
        "mode": "offline-preflight" if not compare_run else "offline-hf-tp-comparison",
        "reference_source": str(trusted_path(model_dir)),
        "shards": str(Path(shards).resolve(strict=True)),
        "workers": workers, "model_architecture": "qwen2",
        "prompt_ids": ids, "weight_dtype": meta.get("weight_dtype", "float32"),
        "chat_template_applied": bool(chat),
        "real_reference_executed": False,
        "provenance_note": ("SHA-256 verifies file bytes, not origin, authorship "
                            "or that exported tensors match the originals."),
        "config_fingerprint": sha256_regular_file(
            trusted_path(model_dir) / "config.json", max_bytes=32768),
        "export_manifest_fingerprint": sha256_regular_file(
            Path(shards) / "manifest.json", max_bytes=16384),
        "source_weight_fingerprints": None,
    }
    if fingerprint_weights:
        from compute.qwen2_safetensors import MAX_WEIGHT_FILE_BYTES
        folder = trusted_path(model_dir)
        summary["source_weight_fingerprints"] = {
            name: sha256_regular_file(folder / name,
                                     max_bytes=MAX_WEIGHT_FILE_BYTES)
            for name in original["input_files"]
        }
    if compare_run:
        # Explicitly opt-in; may require >8 GB RSS with 1.5B-class models.
        reference = hf_reference(model_dir, ids, max_new_tokens)
        distributed = local_tp(shards, ids, max_new_tokens, workers)
        summary["real_reference_executed"] = True
        summary["metrics"] = compare_logits(
            reference, distributed, atol=atol, rtol=rtol)
        summary["reference_generated_ids"] = reference["generated_ids"]
        summary["distributed_generated_ids"] = distributed["generated_ids"]
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="No-download trusted Qwen2 tokenizer / HF-versus-Gloo validation")
    parser.add_argument("--model-dir", required=True)
    parser.add_argument("--tp-shards", required=True)
    parser.add_argument("--prompt", required=True)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--max-new-tokens", type=int, default=2)
    parser.add_argument("--compare-run", action="store_true",
                        help="explicitly load the full trusted HF model and run local TP")
    parser.add_argument("--chat", action="store_true",
                        help="use the trusted local tokenizer chat template")
    parser.add_argument("--fingerprint-weights", action="store_true",
                        help="stream SHA-256 over every local Safetensors source (can be slow)")
    parser.add_argument("--atol", type=float, default=0.005)
    parser.add_argument("--rtol", type=float, default=0.001)
    options = parser.parse_args(argv)
    report = run(options.model_dir, options.tp_shards, options.prompt,
                 workers=options.workers,
                 max_new_tokens=options.max_new_tokens,
                 compare_run=options.compare_run,
                 atol=options.atol, rtol=options.rtol,
                 chat=options.chat,
                 fingerprint_weights=options.fingerprint_weights)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if report.get("metrics", {"passed": True})["passed"] is False:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
