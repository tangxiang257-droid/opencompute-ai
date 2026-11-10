"""Explicit pass/fail criteria for Qwen2 full-vocabulary per-token parity.

Never infer correctness from plausible text, loss alone, or aggregate logits
alone. Each greedy token AND each step's full logits must meet the policy.
Synthetic-reference parity is not evidence of real Hugging Face parity.
"""
import math
import torch


def _checked_logits(report, label):
    if not isinstance(report, dict) or not isinstance(report.get("generated_ids"), list):
        raise ValueError(f"{label}: missing generated token IDs")
    tokens = report["generated_ids"]
    if not 1 <= len(tokens) <= 16 or any(
            type(v) is not int or v < 0 for v in tokens):
        raise ValueError(f"{label}: invalid generated tokens")
    try:
        logits = torch.as_tensor(report["step_logits"], dtype=torch.float64)
    except (KeyError, ValueError, TypeError, RuntimeError) as exc:
        raise ValueError(f"{label}: missing/malformed per-step logits") from exc
    if (logits.ndim != 2 or logits.shape[0] != len(tokens)
            or not 2 <= logits.shape[1] <= 262144
            or logits.numel() > 4194304 or not torch.isfinite(logits).all().item()):
        raise ValueError(f"{label}: incompatible or non-finite logits")
    decoded = logits.argmax(dim=1).tolist()
    if decoded != tokens:
        raise ValueError(f"{label}: reported tokens disagree with recorded logits")
    return tokens, logits


def validate_generation_parity(reference, candidate, *, atol=1e-5, rtol=1e-5,
                               reference_name="dense-synthetic",
                               candidate_name="qwen2-gloo"):
    for name, limit in (("atol", atol), ("rtol", rtol)):
        if type(limit) not in (int, float) or not math.isfinite(limit) or limit < 0:
            raise ValueError(f"{name}: expected nonnegative finite number")
    rtokens, logits = _checked_logits(reference, reference_name)
    ctokens, other = _checked_logits(candidate, candidate_name)
    if logits.shape != other.shape:
        raise ValueError("candidate and reference steps/vocabulary do not match")
    step_metrics = []
    all_close = True
    for i in range(len(rtokens)):
        x, y = logits[i], other[i]
        diff = (x - y).abs()
        close = bool(torch.allclose(x, y, atol=atol, rtol=rtol))
        all_close = all_close and close
        margin = float((torch.topk(x, 2).values[0] -
                        torch.topk(x, 2).values[1]).item())
        max_diff = float(diff.max().item())
        denom = x.abs().clamp_min(1e-9)
        step_metrics.append({
            "step": i,
            "reference_token": rtokens[i],
            "candidate_token": ctokens[i],
            "token_match": rtokens[i] == ctokens[i],
            "logits_within_tolerance": close,
            "max_abs_delta": max_diff,
            "mean_abs_delta": float(diff.mean().item()),
            "rms_delta": float(torch.sqrt((diff * diff).mean()).item()),
            "max_rel_delta": float((diff / denom).max().item()),
            "reference_top2_margin": margin,
            "near_tie_relative_to_error": margin <= 2 * max_diff,
        })
    tokens_match = rtokens == ctokens
    return {
        "passed": tokens_match and all_close,
        "reference": reference_name,
        "candidate": candidate_name,
        "tokens_identical": tokens_match,
        "all_step_logits_within_tolerance": all_close,
        "steps": len(rtokens),
        "vocab_size": logits.shape[1],
        "max_abs_delta": max(x["max_abs_delta"] for x in step_metrics),
        "tolerances": {"atol": float(atol), "rtol": float(rtol)},
        "step_metrics": step_metrics,
        "note": "Synthetic tests check sharding; official weights require independent HF reference.",
    }


def summarize_profile(output, *, layers, prompt_length):
    """Fail closed on malformed optional Qwen2 profiling counters."""
    if not isinstance(output, dict) or not isinstance(output.get("profile"), dict):
        raise ValueError("profile was not captured")
    prof = output["profile"]
    world = output.get("workers")
    tokens = len(output.get("generated_ids", []))
    ranks = prof.get("ranks")
    if (type(world) is not int or world < 1 or type(layers) is not int
            or layers < 1 or type(prompt_length) is not int or prompt_length < 1
            or not 1 <= tokens <= 16 or not isinstance(ranks, list)
            or len(ranks) != world):
        raise ValueError("inconsistent profile topology or prompt")
    expected_reduce_calls = tokens * 2 * layers
    expected_broadcast_calls = tokens
    totals = []
    for record in ranks:
        if not isinstance(record, dict):
            raise ValueError("missing rank statistics")
        for name in ("decode_us", "collective_wall_us", "preflight_us",
                     "all_reduce_calls", "all_reduce_tensor_bytes",
                     "broadcast_calls", "broadcast_tensor_bytes",
                     "rss_sample_bytes"):
            value = record.get(name)
            if type(value) is not int or value < 0:
                raise ValueError(f"bad profiler counter: {name}")
        if (record["all_reduce_calls"] != expected_reduce_calls
                or record["broadcast_calls"] != expected_broadcast_calls
                or record["decode_us"] <= 0
                or record["collective_wall_us"] > record["decode_us"]):
            raise ValueError("profiler collective count/timing mismatch")
        totals.append({
            "decode_seconds": round(record["decode_us"] / 1e6, 6),
            "preflight_seconds": round(record["preflight_us"] / 1e6, 6),
            "collective_seconds": round(record["collective_wall_us"] / 1e6, 6),
            "collective_fraction": round(record["collective_wall_us"] /
                                         record["decode_us"], 6),
            "all_reduce_calls": record["all_reduce_calls"],
            "broadcast_calls": record["broadcast_calls"],
            "all_reduce_tensor_bytes": record["all_reduce_tensor_bytes"],
            "broadcast_tensor_bytes": record["broadcast_tensor_bytes"],
            "rss_sample_bytes": record["rss_sample_bytes"],
        })
    return {
        "ranks": totals,
        "workers": world, "steps": tokens,
        "slowest_rank_decode_seconds": max(i["decode_seconds"] for i in totals),
        "slowest_rank_collective_seconds": max(
            i["collective_seconds"] for i in totals),
        "scope": "Measured localhost Gloo wall times; tensor bytes are NOT wire bytes.",
        "memory_limit": "RSS is one end-of-run sample, not process-tree peak memory.",
    }
