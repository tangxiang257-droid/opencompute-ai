"""Qwen2 v0.4.8: bounded two-stage, multi-request Gloo pipeline.

Uses the trusted v0.4.7 stage bundle. Distinct requests retain distinct
per-stage KV caches. Nonblocking Gloo P2P permits stage 0 to process request B
while stage 1 works on request A; this is an *opportunity* for overlap, NOT a
guarantee of parallel CPU utilization or improved throughput.

Research only: 2 local trusted ranks, fixed 1..4 active requests, one global
generation length, no dynamic arrival/cancellation, no remote Worker support.
"""
import argparse
import json
import os
from pathlib import Path
import re
import sys
import time

import torch
import torch.distributed as dist

from compute.qwen2_core import forward_layers, rms_norm
from compute.qwen2_pipeline_v047 import (
    _load_stage, _peak_rss_linux, inspect_pipeline)
from compute.rendezvous import init_gloo


MAX_REQUESTS = 4
MAX_NEW_TOKENS = 8
MAX_PROMPTS_FILE_BYTES = 8192
REQUEST_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,31}\Z")
PROFILE_FIELDS = (
    "wall_us", "compute_us", "p2p_wait_us", "send_calls", "recv_calls",
    "send_tensor_bytes", "recv_tensor_bytes", "peak_rss_linux_bytes",
    "kv_cache_bytes_end",
)


def validate_requests(raw, meta, new_tokens):
    if (not isinstance(raw, list) or not 1 <= len(raw) <= MAX_REQUESTS
            or type(new_tokens) is not int
            or not 1 <= new_tokens <= MAX_NEW_TOKENS):
        raise ValueError("batch requires 1..4 requests and 1..8 new tokens")
    ids = set()
    result = []
    for entry in raw:
        if not isinstance(entry, dict) or set(entry) != {"id", "prompt_ids"}:
            raise ValueError("request must contain only id and prompt_ids")
        name, prompt = entry["id"], entry["prompt_ids"]
        if (not isinstance(name, str) or not REQUEST_ID.fullmatch(name)
                or name in ids):
            raise ValueError("invalid or duplicate request id")
        ids.add(name)
        if (not isinstance(prompt, list) or not 1 <= len(prompt) <= 128
                or len(prompt) + new_tokens - 1 > meta["positions"]
                or any(type(v) is not int or not 0 <= v < meta["vocab"]
                       for v in prompt)):
            raise ValueError("invalid request prompt/context")
        result.append({"id": name, "prompt_ids": list(prompt)})
    return result


def load_requests(path):
    p = Path(path)
    if (p.is_symlink() or not p.is_file()
            or not 0 < p.stat().st_size <= MAX_PROMPTS_FILE_BYTES):
        raise ValueError("missing, oversized or unsafe local request JSON")
    try:
        document = json.loads(p.read_text(encoding="utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("malformed local request JSON") from exc
    if not isinstance(document, dict) or set(document) != {"requests"}:
        raise ValueError("expected JSON object with requests list")
    return document["requests"]


def kv_bytes(caches):
    return sum(
        tensor.numel() * tensor.element_size()
        for per_request in caches
        for pair in per_request if pair is not None
        for tensor in pair
    )


def projected_kv_bytes(meta, lengths, new_tokens, *, stage_index):
    """Double the final KV footprint to reserve for torch.cat transient peaks.

    This is a KV-specific envelope, not a hard process-RSS or memory cap.
    """
    bounds = [((meta["layers"] + 1) // 2), meta["layers"]]
    layers = bounds[0] if stage_index == 0 else bounds[1] - bounds[0]
    return (
        2 * 2 * 4 * meta["kv_heads"] * meta["head_dim"]
        * layers * sum(n + new_tokens - 1 for n in lengths)
    )


@torch.inference_mode()
def generate_multi(bundle, raw_requests, new_tokens, rank, world, *,
                   mode="overlap", cache_mib=0, max_kv_mib=64,
                   report_logits=True):
    if world != 2 or rank not in (0, 1) or mode not in ("overlap", "sequential"):
        raise ValueError("only localhost two-stage PP modes are supported")
    if (type(cache_mib) is not int or not 0 <= cache_mib <= 512
            or type(max_kv_mib) is not int or not 1 <= max_kv_mib <= 4096):
        raise ValueError("invalid weight cache or KV budget")
    error = None
    cfg = meta = stream = io = local_meta = requests = None
    try:
        cfg = inspect_pipeline(bundle, rank=rank)
        meta = cfg["source_model"]
        if type(new_tokens) is not int or not 1 <= new_tokens <= MAX_NEW_TOKENS:
            raise ValueError("generation length outside permitted range")
        if rank == 0:
            requests = validate_requests(raw_requests, meta, new_tokens)
        stream, io, local_meta = _load_stage(
            bundle, rank, cfg, cache_mib=cache_mib)
    except Exception as exc:
        error = str(exc)
    # Both peers complete this collective even when one rank's local input
    # or stage file fails. No P2P traffic is started on a failed preflight.
    ready = torch.tensor([0 if error else 1], dtype=torch.int32)
    dist.all_reduce(ready, op=dist.ReduceOp.MIN)
    if not bool(ready.item()):
        raise ValueError("Qwen2 multi-request pipeline preflight failed: "
                         + (error or "peer rejected input or stage weights"))

    sizes = torch.zeros(MAX_REQUESTS + 1, dtype=torch.int64)
    if rank == 0:
        sizes[0] = len(requests)
        for index, request in enumerate(requests):
            sizes[1 + index] = len(request["prompt_ids"])
    dist.broadcast(sizes, src=0)
    count = int(sizes[0].item())
    lengths = [int(item) for item in sizes[1:count+1].tolist()]
    budget_ok = (
        1 <= count <= MAX_REQUESTS
        and all(1 <= n <= 128 and n + new_tokens - 1 <= meta["positions"]
                for n in lengths)
        and projected_kv_bytes(meta, lengths, new_tokens, stage_index=rank)
            <= max_kv_mib * 1048576
    )
    limit = torch.tensor([1 if budget_ok else 0], dtype=torch.int32)
    dist.all_reduce(limit, op=dist.ReduceOp.MIN)
    if not bool(limit.item()):
        raise ValueError("Qwen2 multi-request pipeline KV/context budget exceeded")

    caches = [[None] * len(stream) for _ in range(count)]
    generated = [[] for _ in range(count)]
    logits_by_request = [[] for _ in range(count)] if report_logits else None
    pending_tokens = ([r["prompt_ids"][:] for r in requests]
                      if rank == 0 else None)

    send_calls = recv_calls = send_bytes = recv_bytes = 0
    compute_ns = wait_ns = 0
    # Counts scheduling opportunities, not demonstrated concurrent execution.
    scheduling_opportunities = 0
    peak_inflight = 0
    started = time.perf_counter_ns()
    prefill_us = 0

    def account_send(tensor):
        nonlocal send_calls, send_bytes
        send_calls += 1
        send_bytes += tensor.numel() * tensor.element_size()

    def account_recv(tensor):
        nonlocal recv_calls, recv_bytes
        recv_calls += 1
        recv_bytes += tensor.numel() * tensor.element_size()

    for step in range(new_tokens):
        step_started = time.perf_counter_ns()
        if rank == 0:
            in_flight = []
            for rid in range(count):
                t = time.perf_counter_ns()
                tokens = torch.tensor(pending_tokens[rid], dtype=torch.long)
                x = io.embed(tokens).unsqueeze(0).contiguous()
                x, caches[rid] = forward_layers(
                    x, stream, caches[rid], local_meta)
                compute_ns += time.perf_counter_ns() - t
                outgoing = x.contiguous()
                returned = torch.empty_like(outgoing)
                tag = 2 * (step * MAX_REQUESTS + rid)
                if mode == "overlap":
                    # Post both directions before computing the next request.
                    sent = dist.isend(outgoing, dst=1, tag=tag)
                    received = dist.irecv(returned, src=1, tag=tag+1)
                    in_flight.append((outgoing, returned, sent, received))
                    peak_inflight = max(peak_inflight, len(in_flight))
                    if rid:
                        scheduling_opportunities += 1
                else:
                    t = time.perf_counter_ns()
                    dist.send(outgoing, dst=1, tag=tag)
                    dist.recv(returned, src=1, tag=tag+1)
                    wait_ns += time.perf_counter_ns() - t
                    in_flight.append((outgoing, returned, None, None))
                account_send(outgoing)
                account_recv(returned)

                if mode == "sequential":
                    t = time.perf_counter_ns()
                    tail = rms_norm(returned[:, -1, :], io.norm, meta["rms_eps"])
                    values = io.logits(tail)[0]
                    token = int(values.argmax().item())
                    generated[rid].append(token)
                    pending_tokens[rid] = [token]
                    if report_logits:
                        logits_by_request[rid].append(values.tolist())
                    compute_ns += time.perf_counter_ns() - t
            if mode == "overlap":
                for rid, (outgoing, returned, sent, received) in enumerate(in_flight):
                    t = time.perf_counter_ns()
                    sent.wait()
                    received.wait()
                    wait_ns += time.perf_counter_ns() - t
                    t = time.perf_counter_ns()
                    tail = rms_norm(returned[:, -1, :], io.norm, meta["rms_eps"])
                    values = io.logits(tail)[0]
                    token = int(values.argmax().item())
                    generated[rid].append(token)
                    pending_tokens[rid] = [token]
                    if report_logits:
                        logits_by_request[rid].append(values.tolist())
                    compute_ns += time.perf_counter_ns() - t
                # Keep all send buffers alive until their requests are done.
        else:
            outstanding = []
            for rid, length in enumerate(lengths):
                width = length if step == 0 else 1
                incoming = torch.empty((1, width, meta["hidden"]), dtype=torch.float32)
                tag = 2 * (step * MAX_REQUESTS + rid)
                t = time.perf_counter_ns()
                dist.recv(incoming, src=0, tag=tag)
                wait_ns += time.perf_counter_ns() - t
                account_recv(incoming)
                t = time.perf_counter_ns()
                output, caches[rid] = forward_layers(
                    incoming, stream, caches[rid], local_meta)
                compute_ns += time.perf_counter_ns() - t
                outgoing = output.contiguous()
                if mode == "overlap":
                    handle = dist.isend(outgoing, dst=0, tag=tag+1)
                    outstanding.append((outgoing, handle))
                    peak_inflight = max(peak_inflight, len(outstanding))
                    if rid:
                        scheduling_opportunities += 1
                else:
                    t = time.perf_counter_ns()
                    dist.send(outgoing, dst=0, tag=tag+1)
                    wait_ns += time.perf_counter_ns() - t
                account_send(outgoing)
            for outgoing, handle in outstanding:
                t = time.perf_counter_ns()
                handle.wait()
                wait_ns += time.perf_counter_ns() - t
        if step == 0:
            prefill_us = (time.perf_counter_ns() - step_started) // 1000

    elapsed = (time.perf_counter_ns() - started) / 1e9
    local_stats = torch.tensor([
        int(elapsed * 1e6), compute_ns // 1000, wait_ns // 1000,
        send_calls, recv_calls, send_bytes, recv_bytes,
        _peak_rss_linux(), kv_bytes(caches)
    ], dtype=torch.int64)
    peers = [torch.zeros_like(local_stats) for _ in range(world)]
    dist.all_gather(peers, local_stats)
    if rank != 0:
        return None
    profiles = [
        dict(zip(PROFILE_FIELDS, map(int, record.tolist())))
        for record in peers
    ]
    return {
        "engine": "opencompute-qwen2-pipeline-batched-v0.4.8",
        "mode": mode,
        "model": meta["source"],
        "workers": world,
        "request_count": count,
        "max_active_requests": MAX_REQUESTS,
        "generated_tokens_total": count * new_tokens,
        "elapsed_seconds_rank0": round(elapsed, 6),
        "total_tokens_per_second_rank0": round(count * new_tokens / elapsed, 3),
        "rank_profiles": profiles,
        "scheduling_opportunities_rank0": scheduling_opportunities,
        "peak_inflight_rank0": peak_inflight,
        "kv_budget_bytes_per_rank": max_kv_mib * 1048576,
        "overlap_note": ("Nonblocking P2P permits stage-level work to overlap, "
                         "but scheduling opportunities do NOT prove CPU overlap."),
        "scope": ("trusted localhost Gloo CPU; no pre-trained weights, dynamic "
                   "admission, remote connectivity or request cancellation"),
        "results": [
            {"id": req["id"], "prompt_ids": req["prompt_ids"],
             "generated_ids": generated[rid],
             **({"step_logits": logits_by_request[rid]} if report_logits else {})}
            for rid, req in enumerate(requests)
        ],
    }


def main():
    parser = argparse.ArgumentParser(
        description="Bounded trusted-localhost Qwen2 two-stage multi-request pipeline")
    parser.add_argument("--bundle", required=True)
    parser.add_argument("--requests-json", required=True)
    parser.add_argument("--mode", choices=("overlap", "sequential"),
                        default="overlap")
    parser.add_argument("--max-new-tokens", type=int, default=3)
    parser.add_argument("--max-kv-mib", type=int, default=64)
    parser.add_argument("--cache-mib", type=int, default=0)
    parser.add_argument("--report-logits", action="store_true")
    args = parser.parse_args()
    torch.set_num_threads(1)
    rank, world = int(os.environ["RANK"]), int(os.environ["WORLD_SIZE"])
    # Read user data only at rank0 and fail through synchronized preflight.
    requests = None
    if rank == 0:
        try:
            requests = load_requests(args.requests_json)
        except (ValueError, OSError) as exc:
            requests = [{"id": "invalid", "prompt_ids": []}]
    store = init_gloo(timeout_seconds=90)
    try:
        result = generate_multi(
            args.bundle, requests, args.max_new_tokens, rank, world,
            mode=args.mode, cache_mib=args.cache_mib,
            max_kv_mib=args.max_kv_mib, report_logits=args.report_logits)
        if rank == 0:
            print(json.dumps(result, ensure_ascii=False), flush=True)
    finally:
        dist.destroy_process_group()
        del store


if __name__ == "__main__":
    main()
