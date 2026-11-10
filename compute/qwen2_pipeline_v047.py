"""Trusted-localhost Qwen2 2-stage pipeline prototype (PP), NOT TP.

Each rank owns entire, *different* transformer layers, unlike Qwen2 tensor
parallelism. Stage 0 keeps the complete chunked vocabulary IO; stage 1 returns
the final hidden tensor. This prototype deliberately has NO public-Worker
scheduler integration, remote peer connection, weight URL, or arbitrary code
execution. Stages are serialized for a single autoregressive request.
"""
import argparse
import hashlib
import json
import os
import shutil
import sys
import time
from pathlib import Path

import torch
import torch.distributed as dist

from compute.qwen2_chunked import (
    ChunkedIO, LayerStream, FORMAT as TP_FORMAT, MAX_FILE_BYTES,
    chunks, rank_files)
from compute.qwen2_core import forward_layers, rms_norm
from compute.rendezvous import init_gloo
from cluster_v031.checkpoints import inspect_bundle, validate_manifest


FORMAT = "opencompute-qwen2-pipeline-v1"
MAX_FILES = 512


def stage_bounds(layers):
    if type(layers) is not int or not 2 <= layers <= 48:
        raise ValueError("two-stage pipeline needs 2..48 full layers")
    boundary = (layers + 1) // 2
    return [[0, boundary], [boundary, layers]]


def expected_stage_paths(meta, bounds):
    """Derive all paths from validated dimensions, never trust arbitrary names."""
    if (not isinstance(bounds, list) or bounds != stage_bounds(meta["layers"])
            or meta["workers"] != 1 or meta["format"] != TP_FORMAT):
        raise ValueError("unexpected Qwen2 PP stage layout")
    paths = []
    for rank, (first, stop) in enumerate(bounds):
        paths += [
            f"stage_{rank}/rank_0_layer_{local:03d}.safetensors"
            for local in range(stop-first)
        ]
    paths.append("stage_0/rank0_norm.safetensors")
    for kind in ("embed", "head"):
        if kind == "head" and meta["tied_embeddings"]:
            continue
        paths.extend([
            f"stage_0/rank0_{kind}_{index:03d}.safetensors"
            for index in range(len(chunks(meta)))])
    if len(paths) > MAX_FILES or len(set(paths)) != len(paths):
        raise ValueError("too many or repeated PP shard files")
    return sorted(paths)


def _sha256(path):
    dig = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            dig.update(block)
    return dig.hexdigest()


def partition_one_rank_v2(source, destination):
    """Make a separate, checksum-pinned two-stage bundle from a trusted TP1.

    Source is immutable to this function. Output must not already exist.
    Conversion never deserializes Python objects or downloads weights.
    """
    src = Path(source).resolve(strict=True)
    if not src.is_dir() or (src / "manifest.json").is_symlink():
        raise ValueError("unsafe single-rank source directory")
    meta = json.loads((src / "manifest.json").read_text(encoding="utf-8"))
    validate_manifest(meta, expected_workers=1)
    if meta["format"] != TP_FORMAT:
        raise ValueError("pipeline requires one-rank Qwen2 v2 Safetensors")
    inspect_bundle(src, meta)
    bounds = stage_bounds(meta["layers"])
    dest = Path(destination)
    resolved = dest.resolve()
    if resolved == src or src in resolved.parents or resolved in src.parents:
        raise ValueError("pipeline output must be separate from source tree")
    expected = expected_stage_paths(meta, bounds)
    dest.mkdir(parents=True, exist_ok=False)
    try:
        for rank, (first, stop) in enumerate(bounds):
            (dest / f"stage_{rank}").mkdir()
            for local, global_index in enumerate(range(first, stop)):
                shutil.copyfile(
                    src / f"rank_0_layer_{global_index:03d}.safetensors",
                    dest / f"stage_{rank}/rank_0_layer_{local:03d}.safetensors")
        for name in rank_files(meta, 0):
            if name == "manifest.json" or "_layer_" in name:
                continue
            shutil.copyfile(src / name, dest / "stage_0" / name)
        records = {}
        for relpath in expected:
            path = dest / relpath
            size = path.stat().st_size
            if not 0 < size <= MAX_FILE_BYTES:
                raise ValueError("invalid stage file size")
            records[relpath] = {"size": size, "sha256": _sha256(path)}
        manifest = {
            "format": FORMAT, "world_size": 2,
            "source_model": meta, "bounds": bounds,
            "files": records,
            "scope": "trusted-localhost research only",
        }
        (dest / "pipeline_manifest.json").write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False),
            encoding="utf-8")
        return manifest
    except BaseException:
        shutil.rmtree(dest, ignore_errors=True)
        raise


def inspect_pipeline(bundle, *, rank=None):
    folder = Path(bundle)
    manifest_path = folder / "pipeline_manifest.json"
    if manifest_path.is_symlink() or not manifest_path.is_file() or (
            manifest_path.stat().st_size > 32768):
        raise ValueError("pipeline manifest missing or unsafe")
    config = json.loads(manifest_path.read_text(encoding="utf-8"))
    if (not isinstance(config, dict) or config.get("format") != FORMAT
            or config.get("world_size") != 2):
        raise ValueError("unsupported pipeline manifest")
    meta = config.get("source_model")
    validate_manifest(meta, expected_workers=1)
    expected = expected_stage_paths(meta, config.get("bounds"))
    records = config.get("files")
    if not isinstance(records, dict) or set(records) != set(expected):
        raise ValueError("pipeline file inventory mismatch")
    if rank is not None and rank not in (0, 1):
        raise ValueError("invalid PP rank")
    # Strictly inspect a rank's own files. Files for peer rank may be absent
    # on that worker; the full bundle is checked at export time.
    selected = expected if rank is None else [
        name for name in expected if name.startswith(f"stage_{rank}/")]
    for rel in selected:
        p = folder / rel
        if p.is_symlink() or not p.is_file():
            raise ValueError(f"missing/symlinked pipeline shard: {rel}")
        item = records[rel]
        size = p.stat().st_size
        if (not isinstance(item, dict) or type(item.get("size")) is not int
                or not 0 < size <= MAX_FILE_BYTES
                or size != item["size"] or not isinstance(item.get("sha256"), str)
                or len(item["sha256"]) != 64 or _sha256(p) != item["sha256"]):
            raise ValueError(f"pipeline shard integrity mismatch: {rel}")
    return config


def _load_stage(folder, rank, cfg, cache_mib=0):
    meta = cfg["source_model"]
    first, stop = cfg["bounds"][rank]
    local_meta = dict(meta, layers=stop-first)
    stage_dir = Path(folder) / f"stage_{rank}"
    stream = LayerStream(stage_dir, 0, local_meta, cache_mib=cache_mib)
    for i in range(len(stream)):
        stream._read_layer(i)
    io = ChunkedIO(stage_dir, meta, cache_mib=cache_mib) if rank == 0 else None
    return stream, io, local_meta


def _peak_rss_linux():
    if not sys.platform.startswith("linux"):
        return 0
    try:
        import resource
        return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024)
    except (ImportError, OSError):
        return 0


@torch.inference_mode()
def generate_pipeline(bundle, prompt_ids, max_new_tokens, rank, world,
                      *, report_logits=False, cache_mib=0):
    if world != 2 or rank not in (0, 1):
        raise ValueError("prototype supports exactly two Gloo ranks")
    err = None
    cfg = stream = io = meta = None
    try:
        cfg = inspect_pipeline(bundle, rank=rank)
        meta = cfg["source_model"]
        if type(max_new_tokens) is not int or not 1 <= max_new_tokens <= 16:
            raise ValueError("invalid generation length")
        if rank == 0:
            if (not isinstance(prompt_ids, list) or not prompt_ids
                    or any(type(i) is not int or not 0 <= i < meta["vocab"]
                           for i in prompt_ids)
                    or len(prompt_ids) + max_new_tokens - 1 > meta["positions"]):
                raise ValueError("invalid PP prompt tokens")
        stream, io, local_meta = _load_stage(
            bundle, rank, cfg, cache_mib=cache_mib)
    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"
    all_ready = torch.tensor([0 if err else 1], dtype=torch.int32)
    dist.all_reduce(all_ready, op=dist.ReduceOp.MIN)
    if not all_ready.item():
        raise ValueError("Qwen2 pipeline rank preflight failed: "
                         + (err or "peer stage rejected its files"))

    length = torch.tensor([len(prompt_ids) if rank == 0 else 0],
                          dtype=torch.int64)
    dist.broadcast(length, src=0)
    n = int(length.item())
    if not 1 <= n <= meta["positions"] - max_new_tokens + 1:
        raise ValueError("invalid PP synchronized context")
    generated, all_logits, caches = [], [], [None] * len(stream)
    pending = list(prompt_ids) if rank == 0 else []
    comm_ns, bytes_sent, bytes_received = 0, 0, 0
    started_ns = time.perf_counter_ns()
    prefill_us = 0

    def send(value, target, tag):
        nonlocal comm_ns, bytes_sent
        t = time.perf_counter_ns()
        dist.send(value.contiguous(), dst=target, tag=tag)
        comm_ns += time.perf_counter_ns() - t
        bytes_sent += value.numel() * value.element_size()

    def recv(value, target, tag):
        nonlocal comm_ns, bytes_received
        t = time.perf_counter_ns()
        dist.recv(value, src=target, tag=tag)
        comm_ns += time.perf_counter_ns() - t
        bytes_received += value.numel() * value.element_size()

    for step in range(max_new_tokens):
        step_ns = time.perf_counter_ns()
        sequence_length = n if step == 0 else 1
        if rank == 0:
            tokens = torch.tensor(pending, dtype=torch.long)
            x = io.embed(tokens).unsqueeze(0)
            x, caches = forward_layers(x, stream, caches, local_meta)
            send(x, 1, 2 * step)
            returned = torch.empty_like(x)
            recv(returned, 1, 2 * step + 1)
            tail = rms_norm(returned[:, -1, :], io.norm, meta["rms_eps"])
            logits = io.logits(tail)[0]
            chosen = int(logits.argmax().item())
            pending = [chosen]
            generated.append(chosen)
            if report_logits:
                all_logits.append(logits.tolist())
        else:
            incoming = torch.empty((1, sequence_length, meta["hidden"]),
                                   dtype=torch.float32)
            recv(incoming, 0, 2 * step)
            x, caches = forward_layers(incoming, stream, caches, local_meta)
            send(x, 0, 2 * step + 1)
        if step == 0:
            prefill_us = (time.perf_counter_ns() - step_ns) // 1000
    elapsed = (time.perf_counter_ns() - started_ns) / 1e9

    record = torch.tensor([
        int(elapsed * 1e6), int(comm_ns // 1000),
        bytes_sent, bytes_received, prefill_us, _peak_rss_linux(),
    ], dtype=torch.int64)
    gathered = [torch.zeros_like(record) for _ in range(world)]
    dist.all_gather(gathered, record)
    if rank != 0:
        return None
    stats = [dict(zip(("decode_us", "send_recv_wall_us",
                       "sent_tensor_bytes", "received_tensor_bytes",
                       "prefill_us", "peak_rss_bytes_linux"),
                      [int(v) for v in item.tolist()]))
             for item in gathered]
    return {
        "engine": "opencompute-qwen2-pipeline-v0.4.7",
        "model": meta["source"], "workers": world, "stages": cfg["bounds"],
        "prompt_ids": prompt_ids, "generated_ids": generated,
        "elapsed_seconds_rank0": round(elapsed, 6),
        "tokens_per_second_rank0": round(max_new_tokens / elapsed, 3),
        "rank_profiles": stats,
        "measurement_scope": (
            "single host Gloo send/recv wall time includes waiting; "
            "tensor bytes are not measured wire bytes"),
        **({"step_logits": all_logits} if report_logits else {}),
    }


def main():
    parser = argparse.ArgumentParser(description="Trusted-localhost 2-stage Qwen2 pipeline")
    parser.add_argument("--bundle", required=True)
    parser.add_argument("--prompt-ids", default="1,2,3")
    parser.add_argument("--max-new-tokens", type=int, default=3)
    parser.add_argument("--report-logits", action="store_true")
    parser.add_argument("--cache-mib", type=int, default=0)
    opts = parser.parse_args()
    if not 0 <= opts.cache_mib <= 512:
        parser.error("cache budget must be 0..512 MiB")
    torch.set_num_threads(1)
    rank, world = int(os.environ["RANK"]), int(os.environ["WORLD_SIZE"])
    store = init_gloo(timeout_seconds=120)
    try:
        error, tokens = None, []
        if rank == 0:
            try:
                tokens = [int(item.strip()) for item in opts.prompt_ids.split(",")]
            except Exception as exc:
                error = f"prompt parse failed: {exc}"
        # Even malformed rank-0 prompts enter a common preflight so peer
        # ranks do not hang on the first expected send/recv.
        if error:
            tokens = [-1]
        result = generate_pipeline(
            Path(opts.bundle), tokens, opts.max_new_tokens, rank, world,
            report_logits=opts.report_logits, cache_mib=opts.cache_mib)
        if rank == 0:
            print(json.dumps(result, ensure_ascii=False), flush=True)
    finally:
        dist.destroy_process_group()
        del store


if __name__ == "__main__":
    main()
