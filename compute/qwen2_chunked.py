"""Qwen2 TP v2: chunked vocabulary IO and one-file-per-rank-per-layer Safetensors.

The exporter processes one layer at a time. The inference runner reads vocabulary
chunks and layer shards on demand, trading throughput for bounded weight RAM.
This is NOT guaranteed constant process RSS and has no network/untrusted-node
security guarantees. Legacy v1 .pt checkpoints remain separately supported.
"""
import json
import math
import shutil
from collections import OrderedDict
from pathlib import Path

import torch
import torch.nn.functional as F
from safetensors import safe_open
from safetensors.torch import load_file, save_file

from compute.qwen2_shards import manifest_from_config

FORMAT = "opencompute-qwen2-tp-v2"
MAX_FILE_BYTES = 512_000_000
MAX_FILES = 512


def chunks(meta):
    rows = meta["io_chunk_rows"]
    return [(start, min(start + rows, meta["vocab"]))
            for start in range(0, meta["vocab"], rows)]


def rank_files(meta, rank):
    if not 0 <= rank < meta["workers"]:
        raise ValueError("invalid rank")
    names = ["manifest.json"] + [
        f"rank_{rank}_layer_{index:03d}.safetensors"
        for index in range(meta["layers"])]
    if rank == 0:
        names.append("rank0_norm.safetensors")
        names += [f"rank0_embed_{i:03d}.safetensors"
                  for i in range(len(chunks(meta)))]
        if not meta["tied_embeddings"]:
            names += [f"rank0_head_{i:03d}.safetensors"
                      for i in range(len(chunks(meta)))]
    return names


def all_files(meta):
    names = []
    for rank in range(meta["workers"]):
        names.extend(rank_files(meta, rank))
    names = list(dict.fromkeys(names))
    if len(names) > MAX_FILES:
        raise ValueError("too many individual shard files")
    return names


def weight_dtype(meta):
    name = meta.get("weight_dtype", "float32")
    if name not in ("float32", "bfloat16"):
        raise ValueError("unsupported Qwen2 storage precision")
    return torch.float32 if name == "float32" else torch.bfloat16


def _write_tensor(file, tensor, storage_type=torch.float32):
    # Safetensors is data-only. Compute always promotes BF16 weights to FP32.
    save_file({"weight": tensor.detach().to(
        device="cpu", dtype=storage_type).contiguous()}, str(file))
    if file.stat().st_size > MAX_FILE_BYTES:
        raise ValueError(f"individual output exceeds safe file cap: {file.name}")


def _get_rows(state, key, first, last, shape):
    if hasattr(state, "get_rows"):
        value = state.get_rows(key, first, last)
    else:
        value = state[key][first:last]
    if not isinstance(value, torch.Tensor) or tuple(value.shape) != (
            last - first, shape[1]):
        raise ValueError(f"wrong sliced tensor shape: {key}")
    return value.to(dtype=torch.float32, device="cpu").contiguous()


def export_chunked(state, config, destination, workers=2, *,
                   chunk_target_bytes=64 * 1024 * 1024, chunk_rows=None,
                   source="trusted-local", storage_dtype="float32"):
    """Write one trusted layer shard at a time; never accumulate whole ranks.

    For on-demand source input, a Mapping with get_rows() avoids reading the
    full 0.9GB embedding/LM head tensor even in the exporter.
    """
    meta = manifest_from_config(config, workers, source)
    if storage_dtype not in ("float32", "bfloat16"):
        raise ValueError("storage_dtype must be float32 or bfloat16")
    bytes_per_weight = 2 if storage_dtype == "bfloat16" else 4
    storage_type = torch.bfloat16 if storage_dtype == "bfloat16" else torch.float32
    if chunk_rows is None:
        chunk_rows = max(1, min(meta["vocab"],
                         chunk_target_bytes // (bytes_per_weight * meta["hidden"])))
    if type(chunk_rows) is not int or not 1 <= chunk_rows <= meta["vocab"]:
        raise ValueError("invalid vocabulary chunk size")
    meta.update({"format": FORMAT, "io_chunk_rows": chunk_rows,
                 "weight_dtype": storage_dtype})
    names = all_files(meta)
    output = Path(destination)
    output.mkdir(parents=True, exist_ok=False)
    try:
        # Perform the output-size check from dimensions before allocating weights.
        head_bytes = bytes_per_weight * chunk_rows * meta["hidden"] + 4096
        h, q, kv, d, f = (meta["hidden"], meta["heads"], meta["kv_heads"],
                          meta["head_dim"], meta["inner"])
        rank_layer_bytes = bytes_per_weight * (2*h + (q*d//workers + 2*kv*d//workers)*h
                                + (q*d//workers + 2*kv*d//workers)
                                + h*(q*d//workers) + 3*h*(f//workers)) + 16384
        if head_bytes > MAX_FILE_BYTES or rank_layer_bytes > MAX_FILE_BYTES:
            raise ValueError("one vocabulary chunk or rank layer exceeds file cap")
        def weight(key, shape):
            x = state[key]
            if (not isinstance(x, torch.Tensor)
                    or tuple(x.shape) != tuple(shape)):
                raise ValueError(f"wrong Qwen2 tensor dimensions: {key}")
            return x.detach().to(device="cpu", dtype=torch.float32).contiguous()

        def bias(key, size):
            return weight(key, (size,)) if key in state else torch.zeros(size)

        for kind, original in (("embed", "model.embed_tokens.weight"),
                               ("head", "lm_head.weight")):
            if kind == "head" and meta["tied_embeddings"]:
                continue
            for i, (start, stop) in enumerate(chunks(meta)):
                value = _get_rows(state, original, start, stop,
                                  (meta["vocab"], h))
                _write_tensor(output / f"rank0_{kind}_{i:03d}.safetensors",
                              value, storage_type)
                del value
        _write_tensor(output / "rank0_norm.safetensors",
                      weight("model.norm.weight", (h,)), storage_type)

        local_q, local_kv, local_f = q*d//workers, kv*d//workers, f//workers
        for layer_id in range(meta["layers"]):
            prefix = f"model.layers.{layer_id}."
            attention = prefix + "self_attn."
            mlp = prefix + "mlp."
            q_w = weight(attention+"q_proj.weight", (q*d,h))
            k_w = weight(attention+"k_proj.weight", (kv*d,h))
            v_w = weight(attention+"v_proj.weight", (kv*d,h))
            q_b = bias(attention+"q_proj.bias", q*d)
            k_b = bias(attention+"k_proj.bias", kv*d)
            v_b = bias(attention+"v_proj.bias", kv*d)
            o_w = weight(attention+"o_proj.weight", (h,q*d))
            gate = weight(mlp+"gate_proj.weight", (f,h))
            up = weight(mlp+"up_proj.weight", (f,h))
            down = weight(mlp+"down_proj.weight", (h,f))
            in_norm = weight(prefix+"input_layernorm.weight", (h,))
            post_norm = weight(prefix+"post_attention_layernorm.weight", (h,))
            for rank in range(workers):
                qs = slice(rank*local_q, (rank+1)*local_q)
                ks = slice(rank*local_kv, (rank+1)*local_kv)
                fs = slice(rank*local_f, (rank+1)*local_f)
                shard = {
                    "input_norm": in_norm.clone(), "post_norm": post_norm.clone(),
                    "q_w": q_w[qs].contiguous(), "q_b": q_b[qs].contiguous(),
                    "k_w": k_w[ks].contiguous(), "k_b": k_b[ks].contiguous(),
                    "v_w": v_w[ks].contiguous(), "v_b": v_b[ks].contiguous(),
                    "o_w": o_w[:,qs].contiguous(),
                    "gate_w": gate[fs].contiguous(),
                    "up_w": up[fs].contiguous(),
                    "down_w": down[:,fs].contiguous(),
                }
                dest = output / f"rank_{rank}_layer_{layer_id:03d}.safetensors"
                save_file({key: value.to(dtype=storage_type).contiguous()
                           for key, value in shard.items()}, str(dest))
                if dest.stat().st_size > MAX_FILE_BYTES:
                    raise ValueError("layer shard exceeds safe file cap")
        (output / "manifest.json").write_text(
            json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
        from cluster_v031.checkpoints import inspect_bundle
        inspect_bundle(output, meta)
        return meta
    except BaseException:
        # Created with exist_ok=False. Never erase a pre-existing folder.
        shutil.rmtree(output, ignore_errors=True)
        raise


def _checked_tensor(path, shape, name="weight"):
    data = load_file(str(path), device="cpu")
    if (set(data) != {name} or tuple(data[name].shape) != tuple(shape)
            or data[name].dtype != torch.float32
            or not torch.isfinite(data[name]).all().item()):
        raise ValueError(f"invalid chunk tensor {path.name}")
    return data[name]


def _layer_shapes(meta):
    h, d = meta["hidden"], meta["head_dim"]
    q = meta["heads"]*d//meta["workers"]
    kv = meta["kv_heads"]*d//meta["workers"]
    f = meta["inner"]//meta["workers"]
    return {"input_norm": (h,), "post_norm": (h,),
            "q_w": (q,h), "q_b": (q,), "k_w": (kv,h), "k_b": (kv,),
            "v_w": (kv,h), "v_b": (kv,), "o_w": (h,q),
            "gate_w": (f,h), "up_w": (f,h), "down_w": (h,f)}


class LayerStream:
    def __init__(self, folder, rank, meta):
        self.root, self.rank, self.meta = Path(folder), rank, meta

    def __len__(self):
        return self.meta["layers"]

    def __iter__(self):
        shapes = _layer_shapes(self.meta)
        for i in range(len(self)):
            path = self.root / f"rank_{self.rank}_layer_{i:03d}.safetensors"
            layer = load_file(str(path), device="cpu")
            if set(layer) != set(shapes):
                raise ValueError(f"invalid layer keys: {path.name}")
            for key, shape in shapes.items():
                x = layer[key]
                if (tuple(x.shape) != shape or x.dtype != torch.float32
                        or not torch.isfinite(x).all().item()):
                    raise ValueError(f"invalid layer tensor: {key}")
            yield layer


class ChunkedIO:
    def __init__(self, folder, meta):
        self.root, self.meta = Path(folder), meta
        self.norm = _checked_tensor(self.root / "rank0_norm.safetensors",
                                    (meta["hidden"],))

    def _chunk(self, kind, i, start, stop):
        if kind == "head" and self.meta["tied_embeddings"]:
            kind = "embed"
        return _checked_tensor(
            self.root / f"rank0_{kind}_{i:03d}.safetensors",
            (stop-start, self.meta["hidden"]))

    def embed(self, token_ids):
        flat = token_ids.reshape(-1)
        out = torch.empty((flat.numel(), self.meta["hidden"]),
                          dtype=torch.float32)
        for i, (start, stop) in enumerate(chunks(self.meta)):
            mask = (flat >= start) & (flat < stop)
            if mask.any().item():
                matrix = self._chunk("embed", i, start, stop)
                out[mask] = matrix[(flat[mask]-start).long()]
        return out.view(*token_ids.shape, self.meta["hidden"])

    def logits(self, hidden):
        parts = []
        for i, (start, stop) in enumerate(chunks(self.meta)):
            matrix = self._chunk("head", i, start, stop)
            parts.append(F.linear(hidden, matrix))
        return torch.cat(parts, dim=-1)


def load_chunked_rank(folder, rank, meta):
    files = rank_files(meta, rank)
    # Header preflight catches missing/corrupted files before any Gloo
    # broadcast, and neither stores all layers nor the full vocabulary in RAM.
    for name in files:
        if name == "manifest.json":
            continue
        path = Path(folder) / name
        if (path.is_symlink() or not path.is_file()
                or not 0 < path.stat().st_size <= MAX_FILE_BYTES):
            raise ValueError(f"missing, oversized or unsafe rank file: {name}")
        with safe_open(str(path), framework="pt", device="cpu") as obj:
            if "_layer_" in name:
                expected_shapes = _layer_shapes(meta)
            elif name == "rank0_norm.safetensors":
                expected_shapes = {"weight": (meta["hidden"],)}
            else:
                kind, slot = name.replace("rank0_", "").split("_")
                index = int(slot.split(".")[0])
                slices = chunks(meta)
                if index >= len(slices) or kind not in ("embed", "head"):
                    raise ValueError("unexpected vocabulary chunk name")
                start, stop = slices[index]
                expected_shapes = {"weight": (stop-start, meta["hidden"])}
            if set(obj.keys()) != set(expected_shapes):
                raise ValueError(f"invalid Safetensors header keys: {name}")
            for key, shape in expected_shapes.items():
                part = obj.get_slice(key)
                if tuple(part.get_shape()) != tuple(shape) or part.get_dtype() != "F32":
                    raise ValueError(f"invalid Safetensors header tensor: {name}:{key}")
    stream = LayerStream(folder, rank, meta)
    for _ in stream:  # Full validation, at most one loaded layer at a time.
        pass
    io = ChunkedIO(folder, meta) if rank == 0 else None
    return stream, io
