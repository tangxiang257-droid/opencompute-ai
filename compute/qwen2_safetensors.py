"""Offline-only, trust-bound Qwen2 Safetensors inspector and exporter.

Reads locally supplied config.json plus model.safetensors or the official
model.safetensors.index.json layout. Does not fetch any URL, run remote Python
code, load a pickle, or claim that a large model is currently deployable.

For checkpoints within the current per-file TP limit, LazilyDecodedWeights
provides an on-demand Mapping to the existing trusted Qwen2 rank exporter.
Export currently retains TP rank layer dictionaries in RAM: it is not a
constant-memory conversion of 1.5B+ models.
"""
import argparse
import hashlib
import json
import re
from collections.abc import Mapping
from pathlib import Path

from compute.qwen2_shards import export_state, manifest_from_config
from cluster_v031.checkpoints import MAX_FILE_BYTES

MAX_CONFIG_BYTES = 32768
MAX_INDEX_BYTES = 4_000_000
MAX_WEIGHT_FILE_BYTES = 12_000_000_000
MAX_WEIGHT_FILES = 128
OUTPUT_HEADROOM = 1_000_000
SAFE_SHARD_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,120}\.safetensors\Z")


def _regular(path, limit):
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"missing/unsafe regular file: {path.name}")
    size = path.stat().st_size
    if not 0 < size <= limit:
        raise ValueError(f"file size outside permitted range: {path.name}")
    return path


def _json_file(path, limit):
    file = _regular(path, limit)
    try:
        return json.loads(file.read_text(encoding="utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid JSON: {file.name}") from exc


def _expected_shapes(m):
    h, a, kv, head_dim, inner = (
        m["hidden"], m["heads"], m["kv_heads"], m["head_dim"], m["inner"])
    expected = {
        "model.embed_tokens.weight": (m["vocab"], h),
        "model.norm.weight": (h,),
    }
    if not m["tied_embeddings"]:
        expected["lm_head.weight"] = (m["vocab"], h)
    for i in range(m["layers"]):
        base = f"model.layers.{i}."
        attn = base + "self_attn."
        expected.update({
            base + "input_layernorm.weight": (h,),
            base + "post_attention_layernorm.weight": (h,),
            attn + "q_proj.weight": (a * head_dim, h),
            attn + "k_proj.weight": (kv * head_dim, h),
            attn + "v_proj.weight": (kv * head_dim, h),
            attn + "o_proj.weight": (h, a * head_dim),
            base + "mlp.gate_proj.weight": (inner, h),
            base + "mlp.up_proj.weight": (inner, h),
            base + "mlp.down_proj.weight": (h, inner),
        })
    return expected


def _optional_bias_shapes(m):
    expected = {}
    for i in range(m["layers"]):
        attn = f"model.layers.{i}.self_attn."
        for kind, count in (("q", m["heads"]), ("k", m["kv_heads"]),
                            ("v", m["kv_heads"])):
            expected[attn + kind + "_proj.bias"] = (count * m["head_dim"],)
    return expected


def storage_plan(meta):
    """Conservative float32 serialized-weight estimates, before any I/O."""
    h, d, w = meta["hidden"], meta["head_dim"], meta["workers"]
    q = meta["heads"] * d // w
    kv = meta["kv_heads"] * d // w
    f = meta["inner"] // w
    per_layer_elements = (
        2 * h + (q + 2 * kv) * h + q + 2 * kv
        + h * q + 3 * h * f)
    io_bytes = 4 * (2 * meta["vocab"] * h + h)
    rank_bytes = 4 * meta["layers"] * per_layer_elements
    limit = MAX_FILE_BYTES - OUTPUT_HEADROOM
    can_export = io_bytes <= limit and rank_bytes <= limit
    return {
        "architecture": "qwen2",
        "source": meta["source"],
        "workers": w,
        "rank0_io_estimated_bytes_fp32": io_bytes,
        "rank_shard_estimated_bytes_fp32": rank_bytes,
        "current_per_file_limit_bytes": MAX_FILE_BYTES,
        "exportable_under_current_limit": can_export,
        "reason": ("within current FP32 rank file envelope" if can_export else
                   "rank0_io.pt or a rank_N.pt is projected to exceed "
                   "the 512 MB file limit; requires chunked IO and memory-aware loading"),
    }


def plan_config(config, workers, *, source="trusted-offline-config"):
    meta = manifest_from_config(config, workers, source=source)
    return {"manifest": meta, "plan": storage_plan(meta)}


class LazySafetensors(Mapping):
    """Safe-open tensors on demand; never materialize all source tensors at once."""

    def __init__(self, root, mapping, expected):
        self.root = Path(root)
        self.mapping = mapping
        self.expected = expected

    def __len__(self):
        return len(self.mapping)

    def __iter__(self):
        return iter(self.mapping)

    def __getitem__(self, key):
        if key not in self.mapping:
            raise KeyError(key)
        from safetensors import safe_open
        file = self.root / self.mapping[key]
        _regular(file, MAX_WEIGHT_FILE_BYTES)
        with safe_open(str(file), framework="pt", device="cpu") as data:
            value = data.get_tensor(key)
        if tuple(value.shape) != self.expected[key]:
            raise ValueError("tensor shape changed after metadata inspection")
        return value


def inspect_directory(model_dir, workers, *, verify_headers=True):
    """Preflight trusted local Qwen2 weights; no model download or eager HF load."""
    root = Path(model_dir).resolve(strict=True)
    if not root.is_dir():
        raise ValueError("model directory is not a directory")
    config = _json_file(root / "config.json", MAX_CONFIG_BYTES)
    if not isinstance(config, dict):
        raise ValueError("model config must be a JSON object")
    meta = manifest_from_config(config, workers, source=root.name)
    plan = storage_plan(meta)

    index_path = root / "model.safetensors.index.json"
    direct_path = root / "model.safetensors"
    if index_path.exists() and direct_path.exists():
        raise ValueError("ambiguous single-file and sharded weight layout")
    if index_path.exists():
        index = _json_file(index_path, MAX_INDEX_BYTES)
        weight_map = index.get("weight_map") if isinstance(index, dict) else None
        if not isinstance(weight_map, dict) or not weight_map:
            raise ValueError("invalid Safetensors weight index")
        if any(not isinstance(k, str) or not isinstance(v, str)
               for k, v in weight_map.items()):
            raise ValueError("invalid tensor name or weight file in index")
        mapping = dict(weight_map)
    else:
        _regular(direct_path, MAX_WEIGHT_FILE_BYTES)
        mapping = None
    from safetensors import safe_open
    if mapping is None:
        with safe_open(str(direct_path), framework="pt", device="cpu") as weights:
            mapping = {k: direct_path.name for k in weights.keys()}
    files = sorted(set(mapping.values()))
    if not 1 <= len(files) <= MAX_WEIGHT_FILES:
        raise ValueError("unsupported number of Safetensors files")
    for name in files:
        if (not SAFE_SHARD_NAME.fullmatch(name) or name in (".", "..")
                or "/" in name or "\\" in name):
            raise ValueError("unsafe Safetensors filename in index")
        _regular(root / name, MAX_WEIGHT_FILE_BYTES)

    required = _expected_shapes(meta)
    biases = _optional_bias_shapes(meta)
    actual_keys = set(mapping)
    missing = set(required) - actual_keys
    unexpected = actual_keys - set(required) - set(biases)
    if missing:
        raise ValueError("missing required Qwen2 tensor: " + sorted(missing)[0])
    if unexpected:
        raise ValueError("unrecognized Qwen2 tensor: " + sorted(unexpected)[0])
    shapes = {**required, **{k: v for k, v in biases.items() if k in mapping}}
    if verify_headers:
        seen = set()
        for name in files:
            with safe_open(str(root / name), framework="pt", device="cpu") as weights:
                on_disk = set(weights.keys())
                promised = {key for key, value in mapping.items() if value == name}
                if on_disk != promised:
                    raise ValueError(f"Safetensors index mismatch in {name}")
                seen |= on_disk
                for key in on_disk:
                    part = weights.get_slice(key)
                    if tuple(part.get_shape()) != shapes[key]:
                        raise ValueError(f"Qwen2 tensor has wrong shape: {key}")
                    if part.get_dtype() not in ("F32", "F16", "BF16"):
                        raise ValueError(f"unsupported tensor type: {key}")
        if seen != actual_keys:
            raise ValueError("missing weight files or index keys")
    return {"manifest": meta, "plan": plan,
            "input_files": files, "tensor_count": len(mapping),
            "weights": LazySafetensors(root, mapping, shapes)}


def export_local(model_dir, output, workers, *, allow_existing=False):
    """Export only small checked models compatible with the current TP file caps."""
    inspected = inspect_directory(model_dir, workers)
    if not inspected["plan"]["exportable_under_current_limit"]:
        raise ValueError(inspected["plan"]["reason"])
    target = Path(output)
    source = Path(model_dir).resolve(strict=True)
    resolved = target.resolve()
    if resolved == source or source in resolved.parents:
        raise ValueError("cannot export inside the source model directory")
    if target.exists() and not allow_existing:
        raise ValueError("destination already exists; select an empty new folder")
    if target.is_symlink():
        raise ValueError("destination cannot be a symlink")
    # validate input *before* invoking torch.save via the existing exporter
    config = _json_file(source / "config.json", MAX_CONFIG_BYTES)
    try:
        manifest = export_state(inspected["weights"], config, target, workers,
                                source=source.name)
        from cluster_v031.checkpoints import inspect_bundle
        inspect_bundle(target, manifest)
        return manifest
    except BaseException:
        # Never automatically erase preexisting user files.
        if not target.exists() or allow_existing:
            raise
        import shutil
        shutil.rmtree(target, ignore_errors=True)
        raise


def main():
    parser = argparse.ArgumentParser(description="Inspect trusted local Qwen2 Safetensors")
    parser.add_argument("--model-dir", help="directory with config.json and safetensors")
    parser.add_argument("--config", help="config.json for size planning only")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--export-to", help="export currently supported small weights")
    args = parser.parse_args()
    if bool(args.config) == bool(args.model_dir):
        parser.error("choose exactly one of --model-dir or --config")
    if args.config:
        if args.export_to:
            parser.error("export requires --model-dir")
        config = _json_file(args.config, MAX_CONFIG_BYTES)
        result = plan_config(config, args.workers)
    elif args.export_to:
        manifest = export_local(args.model_dir, args.export_to, args.workers)
        result = {"exported": True, "manifest": manifest}
    else:
        inspected = inspect_directory(args.model_dir, args.workers)
        result = {k: v for k, v in inspected.items() if k != "weights"}
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
