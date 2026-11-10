"""Validation and immutable-size/digest checks for trusted local GPT-2 shards.

SHA-256 pins session *bytes*, not model authorship or untrusted Worker results.
Only files already exported and approved by the local administrator qualify.
"""
import hashlib
import json
import math
from pathlib import Path

MAX_FILE_BYTES = 512_000_000
MAX_MANIFEST_BYTES = 16_384


def validate_manifest(manifest, *, expected_workers=None):
    if not isinstance(manifest, dict) or manifest.get("format") not in (
            "opencompute-gpt2-tp-v1", "opencompute-qwen2-tp-v1",
            "opencompute-qwen2-tp-v2"):
        raise ValueError("unsupported tensor-parallel manifest")
    qwen2 = manifest["format"] in ("opencompute-qwen2-tp-v1",
                                     "opencompute-qwen2-tp-v2")
    limits = {"workers": 4, "hidden": 4096, "heads": 128,
              "layers": 48, "positions": 131072, "inner": 32768,
              "vocab": 262144}
    for name, upper in limits.items():
        v = manifest.get(name)
        if type(v) is not int or not 1 <= v <= upper:
            raise ValueError(f"invalid or oversized manifest dimension: {name}")
    if expected_workers is not None and manifest["workers"] != expected_workers:
        raise ValueError("checkpoint rank count mismatch")
    if (manifest["hidden"] % manifest["heads"]
            or manifest["heads"] % manifest["workers"]
            or manifest["inner"] % manifest["workers"]):
        raise ValueError("checkpoint heads/MLP are not divisible")
    if qwen2:
        kv = manifest.get("kv_heads")
        dim = manifest.get("head_dim")
        if (type(kv) is not int or type(dim) is not int
                or not 1 <= kv <= manifest["heads"]
                or not 1 <= dim <= 256 or dim % 2
                or manifest["heads"] % kv or kv % manifest["workers"]
                or dim * manifest["heads"] != manifest["hidden"]):
            raise ValueError("invalid Qwen2 GQA/TP head dimensions")
        eps = manifest.get("rms_eps")
        theta = manifest.get("rope_theta")
        if (type(eps) not in (float, int) or not math.isfinite(eps)
                or not 0 < eps <= 0.1):
            raise ValueError("invalid RMS epsilon")
        if (type(theta) not in (float, int) or not math.isfinite(theta)
                or not 1 < theta <= 1e12):
            raise ValueError("invalid RoPE theta")
        if type(manifest.get("tied_embeddings")) is not bool:
            raise ValueError("invalid tied-embedding option")
        if manifest["format"] == "opencompute-qwen2-tp-v2":
            rows = manifest.get("io_chunk_rows")
            if type(rows) is not int or not 1 <= rows <= manifest["vocab"]:
                raise ValueError("invalid Qwen2 vocabulary chunk rows")
            count = (manifest["vocab"] + rows - 1) // rows
            file_count = (2 + manifest["workers"] * manifest["layers"]
                          + count * (1 if manifest["tied_embeddings"] else 2))
            if count > 128 or file_count > 512:
                raise ValueError("too many Qwen2 vocabulary/rank chunk files")
    else:
        eps = manifest.get("layer_norm_epsilon")
        if type(eps) not in (float, int) or not math.isfinite(eps) or not 0 < eps <= 0.1:
            raise ValueError("invalid layer normalization epsilon")
        if manifest.get("activation") != "gelu_new":
            raise ValueError("unsupported activation")
    source = manifest.get("source")
    if not isinstance(source, str) or not 1 <= len(source) <= 1024:
        raise ValueError("invalid checkpoint source")
    return manifest


def expected_files(workers):
    if isinstance(workers, dict):
        validate_manifest(workers)
        if workers["format"] == "opencompute-qwen2-tp-v2":
            from compute.qwen2_chunked import all_files
            return all_files(workers)
        workers = workers["workers"]
    if type(workers) is not int or not 1 <= workers <= 4:
        raise ValueError("invalid checkpoint rank count")
    return ["manifest.json", "rank0_io.pt"] + [
        f"rank_{rank}.pt" for rank in range(workers)]


def inspect_file(path, *, max_size=MAX_FILE_BYTES):
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        raise ValueError("checkpoint must be a regular, non-symlink file")
    size = path.stat().st_size
    if not 0 < size <= max_size:
        raise ValueError("checkpoint file is empty or too large")
    checksum = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            checksum.update(block)
    if path.stat().st_size != size:
        raise ValueError("checkpoint file changed during inspection")
    return {"name": path.name, "size": size, "sha256": checksum.hexdigest()}


def inspect_bundle(folder, manifest, *, expected=None):
    validate_manifest(manifest)
    folder = Path(folder)
    names = expected_files(manifest)
    indexes = {}
    for name in names:
        cap = MAX_MANIFEST_BYTES if name == "manifest.json" else MAX_FILE_BYTES
        indexes[name] = inspect_file(folder / name, max_size=cap)
    actual = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
    if actual != manifest:
        raise ValueError("stored manifest differs from expected model configuration")
    if expected is not None:
        if set(expected) != set(indexes):
            raise ValueError("historical checkpoint file set does not match")
        for name, index in indexes.items():
            previous = expected[name]
            if (index["size"] != previous["size"]
                    or index["sha256"] != previous["sha256"]):
                raise ValueError(f"checkpoint integrity mismatch: {name}")
    return indexes
