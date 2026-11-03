"""Generate deterministic rank-local row shards without constructing full weights.

The deterministic formula makes every weight reproducible by global row/column.
Production model shards should instead be produced offline from real checkpoints.
"""
import argparse
import json
from pathlib import Path
import torch


def weight_rows(start: int, end: int, hidden: int) -> torch.Tensor:
    rows = torch.arange(start, end, dtype=torch.float64).unsqueeze(1)
    cols = torch.arange(hidden, dtype=torch.float64).unsqueeze(0)
    return (torch.sin(rows * 0.013 + cols * 0.017) * 0.1).to(torch.float32)


def prepare(output: str, hidden: int, workers: int) -> dict:
    if hidden <= 0 or workers <= 0 or hidden % workers:
        raise ValueError("hidden must be positive and divisible by workers")
    root = Path(output)
    root.mkdir(parents=True, exist_ok=True)
    shard_rows = hidden // workers
    for rank in range(workers):
        start = rank * shard_rows
        shard = weight_rows(start, start + shard_rows, hidden)
        torch.save(shard, root / f"rank_{rank}.pt")
    manifest = {"hidden": hidden, "workers": workers, "dtype": "float32",
                "layout": "row", "shard_shape": [shard_rows, hidden]}
    (root / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="shards")
    parser.add_argument("--hidden", type=int, default=512)
    parser.add_argument("--workers", type=int, default=2)
    args = parser.parse_args()
    print(json.dumps(prepare(args.output, args.hidden, args.workers)))
