"""Run trusted local Qwen2 Safetensors on a single NVIDIA CUDA device."""
import argparse
import json
import time

import torch

from compute.qwen2_reference import generate_single_device
from compute.qwen2_safetensors import inspect_directory


def run(model_dir, prompt_ids, max_new_tokens, *, device="cuda:0", expected_sha256=None):
    if not 1 <= len(prompt_ids) <= 512 or not 1 <= max_new_tokens <= 64:
        raise ValueError("expected 1–512 prompt tokens and 1–64 new tokens")

    device = torch.device(device)
    if device.type == "cuda":
        if not torch.cuda.is_available():
            raise ValueError("CUDA device unavailable; install CUDA-enabled PyTorch")
        index = torch.cuda.current_device() if device.index is None else device.index
        if index >= torch.cuda.device_count():
            raise ValueError("selected CUDA device is not visible to PyTorch")
        device = torch.device("cuda", index)
    elif device != torch.device("cpu"):
        raise ValueError("device must be cpu or cuda[:index]")

    inspected = inspect_directory(model_dir, workers=1, expected_sha256=expected_sha256)
    meta, plan = inspected["manifest"], inspected["plan"]
    if device.type == "cuda":
        weight_bytes = plan["rank_shard_estimated_bytes_fp32"] + plan["rank0_io_estimated_bytes_fp32"]
        if meta["tied_embeddings"]:
            weight_bytes -= 4 * meta["hidden"] * meta["vocab"]
        context = len(prompt_ids) + max_new_tokens
        kv_bytes = 8 * meta["layers"] * meta["kv_heads"] * meta["head_dim"] * context
        attention_bytes = 16 * meta["heads"] * context**2
        reserve = weight_bytes + kv_bytes + attention_bytes + 256 * 1024**2
        free, _ = torch.cuda.mem_get_info(device)
        if free < reserve:
            raise ValueError(f"insufficient free CUDA memory (estimated {(reserve + 1024**2 - 1) // 1024**2} MiB)")

    started = time.perf_counter()
    result = generate_single_device(inspected["weights"], meta, prompt_ids,
                                    max_new_tokens, device=device)
    return {
        "device": str(device),
        "generated_ids": result["generated_ids"],
        "load_and_generate_seconds": round(time.perf_counter() - started, 3),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", required=True, help="trusted local Qwen2 model directory")
    parser.add_argument("--ids", required=True, type=int, nargs="+", help="prompt token IDs")
    parser.add_argument("--new-tokens", type=int, default=16)
    parser.add_argument("--device", default="cuda:0", help="CUDA device or explicit CPU reference")
    parser.add_argument("--expected-sha256", help="optional SHA-256 of single-file Safetensors")
    args = parser.parse_args()
    try:
        result = run(args.model, args.ids, args.new_tokens, device=args.device,
                     expected_sha256=args.expected_sha256)
    except (OSError, ValueError, RuntimeError) as exc:
        parser.exit(1, f"Qwen2 inference failed: {exc}\n")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
