"""Run a configured Qwen2 model in a bounded Worker subprocess."""
import hashlib
import json
import math
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

from cluster_v07.scheduler import QWEN2_TASK, validate_gpu_task


class Qwen2RequestError(ValueError):
    """A task error safe to return to the scheduler."""


def model_id(directory, weight_sha256):
    digest = hashlib.sha256(bytes.fromhex(weight_sha256))
    for name in ("config.json", "tokenizer.json"):
        digest.update(hashlib.sha256((Path(directory) / name).read_bytes()).digest())
    return digest.hexdigest()


def configure(directory, weight_sha256):
    from tokenizers import Tokenizer

    from compute.qwen2_safetensors import inspect_directory
    from compute.qwen2_single_device import estimate_cuda_bytes

    directory = Path(directory).resolve(strict=True)
    inspected = inspect_directory(directory, workers=1, expected_sha256=weight_sha256)
    Tokenizer.from_file(str(directory / "tokenizer.json"))
    manifest = inspected["manifest"]
    memory = estimate_cuda_bytes(manifest, inspected["plan"], min(576, manifest["positions"] + 1))
    return {"directory": str(directory), "weight_sha256": weight_sha256,
            "id": model_id(directory, weight_sha256),
            "vram_mb": math.ceil(memory / 1024**2)}


def generate_text(model, payload, *, device, required_vram_mb):
    validate_gpu_task(QWEN2_TASK, payload, required_vram_mb)
    if (payload["model_id"] != model["id"]
            or model_id(model["directory"], model["weight_sha256"]) != model["id"]):
        raise Qwen2RequestError("configured Qwen2 model assets changed")
    if required_vram_mb < model["vram_mb"]:
        raise Qwen2RequestError("GPU task reservation is below the model requirement")

    import torch
    from tokenizers import Tokenizer

    from compute.qwen2_single_device import run

    tokenizer = Tokenizer.from_file(str(Path(model["directory"]) / "tokenizer.json"))
    ids = tokenizer.encode(payload["prompt"], add_special_tokens=False).ids
    if not 1 <= len(ids) <= 512:
        raise Qwen2RequestError("Qwen2 prompt must tokenize to 1..512 tokens")
    if device != "cpu":
        free, _ = torch.cuda.mem_get_info(torch.device(device))
        if free < required_vram_mb * 1024**2:
            raise Qwen2RequestError("actual free VRAM is below the task reservation")
    result = run(model["directory"], ids, payload["max_new_tokens"],
                 device=device, expected_sha256=model["weight_sha256"])
    result["text"] = tokenizer.decode(result["generated_ids"], skip_special_tokens=True)
    return result


def execute(model, job, *, claim_started, pulse):
    if model is None or model["id"] != job["payload"]["model_id"]:
        raise ValueError("Qwen2 model is not configured on this Worker")
    deadline = claim_started + min(100, job["lease_seconds"] - 5)
    message = json.dumps({"model": model, "payload": job["payload"],
                          "device": f"cuda:{job['gpu_index']}",
                          "required_vram_mb": job["required_vram_mb"], "deadline": deadline})
    env = {key: value for key, value in os.environ.items()
           if key not in ("OPENCOMPUTE_JOIN_KEY", "OPENCOMPUTE_ADMIN_KEY")}
    with subprocess.Popen([sys.executable, "-m", "cluster_v07.qwen2_job"], stdin=subprocess.PIPE,
                          stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
                          encoding="utf-8", env=env) as child:
        try:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Qwen2 inference exceeded its GPU lease budget")
                try:
                    output, _ = child.communicate(message, timeout=min(5, remaining))
                    break
                except subprocess.TimeoutExpired:
                    message = None
                    pulse()
        finally:
            if child.poll() is None:
                child.kill()
                child.communicate()
    if child.returncode == 124:
        raise TimeoutError("Qwen2 inference exceeded its GPU lease budget")
    if child.returncode:
        raise RuntimeError("Qwen2 Worker could not run the configured model")
    response = json.loads(output)
    if "error" in response:
        raise ValueError(response["error"])
    return response["result"]


def main():
    request = json.load(sys.stdin)
    remaining = request.pop("deadline") - time.monotonic()
    if remaining <= 0:
        return 124
    timer = threading.Timer(remaining, os._exit, args=(124,))
    timer.daemon = True
    timer.start()
    try:
        print(json.dumps({"result": generate_text(**request)}))
    except Qwen2RequestError as exc:
        print(json.dumps({"error": str(exc)}))
    finally:
        timer.cancel()
    return 0


if __name__ == "__main__":
    sys.exit(main())
