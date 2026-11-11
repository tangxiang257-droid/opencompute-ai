"""Optional PyTorch CUDA runtime helpers for the allowlisted GPU Worker."""
import time

from cluster_v07.scheduler import validate_gpu_task


def detect_torch_cuda_runtime():
    """Return a runtime marker only when this process can actually use CUDA."""
    try:
        import torch
        if torch.cuda.is_available():
            return {"runtime": "pytorch_cuda", "runtime_version": str(torch.__version__)[:32]}
    except Exception:
        pass
    return {"runtime": "unknown", "runtime_version": None}


def execute_gpu_task(job):
    """Execute the fixed, bounded GPU matrix checksum task; never executes user code."""
    import torch

    kind = job.get("kind")
    payload = job.get("payload")
    required_vram_mb = job.get("required_vram_mb")
    validate_gpu_task(kind, payload, required_vram_mb)
    gpu_index = job.get("gpu_index")
    if type(gpu_index) is not int or gpu_index < 0 or gpu_index >= torch.cuda.device_count():
        raise ValueError("assigned CUDA device is not available to this Worker")
    free_bytes, _ = torch.cuda.mem_get_info(gpu_index)
    if free_bytes < required_vram_mb * 1024 * 1024:
        raise RuntimeError("actual free VRAM is below the reserved task requirement")

    device = torch.device(f"cuda:{gpu_index}")
    size = payload["size"]
    torch.cuda.synchronize(device)
    started = time.perf_counter()
    values = left = right = product = None
    try:
        values = torch.arange(size * size, device=device, dtype=torch.float32)
        left = values.remainder(17).reshape(size, size)
        right = (values.remainder(19) + 1).reshape(size, size)
        product = torch.matmul(left, right)
        torch.cuda.synchronize(device)
        checksum = int(product.sum(dtype=torch.float64).item())
        elapsed_ms = round((time.perf_counter() - started) * 1000, 3)
        return {
            "operation": "cuda_matmul_checksum",
            "size": size,
            "gpu_index": gpu_index,
            "gpu_name": torch.cuda.get_device_name(gpu_index),
            "checksum": checksum,
            "elapsed_ms": elapsed_ms,
        }
    finally:
        del values, left, right, product
        torch.cuda.empty_cache()
