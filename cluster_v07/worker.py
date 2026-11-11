"""Optional PyTorch CUDA runtime helpers for the allowlisted GPU Worker."""
import time

from cluster_v07.scheduler import validate_gpu_task


def detect_torch_cuda_runtime():
    """Describe logical CUDA devices available to this Worker process."""
    try:
        import torch
        if not torch.cuda.is_available():
            return {"runtime": "unknown", "runtime_version": None, "devices": []}
        visible = []
        for logical_index in range(torch.cuda.device_count()):
            properties = torch.cuda.get_device_properties(logical_index)
            raw_uuid = getattr(properties, "uuid", None)
            visible.append({
                "index": logical_index,
                "uuid": str(raw_uuid) if raw_uuid else None,
                "name": str(torch.cuda.get_device_name(logical_index))[:128],
            })
        if not visible:
            return {"runtime": "unknown", "runtime_version": None, "devices": []}
        return {
            "runtime": "pytorch_cuda",
            "runtime_version": str(torch.__version__)[:32],
            "devices": visible,
        }
    except Exception:
        return {"runtime": "unknown", "runtime_version": None, "devices": []}


def map_gpu_inventory_to_cuda(devices, runtime):
    """Map nvidia-smi physical inventory to PyTorch logical CUDA indices.

    Prefer UUID matching. Fall back to a unique model-name match only when it
    is unambiguous. Ambiguous devices remain visible only when their physical
    index does not collide with a mapped CUDA logical index.
    """
    visible = runtime.get("devices", []) if runtime.get("runtime") == "pytorch_cuda" else []
    version = runtime.get("runtime_version")
    uuid_to_index = {
        str(item["uuid"]): item["index"]
        for item in visible if item.get("uuid")
    }
    visible_name_counts = {}
    inventory_name_counts = {}
    for item in visible:
        visible_name_counts[item["name"]] = visible_name_counts.get(item["name"], 0) + 1
    for item in devices:
        inventory_name_counts[item["name"]] = inventory_name_counts.get(item["name"], 0) + 1

    mapped, unmapped, used_indices = [], [], set()
    for device in devices:
        logical_index = uuid_to_index.get(str(device.get("uuid", "")))
        if logical_index is None:
            name = device.get("name")
            if (name and visible_name_counts.get(name) == 1
                    and inventory_name_counts.get(name) == 1):
                match = next((item for item in visible if item["name"] == name), None)
                if match is not None:
                    logical_index = match["index"]
        if (type(logical_index) is int and 0 <= logical_index < 16
                and logical_index not in used_indices):
            item = dict(device)
            item["index"] = logical_index
            item["runtime"] = "pytorch_cuda"
            item["runtime_version"] = version
            mapped.append(item)
            used_indices.add(logical_index)
        else:
            unmapped.append(device)

    # Preserve non-CUDA-visible hardware telemetry only when its physical index
    # cannot collide with a mapped logical index or another reported device.
    for device in unmapped:
        physical_index = device.get("index")
        if (type(physical_index) is int and 0 <= physical_index < 16
                and physical_index not in used_indices):
            item = dict(device)
            item["runtime"] = "unknown"
            item["runtime_version"] = None
            mapped.append(item)
            used_indices.add(physical_index)
    return sorted(mapped, key=lambda item: item["index"])

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
