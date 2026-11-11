"""Standalone OpenCompute public Worker. CPU mode needs Python 3.9 or later."""
import argparse
import json
import os
import sys
import time
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


MATRIX_SIZE = 48
API = "/v0.8/public"


def matrix_parameters(seed):
    values = bytes.fromhex(seed)
    return values[0] % 30 + 1, values[1], values[2] % 30 + 1, values[3]


def cpu_checksum(seed):
    a, b, c, d = matrix_parameters(seed)
    n = MATRIX_SIZE
    left = [[((i * n + k) * a + b) % 31 for k in range(n)] for i in range(n)]
    columns = [[((k * n + j) * c + d) % 31 for k in range(n)] for j in range(n)]
    return sum(sum(x * y for x, y in zip(row, col)) for row in left for col in columns)


def cuda_checksum(seed, device):
    import torch

    a, b, c, d = matrix_parameters(seed)
    grid = torch.arange(MATRIX_SIZE**2, device=device, dtype=torch.int32)
    left = ((grid * a + b) % 31).reshape(MATRIX_SIZE, MATRIX_SIZE).float()
    right = ((grid * c + d) % 31).reshape(MATRIX_SIZE, MATRIX_SIZE).float()
    return int((left @ right).sum(dtype=torch.float64).item())


def cuda_inventory(device, node_id):
    """Report one CUDA-visible GPU; telemetry remains worker-reported, not attested."""
    import torch

    properties = torch.cuda.get_device_properties(device)
    free_bytes, _ = torch.cuda.mem_get_info(device)
    major, minor = int(properties.major), int(properties.minor)
    raw_uuid = getattr(properties, "uuid", None)
    if isinstance(raw_uuid, bytes):
        raw_uuid = raw_uuid.hex()
    identifier = str(raw_uuid) if raw_uuid else f"public-{node_id}-cuda0"
    try:
        utilization = float(torch.cuda.utilization(device))
    except Exception:
        utilization = 0.0
    dtypes = ["fp32", "fp16"]
    if major >= 8:
        dtypes.extend(("bf16", "tf32"))
    return {
        "index": 0, "uuid": identifier[:128],
        "name": str(torch.cuda.get_device_name(device))[:128],
        "vendor": "nvidia", "backend": "cuda",
        "total_memory_mb": max(1, int(properties.total_memory) // 1024**2),
        "free_memory_mb": min(int(properties.total_memory) // 1024**2,
                              max(0, free_bytes // 1024**2)),
        "utilization_pct": max(0.0, min(100.0, utilization)),
        "compute_capability": f"{major}.{minor}",
        "supported_dtypes": dtypes,
        "runtime": "pytorch_cuda",
        "runtime_version": str(torch.__version__)[:32],
    }


def execute_gpu_task(job, device):
    """Execute only the bounded matmul checksum allowlist; never user-supplied code."""
    import math
    import torch

    if not isinstance(job, dict) or job.get("kind") != "matmul_checksum":
        raise ValueError("public CUDA Worker supports only matmul_checksum tasks")
    payload = job.get("payload")
    if not isinstance(payload, dict) or set(payload) != {"size"}:
        raise ValueError("invalid public GPU task payload")
    size = payload["size"]
    if type(size) is not int or not 16 <= size <= 512:
        raise ValueError("GPU matrix size must be in range 16..512")
    required_vram_mb = job.get("required_vram_mb")
    estimated = 64 + math.ceil(3 * size * size * 4 / (1024 * 1024))
    if type(required_vram_mb) is not int or required_vram_mb < estimated:
        raise ValueError("GPU task VRAM reservation is below the workload estimate")
    free_bytes, _ = torch.cuda.mem_get_info(device)
    if free_bytes < required_vram_mb * 1024**2:
        raise RuntimeError("actual free VRAM is below the task reservation")
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
        return {
            "operation": "cuda_matmul_checksum", "size": size,
            "gpu_index": 0, "gpu_name": torch.cuda.get_device_name(device),
            "checksum": checksum,
            "elapsed_ms": round((time.perf_counter() - started) * 1000, 3),
        }
    finally:
        del values, left, right, product
        torch.cuda.empty_cache()


def memory_mb():
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        class MemoryStatus(ctypes.Structure):
            _fields_ = [
                ("length", wintypes.DWORD), ("load", wintypes.DWORD),
                ("physical_total", ctypes.c_uint64), ("physical_free", ctypes.c_uint64),
                ("page_total", ctypes.c_uint64), ("page_free", ctypes.c_uint64),
                ("virtual_total", ctypes.c_uint64), ("virtual_free", ctypes.c_uint64),
                ("extended_virtual_free", ctypes.c_uint64),
            ]

        status = MemoryStatus()
        status.length = ctypes.sizeof(status)
        if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            raise RuntimeError("Windows could not report physical memory")
        return status.physical_total // 1024**2
    if sys.platform == "darwin":
        import subprocess

        return int(subprocess.check_output(["sysctl", "-n", "hw.memsize"], timeout=3)) // 1024**2
    return os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE") // 1024**2


def validate_url(raw):
    url = urlsplit(raw)
    if (not url.hostname or url.username is not None or url.password is not None
            or url.path not in ("", "/") or url.query or url.fragment):
        raise ValueError("Use only the OpenCompute site origin, such as https://example.com")
    if url.scheme != "https" and not (
        url.scheme == "http" and url.hostname in ("localhost", "127.0.0.1", "::1")
    ):
        raise ValueError("Public connections require HTTPS")
    return f"{url.scheme}://{url.netloc}"


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def run(origin, resource):
    resources = {"cores": os.cpu_count() or 1, "ram_mb": memory_mb(), "vram_mb": 0}
    checksum = cpu_checksum
    if resource == "cuda":
        import torch

        if not torch.version.cuda or not torch.cuda.is_available():
            raise RuntimeError("CUDA mode needs NVIDIA GPU access and CUDA-enabled PyTorch")
        device = torch.device("cuda:0")
        resources["vram_mb"] = torch.cuda.get_device_properties(device).total_memory // 1024**2
        def checksum(seed):
            return cuda_checksum(seed, device)

    opener = build_opener(NoRedirect())

    def post(path, body, token=None, node_token=None):
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        if node_token:
            headers["X-Node-Token"] = node_token
        target = origin + (path if path.startswith("/v0.") else API + path)
        request = Request(target, json.dumps(body).encode(), headers)
        with opener.open(request, timeout=10) as response:
            return json.loads(response.read(4097))

    while True:
        try:
            challenge = post("/challenge", {"resource": resource})
            worker = post("/join", {"challenge": challenge, "resources": resources,
                                    "checksum": checksum(challenge["seed"])})
        except HTTPError as exc:
            if exc.code != 429:
                try:
                    message = json.loads(exc.read(4096)).get("detail", "Request rejected")
                except (ValueError, AttributeError):
                    message = "Request rejected"
                raise RuntimeError(f"Admission failed (HTTP {exc.code}): {message}") from None
            print("Admission is busy. Retrying in 20 seconds.", flush=True)
            time.sleep(20)
            continue
        except OSError:
            print("Unable to reach OpenCompute. Retrying in 20 seconds.", flush=True)
            time.sleep(20)
            continue

        print(f"Admitted to the public {resource.upper()} pool. Keep this window open.", flush=True)
        seed = worker["seed"]
        device = None
        if resource == "cuda":
            device = torch.device("cuda:0")
            try:
                post(f"/v0.7/nodes/{worker['node_id']}/gpus",
                     {"devices": [cuda_inventory(device, worker["node_id"])]},
                     node_token=worker["token"])
            except HTTPError as exc:
                raise RuntimeError(f"GPU inventory report failed (HTTP {exc.code}).") from None

        next_heartbeat = time.monotonic() + worker["heartbeat_seconds"]
        next_gpu_report = time.monotonic() + 30 if resource == "cuda" else 0
        while True:
            now = time.monotonic()
            if now >= next_heartbeat:
                try:
                    reply = post("/heartbeat", {"node_id": worker["node_id"], "seed": seed,
                                                "checksum": checksum(seed)}, worker["token"])
                except HTTPError as exc:
                    if exc.code == 429:
                        next_heartbeat = time.monotonic() + 5
                    elif exc.code in (401, 409):
                        print("Public session expired. Reconnecting.", flush=True)
                        break
                    else:
                        raise RuntimeError(f"Public Worker stopped (HTTP {exc.code}).") from None
                except OSError:
                    print("Connection interrupted. Retrying.", flush=True)
                    next_heartbeat = time.monotonic() + 5
                else:
                    seed = reply["seed"]
                    next_heartbeat = time.monotonic() + worker["heartbeat_seconds"]
                    print("Verification accepted.", flush=True)

            if resource == "cuda" and now >= next_gpu_report:
                try:
                    post(f"/v0.7/nodes/{worker['node_id']}/gpus",
                         {"devices": [cuda_inventory(device, worker["node_id"])]},
                         node_token=worker["token"])
                    next_gpu_report = time.monotonic() + 30
                except HTTPError as exc:
                    if exc.code in (401, 409):
                        print("Public GPU credentials expired. Reconnecting.", flush=True)
                        break
                    raise RuntimeError(f"GPU inventory refresh failed (HTTP {exc.code}).") from None
                except OSError:
                    print("GPU inventory refresh temporarily failed.", flush=True)
                    next_gpu_report = time.monotonic() + 5

            if resource == "cuda":
                try:
                    claimed = post(f"/v0.7/nodes/{worker['node_id']}/gpu-claim",
                                   {}, node_token=worker["token"])
                except HTTPError as exc:
                    if exc.code in (401, 409):
                        print("Public GPU session expired. Reconnecting.", flush=True)
                        break
                    if exc.code != 429:
                        raise RuntimeError(f"GPU task claim failed (HTTP {exc.code}).") from None
                    claimed = {"task": None}
                except OSError:
                    print("GPU scheduler temporarily unreachable.", flush=True)
                    claimed = {"task": None}
                task = claimed.get("task")
                if task is not None:
                    error, result = None, {}
                    try:
                        result = execute_gpu_task(task, device)
                    except (ValueError, RuntimeError, ImportError) as exc:
                        error = str(exc)[:500]
                    try:
                        post(f"/v0.7/gpu-tasks/{task['id']}/complete", {
                            "node_id": worker["node_id"],
                            "gpu_index": task["gpu_index"],
                            "lease_token": task["lease_token"],
                            "result": result,
                            "error": error,
                        }, node_token=worker["token"])
                        outcome = "failed" if error else "completed"
                        print(f"GPU task {task['id'][:8]} {outcome}.", flush=True)
                    except HTTPError as exc:
                        if exc.code == 401:
                            print("Public GPU credentials expired. Reconnecting.", flush=True)
                            break
                        if exc.code != 409:
                            raise RuntimeError(f"GPU task completion failed (HTTP {exc.code}).") from None
                        print("GPU task lease expired before completion.", flush=True)
                    except OSError:
                        print("GPU task result could not be delivered; lease will expire safely.", flush=True)
                time.sleep(3)
            else:
                time.sleep(max(1, min(5, next_heartbeat - time.monotonic())))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True, help="OpenCompute website origin")
    parser.add_argument("--device", choices=("cpu", "cuda"), default="cpu")
    args = parser.parse_args()
    try:
        run(validate_url(args.url), args.device)
    except KeyboardInterrupt:
        print("\nWorker stopped.")
    except (ImportError, ValueError, RuntimeError, OSError) as exc:
        parser.exit(1, f"{exc}\n")


if __name__ == "__main__":
    main()
