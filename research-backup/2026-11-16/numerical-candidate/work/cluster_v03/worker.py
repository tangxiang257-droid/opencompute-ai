"""Allowlisted CPU worker agent; no code execution / arbitrary shell commands.

Run only against a trusted private endpoint. A shared join key permits new
registrations and is not suitable for internet-exposed or untrusted peers.
"""
import argparse
import hashlib
import os
import platform
import time

import psutil
import requests

from cluster_v03.store import validate_payload
from cluster_v07 import qwen2_job
from cluster_v07.detection import detect_nvidia_gpus
from cluster_v07.scheduler import QWEN2_TASK
from cluster_v07.worker import detect_torch_cuda_runtime, map_gpu_inventory_to_cuda, execute_gpu_task


def execute_task(kind, payload):
    validate_payload(kind, payload)
    if kind == "sum":
        return {"value": sum(payload["numbers"])}
    if kind == "sha256":
        return {"digest": hashlib.sha256(payload["text"].encode("utf-8")).hexdigest()}
    if kind == "matmul_checksum":
        n = payload["size"]
        # Reproducible pure-Python CPU task with bounded O(n^3) work.
        a = [[(i + 3 * j) % 17 for j in range(n)] for i in range(n)]
        b = [[(2 * i + j) % 19 for j in range(n)] for i in range(n)]
        checksum = 0
        for i in range(n):
            for j in range(n):
                checksum += sum(a[i][k] * b[k][j] for k in range(n))
        return {"checksum": checksum, "size": n}
    raise ValueError("unsupported operation")


def calibrate_cpu_score():
    # Self-reported, untrusted and deliberately inexpensive scheduling hint.
    samples = []
    for _ in range(3):
        start = time.perf_counter()
        execute_task("matmul_checksum", {"size": 24})
        samples.append(time.perf_counter() - start)
    return round(1 / max(min(samples), 1e-4), 4)


def run(*, base_url, join_key, name, score=None, poll_seconds=2.0, once=False,
        qwen2_dir=None, qwen2_sha256=None):
    if not join_key or len(join_key) < 16:
        raise ValueError("OPENCOMPUTE_JOIN_KEY needs at least 16 characters")
    if not base_url.startswith(("http://127.0.0.1:", "http://localhost:", "http://opencompute:")):
        # This research agent can also work on a private network, but warns
        # about unencrypted transport. Production must use authenticated TLS.
        print("WARNING: use a trusted private network, ideally HTTPS with TLS", flush=True)
    model = None
    if qwen2_dir or qwen2_sha256:
        if not qwen2_dir or not qwen2_sha256:
            raise ValueError("Qwen2 Worker needs both a local model directory and weight SHA-256")
        if detect_torch_cuda_runtime()["runtime"] != "pytorch_cuda":
            raise ValueError("Qwen2 Worker needs a visible NVIDIA CUDA device and CUDA-enabled PyTorch")
        model = qwen2_job.configure(qwen2_dir, qwen2_sha256)
    session = requests.Session()

    def post(path, data=None, headers=None, *, allow_stale_completion=False, timeout=15):
        response = session.post(base_url.rstrip("/") + path,
                                json=data, headers=headers or {}, timeout=timeout)
        if allow_stale_completion and response.status_code == 409:
            return {"status": "discarded"}
        response.raise_for_status()
        return response.json()

    resources = psutil.virtual_memory()
    cores = os.cpu_count() or 1
    registration = post("/v0.3/nodes/register", {
        "name": name, "platform": platform.system().lower(),
        "cpu_cores": cores, "ram_mb": max(128, int(resources.total / 1048576)),
        "compute_score": score if score is not None else calibrate_cpu_score(),
        "slots": 1, "capabilities": ["sum", "sha256", "matmul_checksum"],
    }, {"X-Join-Key": join_key})
    node_id, node_token = registration["id"], registration["node_token"]
    key = {"X-Node-Token": node_token}
    print(f"registered {name} as {node_id[:8]} (score={score or 'auto'})", flush=True)

    def report_gpu_inventory():
        try:
            devices = detect_nvidia_gpus()
            runtime = detect_torch_cuda_runtime()
            devices = map_gpu_inventory_to_cuda(devices, runtime)
            if model:
                for gpu in devices:
                    if (gpu["runtime"] == "pytorch_cuda"
                            and gpu["total_memory_mb"] >= model["vram_mb"]):
                        gpu.update(qwen2_model_id=model["id"], qwen2_vram_mb=model["vram_mb"])
            result = post(f"/v0.7/nodes/{node_id}/gpus", {"devices": devices}, key)
            print(f"GPU inventory reported: {result.get('reported', 0)} device(s)", flush=True)
        except Exception as exc:
            # GPU reporting is optional; it must not take a healthy CPU Worker offline.
            print(f"GPU inventory unavailable: {type(exc).__name__}", flush=True)

    report_gpu_inventory()
    gpu_runtime = detect_torch_cuda_runtime()
    last_gpu_report = time.monotonic()
    def heartbeat(timeout=15):
        memory = psutil.virtual_memory()
        post(f"/v0.3/nodes/{node_id}/heartbeat", {
            "free_ram_mb": int(memory.available / 1048576),
            "cpu_load_pct": psutil.cpu_percent(interval=None),
        }, key, timeout=timeout)

    while True:
        heartbeat()
        if time.monotonic() - last_gpu_report >= 30:
            report_gpu_inventory()
            gpu_runtime = detect_torch_cuda_runtime()
            last_gpu_report = time.monotonic()
        job = post(f"/v0.3/nodes/{node_id}/claim", {}, key)["task"]
        if job:
            try:
                result = execute_task(job["kind"], job["payload"])
                error = None
            except Exception as exc:
                result, error = {}, f"execution failed: {type(exc).__name__}"
            completion = post(f"/v0.3/tasks/{job['id']}/complete", {
                "node_id": node_id, "lease_token": job["lease_token"],
                "result": result, "error": error,
            }, key, allow_stale_completion=True)
            print(f"processed {job['id'][:8]}: {completion.get('status') or error or 'completed'}", flush=True)
        if gpu_runtime["runtime"] == "pytorch_cuda":
            try:
                claim_started = time.monotonic()
                gpu_job = post(f"/v0.7/nodes/{node_id}/gpu-claim", {}, key)["task"]
                if gpu_job:
                    lease = {"node_id": node_id, "gpu_index": gpu_job["gpu_index"],
                             "lease_token": gpu_job["lease_token"]}
                    task_url = f"/v0.7/gpu-tasks/{gpu_job['id']}"

                    def pulse():
                        heartbeat(timeout=5)
                        if not post(task_url + "/lease", lease, key, timeout=5)["active"]:
                            raise InterruptedError("GPU task cancelled")

                    try:
                        if gpu_job["kind"] == QWEN2_TASK:
                            gpu_result = qwen2_job.execute(model, gpu_job,
                                                          claim_started=claim_started, pulse=pulse)
                        else:
                            gpu_result = execute_gpu_task(gpu_job)
                        gpu_error = None
                    except (TimeoutError, ValueError, InterruptedError) as exc:
                        gpu_result, gpu_error = {}, f"GPU execution failed: {exc}"[:500]
                    except Exception as exc:
                        gpu_result, gpu_error = {}, f"GPU execution failed: {type(exc).__name__}"
                    gpu_completion = post(task_url + "/complete", {
                        **lease, "result": gpu_result, "error": gpu_error,
                    }, key, allow_stale_completion=True)
                    print(f"GPU task {gpu_job['id'][:8]}: {gpu_completion.get('status') or gpu_error or 'completed'}", flush=True)
            except Exception as exc:
                # A missing/newer GPU API must not interrupt CPU-only operation.
                print(f"GPU scheduler unavailable: {type(exc).__name__}", flush=True)
        if once:
            return
        time.sleep(poll_seconds)


def main():
    p = argparse.ArgumentParser(description="OpenCompute v0.3 CPU worker")
    p.add_argument("--url", default=os.getenv("OPENCOMPUTE_API_URL", "http://127.0.0.1:8000"))
    p.add_argument("--name", default=os.getenv("OPENCOMPUTE_WORKER_NAME", platform.node() or "cpu-worker"))
    p.add_argument("--score", type=float, default=float(os.environ["OPENCOMPUTE_WORKER_SCORE"]) if os.getenv("OPENCOMPUTE_WORKER_SCORE") else None)
    p.add_argument("--qwen2-dir", default=os.getenv("OPENCOMPUTE_QWEN2_DIR"))
    p.add_argument("--qwen2-sha256", default=os.getenv("OPENCOMPUTE_QWEN2_SHA256"))
    p.add_argument("--once", action="store_true")
    a = p.parse_args()
    run(base_url=a.url, join_key=os.getenv("OPENCOMPUTE_JOIN_KEY", ""),
        name=a.name, score=a.score, once=a.once,
        qwen2_dir=a.qwen2_dir, qwen2_sha256=a.qwen2_sha256)


if __name__ == "__main__":
    main()
