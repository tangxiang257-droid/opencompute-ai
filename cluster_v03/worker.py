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


def run(*, base_url, join_key, name, score=None, poll_seconds=2.0, once=False):
    if not join_key or len(join_key) < 16:
        raise ValueError("OPENCOMPUTE_JOIN_KEY needs at least 16 characters")
    if not base_url.startswith(("http://127.0.0.1:", "http://localhost:", "http://opencompute:")):
        # This research agent can also work on a private network, but warns
        # about unencrypted transport. Production must use authenticated TLS.
        print("WARNING: use a trusted private network, ideally HTTPS with TLS", flush=True)
    session = requests.Session()

    def post(path, data=None, headers=None):
        response = session.post(base_url.rstrip("/") + path,
                                json=data, headers=headers or {}, timeout=15)
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
    while True:
        memory = psutil.virtual_memory()
        post(f"/v0.3/nodes/{node_id}/heartbeat", {
            "free_ram_mb": int(memory.available / 1048576),
            "cpu_load_pct": psutil.cpu_percent(interval=None),
        }, key)
        job = post(f"/v0.3/nodes/{node_id}/claim", {}, key)["task"]
        if job:
            try:
                result = execute_task(job["kind"], job["payload"])
                error = None
            except Exception as exc:
                result, error = {}, f"execution failed: {type(exc).__name__}"
            post(f"/v0.3/tasks/{job['id']}/complete", {
                "node_id": node_id, "lease_token": job["lease_token"],
                "result": result, "error": error,
            }, key)
            print(f"processed {job['id'][:8]}: {error or 'completed'}", flush=True)
        if once:
            return
        time.sleep(poll_seconds)


def main():
    p = argparse.ArgumentParser(description="OpenCompute v0.3 CPU worker")
    p.add_argument("--url", default=os.getenv("OPENCOMPUTE_API_URL", "http://127.0.0.1:8000"))
    p.add_argument("--name", default=os.getenv("OPENCOMPUTE_WORKER_NAME", platform.node() or "cpu-worker"))
    p.add_argument("--score", type=float, default=float(os.environ["OPENCOMPUTE_WORKER_SCORE"]) if os.getenv("OPENCOMPUTE_WORKER_SCORE") else None)
    p.add_argument("--once", action="store_true")
    a = p.parse_args()
    run(base_url=a.url, join_key=os.getenv("OPENCOMPUTE_JOIN_KEY", ""),
        name=a.name, score=a.score, once=a.once)


if __name__ == "__main__":
    main()
