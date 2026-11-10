"""End-to-end trusted localhost Qwen2 HTTP Worker registration, downloads and Gloo.

All processes run on the same localhost machine. No cloud or WAN is involved.
"""
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import tempfile
import time

import requests
from compute.qwen2_reference import generate_dense
from compute.qwen2_shards import synthetic_state
from compute.qwen2_chunked import export_chunked


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def run(timeout=60):
    root = Path(__file__).resolve().parents[1]
    admin, join = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
    with tempfile.TemporaryDirectory(prefix="opencompute-qwen2-v043-stream-") as temporary:
        data = Path(temporary)
        state, config = synthetic_state(
            hidden=24, heads=6, kv_heads=2, layers=2, inner=48,
            vocab=64, positions=64)
        checkpoint_name = "qwen2-chunked-fixture"
        checkpoint_root = data / "checkpoints"
        checkpoint_root.mkdir()
        export_chunked(state, config, checkpoint_root / checkpoint_name,
                       workers=2, chunk_rows=13, source="chunked-offline-fixture")
        port = free_port()
        url = f"http://127.0.0.1:{port}"
        env = os.environ.copy()
        env.update({
            "OPENCOMPUTE_ADMIN_KEY": admin,
            "OPENCOMPUTE_JOIN_KEY": join,
            "OPENCOMPUTE_DB_PATH": str(data / "cluster.sqlite"),
            "OPENCOMPUTE_TP_ROOT": str(data / "models"),
            "OPENCOMPUTE_TP_CHECKPOINTS": str(data / "checkpoints"),
            "OMP_NUM_THREADS": "1",
        })
        server = subprocess.Popen([
            sys.executable, "-m", "uvicorn", "cluster_v031.local_server:app",
            "--host", "127.0.0.1", "--port", str(port),
            "--log-level", "error",
        ], cwd=root, env=env, stdout=subprocess.PIPE,
           stderr=subprocess.STDOUT, text=True)
        workers = []
        try:
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                try:
                    if requests.get(url + "/health", timeout=0.5).status_code == 200:
                        break
                except requests.RequestException:
                    pass
                time.sleep(0.1)
            else:
                raise RuntimeError("local server did not start")
            for rank in range(2):
                workers.append(subprocess.Popen([
                    sys.executable, "-m", "cluster_v031.worker", "--url", url,
                    "--name", f"tp-local-{rank}",
                    "--cache", str(data / f"worker-{rank}"),
                    "--once",
                ], cwd=root, env=env, stdout=subprocess.PIPE,
                   stderr=subprocess.STDOUT, text=True))
            while time.monotonic() < deadline:
                status = requests.get(url + "/v0.3/status",
                                      headers={"X-Admin-Key": admin},
                                      timeout=1).json()
                if len(status["nodes"]) >= 2:
                    break
                time.sleep(0.2)
            else:
                raise RuntimeError("not enough registered TP Workers")
            created = requests.post(url + "/v0.4/sessions",
                                    headers={"X-Admin-Key": admin},
                                    timeout=20, json={
                                        "model_id": "qwen2-local-checkpoint",
                                        "checkpoint": checkpoint_name,
                                        "prompt_ids": [1, 2, 3],
                                        "max_new_tokens": 3,
                                    })
            created.raise_for_status()
            session_id = created.json()["session"]["session_id"]
            while time.monotonic() < deadline:
                response = requests.get(
                    url + f"/v0.3.1/sessions/{session_id}",
                    headers={"X-Admin-Key": admin}, timeout=2)
                response.raise_for_status()
                status = response.json()
                if status["status"] in ("completed", "failed"):
                    break
                time.sleep(0.2)
            else:
                raise RuntimeError("TP inference session timed out")
            if status["status"] != "completed":
                raise RuntimeError(f"TP inference failed: {status['error']}")
            for worker in workers:
                worker.wait(timeout=6)
                if worker.returncode:
                    raise RuntimeError("Worker exited nonzero: " + worker.stdout.read()[-800:])
            ranks = {entry["rank"]: entry for entry in status["assigned"]}
            if (0 not in ranks or not ranks[0]["result"]
                    or len(ranks[0]["result"]["generated_ids"]) != 3):
                raise RuntimeError("missing rank zero generated IDs")
            expected = generate_dense(state, config, [1, 2, 3], 3)["generated_ids"]
            if ranks[0]["result"]["generated_ids"] != expected:
                raise AssertionError("managed TP result differs from dense reference")
            return {
                "status": status["status"], "workers": len(workers),
                "generated_ids": ranks[0]["result"]["generated_ids"],
                "ranks": [{"rank": row["rank"], "state": row["state"]}
                          for row in status["assigned"]],
                "model": status["model"]["source"],
                "engine": status["engine"],
                "format": status["model"]["format"],
                "scope": "single-host HTTP agents + actual CPU Gloo",
            }
        finally:
            for process in workers + [server]:
                if process.poll() is None:
                    process.terminate()
            for process in workers + [server]:
                try:
                    process.communicate(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.communicate()


if __name__ == "__main__":
    print(json.dumps(run(), indent=2))
