"""Trusted-localhost TP Worker: authenticated shard download, verification and Gloo.

No arbitrary command execution. Coordinators and model files must be trusted:
this is an experimental research agent, NOT safe for unknown internet peers.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import signal
import shutil
import subprocess
import sys
import threading
import time

import psutil
import requests


def _request(base, method, route, *, headers, json_body=None, timeout=15):
    response = requests.request(method, base.rstrip("/") + route, headers=headers,
                                json=json_body, timeout=timeout)
    response.raise_for_status()
    return response


def _file_digest(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as reader:
        for chunk in iter(lambda: reader.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download_and_verify(base, assignment, node_id, token, data_root):
    """Download only assigned rank files; check SHA256 and tensor shapes."""
    from compute.gpt2_generate import load_rank, read_manifest

    session_id, rank = assignment["session_id"], assignment["rank"]
    folder = Path(data_root).resolve() / session_id
    folder.mkdir(parents=True, exist_ok=True)
    required = {"manifest.json", f"rank_{rank}.pt"}
    if rank == 0:
        required.add("rank0_io.pt")
    if {entry["name"] for entry in assignment["files"]} != required:
        raise ValueError("unexpected manifest files")
    for entry in assignment["files"]:
        name = entry["name"]
        if not name.isascii() or name not in required or entry["size"] > 512_000_000:
            raise ValueError("unsafe model shard filename or size")
        target = folder / name
        if target.is_file() and _file_digest(target) == entry["sha256"]:
            continue
        endpoint = f"/v0.3.1/sessions/{session_id}/files/{name}"
        response = requests.get(base.rstrip("/") + endpoint,
                                params={"node_id": node_id},
                                headers={"X-Node-Token": token},
                                stream=True, timeout=60)
        response.raise_for_status()
        temporary = target.with_suffix(target.suffix + ".tmp")
        try:
            digest = hashlib.sha256()
            total = 0
            with temporary.open("wb") as writer:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        total += len(chunk)
                        if total > entry["size"]:
                            raise ValueError("oversize shard response")
                        digest.update(chunk)
                        writer.write(chunk)
            if total != entry["size"] or digest.hexdigest() != entry["sha256"]:
                raise ValueError("shard checksum or size mismatch")
            temporary.replace(target)
        finally:
            response.close()
            temporary.unlink(missing_ok=True)

    config = read_manifest(folder)
    if config["workers"] != assignment["workers"]:
        raise ValueError("worker count mismatch")
    # Warm disk cache and validate tensor shapes. Inference process reloads its
    # own shard later; weights are NOT kept resident across this boundary.
    layers, io = load_rank(folder, rank, config)
    del layers, io
    return folder


def execute_rank(folder, assignment, timeout=150, cancellation_check=None):
    """Run a fixed module and stop the process if the session is revoked.

    The parent agent continues heartbeat/control polling while the model runs.
    """
    rank, world = assignment["rank"], assignment["workers"]
    env = os.environ.copy()
    for name in ("OPENCOMPUTE_JOIN_KEY", "OPENCOMPUTE_ADMIN_KEY", "X_NODE_TOKEN"):
        env.pop(name, None)
    env.update({
        "RANK": str(rank), "WORLD_SIZE": str(world),
        "MASTER_ADDR": assignment["master_host"],
        "MASTER_PORT": str(assignment["master_port"]),
        "OMP_NUM_THREADS": "1",
    })
    command = [
        sys.executable, "-m", "compute.gpt2_generate",
        "--shards", str(folder),
        "--prompt-ids", ",".join(map(str, assignment["prompt_ids"])),
        "--max-new-tokens", str(assignment["max_new_tokens"]),
    ]
    process = subprocess.Popen(command, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True, env=env,
                               start_new_session=(os.name == "posix"))
    deadline = time.monotonic() + timeout
    def terminate_group():
        if process.poll() is None:
            try:
                if os.name == "posix":
                    os.killpg(process.pid, signal.SIGTERM)
                else:
                    process.terminate()
            except ProcessLookupError:
                return  # Already exited between poll and signal.
            try:
                process.communicate(timeout=3)
            except subprocess.TimeoutExpired:
                try:
                    if os.name == "posix":
                        os.killpg(process.pid, signal.SIGKILL)
                    else:
                        process.kill()
                except ProcessLookupError:
                    pass
                process.communicate()

    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"Gloo rank {rank} exceeded {timeout}s")
            try:
                stdout, stderr = process.communicate(timeout=min(1.0, remaining))
                break
            except subprocess.TimeoutExpired:
                if cancellation_check is not None and cancellation_check():
                    raise RuntimeError("session cancelled or no longer running")
        if process.returncode:
            raise RuntimeError(f"Gloo rank {rank} failed: {stderr[-700:]}")
    finally:
        terminate_group()
    if rank == 0:
        records = [json.loads(line) for line in stdout.splitlines()
                   if line.startswith('{"engine":')]
        if len(records) != 1:
            raise ValueError("rank 0 missing inference report")
        return {
            "generated_ids": records[0]["generated_ids"],
            "tokens_per_second_rank0": records[0]["tokens_per_second_rank0"],
            "weight_bytes_per_rank": records[0]["weight_bytes_per_rank"],
        }
    return {"rank": rank, "completed": True}


def run(*, base_url, join_key, name, cache_root, once=False, poll_seconds=0.5):
    if len(join_key) < 16:
        raise ValueError("join key must have >=16 characters")
    if not (base_url.startswith("http://127.0.0.1:")
            or base_url.startswith("http://localhost:")):
        raise ValueError("v0.3.1 auto-launch is deliberately localhost only")
    resources = psutil.virtual_memory()
    register = _request(
        base_url, "POST", "/v0.3/nodes/register",
        headers={"X-Join-Key": join_key},
        json_body={
            "name": name,
            "platform": platform.system().lower(),
            "cpu_cores": max(1, min(128, os.cpu_count() or 1)),
            "ram_mb": max(128, min(1048576, int(resources.total / 1048576))),
            "compute_score": 100, "slots": 1, "capabilities": ["tp_gpt2"],
        },
    ).json()
    node_id, token = register["id"], register["node_token"]
    headers = {"X-Node-Token": token}
    stop = threading.Event()

    def heartbeat_loop():
        while not stop.is_set():
            try:
                free = min(int(psutil.virtual_memory().available / 1048576), 1048576)
                _request(base_url, "POST", f"/v0.3/nodes/{node_id}/heartbeat",
                         headers=headers, json_body={
                             "free_ram_mb": free,
                             "cpu_load_pct": min(100, max(0, psutil.cpu_percent(interval=None))),
                         })
            except requests.RequestException as exc:
                print(f"heartbeat warning: {exc}", flush=True)
            stop.wait(2)

    heartbeat = threading.Thread(target=heartbeat_loop, daemon=True)
    heartbeat.start()
    warmed = set()
    try:
        while True:
            response = _request(base_url, "GET",
                                f"/v0.3.1/nodes/{node_id}/assignment",
                                headers=headers).json()
            job = response["assignment"]
            if not job:
                # A preparing/running session may have been cancelled/expired
                # while this agent was waiting for the other ranks.
                if warmed:
                    for old_id in warmed:
                        shutil.rmtree(Path(cache_root) / old_id, ignore_errors=True)
                    warmed.clear()
                    if once:
                        return
                time.sleep(poll_seconds)
                continue
            session_id = job["session_id"]
            if session_id not in warmed:
                try:
                    download_and_verify(base_url, job, node_id, token, cache_root)
                    _request(base_url, "POST",
                             f"/v0.3.1/sessions/{session_id}/ready",
                             headers=headers, json_body={"node_id": node_id})
                    warmed.add(session_id)
                    print(f"{name} ready {session_id[:8]}, rank={job['rank']}", flush=True)
                except Exception as exc:
                    print(f"prepare failed: {type(exc).__name__}: {exc}", flush=True)
                    raise
            if not job["run"]:
                time.sleep(poll_seconds)
                continue
            try:
                def revoked():
                    try:
                        current = _request(
                            base_url, "GET",
                            f"/v0.3.1/nodes/{node_id}/assignment",
                            headers=headers, timeout=5).json()["assignment"]
                        return (current is None or current["session_id"] != session_id
                                or not current["run"])
                    except requests.RequestException:
                        # Transient control-plane loss must not immediately kill
                        # a running process; the hard local timeout still applies.
                        return False

                report = execute_rank(Path(cache_root) / session_id, job,
                                      cancellation_check=revoked)
                error = None
            except Exception as exc:
                report = None
                error = f"{type(exc).__name__}: {str(exc)[:350]}"
            try:
                _request(base_url, "POST",
                         f"/v0.3.1/sessions/{session_id}/complete",
                         headers=headers,
                         json_body={"node_id": node_id, "result": report, "error": error})
            except requests.HTTPError as exc:
                if not (error and exc.response is not None
                        and exc.response.status_code == 409):
                    raise
                # Another rank failed / administrator cancelled the session.
                print(f"{name}: discarded late result for {session_id[:8]}", flush=True)
            print(f"{name} finished {session_id[:8]}: {error or 'ok'}", flush=True)
            warmed.remove(session_id)
            shutil.rmtree(Path(cache_root) / session_id, ignore_errors=True)
            if once:
                return
    finally:
        stop.set()
        heartbeat.join(timeout=3)


def main():
    parser = argparse.ArgumentParser(description="Trusted localhost GPT-2 TP Worker")
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--name", default=platform.node() or "tp-cpu")
    parser.add_argument("--cache", default="./.opencompute/tp-worker-cache")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    run(base_url=args.url, join_key=os.getenv("OPENCOMPUTE_JOIN_KEY", ""),
        name=args.name, cache_root=args.cache, once=args.once)


if __name__ == "__main__":
    main()
