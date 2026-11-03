"""Trusted-localhost TP Worker: authenticated shard download, verification and Gloo.

No arbitrary command execution. Coordinators and model files must be trusted:
this is an experimental research agent, NOT safe for unknown internet peers.
"""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import signal
import shutil
import subprocess
import sys
import threading
import time

import psutil
import requests

from cluster_v03.worker import calibrate_cpu_score
from cluster_v031.limits import process_tree_rss_bytes


def _memory_mb():
    """Best-effort host/cgroup memory capacity, never overstate container RAM."""
    mem = psutil.virtual_memory()
    total, available = int(mem.total), int(mem.available)
    # Linux cgroup v2: Docker may report host RAM via psutil.
    try:
        limit_text = Path("/sys/fs/cgroup/memory.max").read_text().strip()
        current_text = Path("/sys/fs/cgroup/memory.current").read_text().strip()
        if limit_text.isdigit() and current_text.isdigit():
            limit, current = int(limit_text), int(current_text)
            if limit > 0:
                total = min(total, limit)
                available = min(available, max(0, limit - current))
    except (OSError, ValueError):
        pass
    total_mib = total // (1024 * 1024)
    if total_mib < 128:
        raise RuntimeError("Worker does not have the minimum 128 MiB memory")
    return (min(1048576, total_mib),
            max(0, min(1048576, available // (1024 * 1024))))


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
    if not isinstance(session_id, str) or not re.fullmatch(r"[0-9a-f]{32}", session_id):
        raise ValueError("invalid session identifier")
    if (type(rank) is not int or type(assignment["workers"]) is not int
            or not 1 <= assignment["workers"] <= 4
            or not 0 <= rank < assignment["workers"]):
        raise ValueError("invalid shard rank assignment")
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


def execute_rank(folder, assignment, timeout=150, cancellation_check=None,
                 rss_limit_mib=None):
    """Run a fixed module and stop the process if the session is revoked.

    The parent agent continues heartbeat/control polling while the model runs.
    """
    rank, world = assignment["rank"], assignment["workers"]
    if os.name != "posix":
        raise RuntimeError("v0.3.4 CPU time isolation requires POSIX")
    if not 1 <= timeout <= 3600:
        raise ValueError("invalid local timeout")
    if rss_limit_mib is not None:
        if type(rss_limit_mib) is not int or not 512 <= rss_limit_mib <= 1048576:
            raise ValueError("invalid worker RSS reservation")
    vmem = os.getenv("OPENCOMPUTE_TP_MAX_VMEM_MIB", "").strip()
    if vmem and (not vmem.isdecimal() or not 512 <= int(vmem) <= 1048576):
        raise ValueError("invalid optional virtual memory limit")
    if (type(rank) is not int or type(world) is not int or
            not 1 <= world <= 4 or not 0 <= rank < world):
        raise ValueError("invalid rank or world size")
    if assignment["master_host"] not in ("127.0.0.1", "localhost"):
        raise ValueError("v0.3.2 only allows trusted localhost rendezvous")
    if (type(assignment["master_port"]) is not int
            or not 1 <= assignment["master_port"] <= 65535):
        raise ValueError("invalid rendezvous port")
    env = os.environ.copy()
    for name in ("OPENCOMPUTE_JOIN_KEY", "OPENCOMPUTE_ADMIN_KEY", "X_NODE_TOKEN"):
        env.pop(name, None)
    env.update({
        "RANK": str(rank), "WORLD_SIZE": str(world),
        "MASTER_ADDR": assignment["master_host"],
        "MASTER_PORT": str(assignment["master_port"]),
        "OPENCOMPUTE_EXTERNAL_STORE": "1",
        "OMP_NUM_THREADS": "1",
    })
    # Limits are applied by the child *before* importing PyTorch; no unsafe
    # preexec_fn is used while the agent heartbeat thread is running.
    command = [
        sys.executable, "-m", "cluster_v031.limits",
        "--cpu-seconds", str(min(3600, math.ceil(timeout) + 30)),
    ]
    if vmem:
        command += ["--max-vmem-mib", vmem]
    command += [
        "--", "--shards", str(folder),
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
                if rss_limit_mib is not None:
                    memory = process_tree_rss_bytes(process.pid)
                    if memory is not None and memory > rss_limit_mib * 1024 * 1024:
                        raise MemoryError(
                            f"Gloo rank {rank} exceeded {rss_limit_mib} MiB RSS reservation")
                    if memory is None and os.getenv("OPENCOMPUTE_TP_REQUIRE_RSS_MONITOR") == "1":
                        raise RuntimeError("rank RSS cannot be monitored; strict mode rejects run")
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
    total_ram_mb, _ = _memory_mb()
    compute_score = max(0.01, min(1e6, calibrate_cpu_score()))
    register = _request(
        base_url, "POST", "/v0.3/nodes/register",
        headers={"X-Join-Key": join_key},
        json_body={
            "name": name,
            "platform": platform.system().lower(),
            "cpu_cores": max(1, min(128, os.cpu_count() or 1)),
            "ram_mb": total_ram_mb,
            "compute_score": compute_score, "slots": 1, "capabilities": ["tp_gpt2"],
        },
    ).json()
    node_id, token = register["id"], register["node_token"]
    headers = {"X-Node-Token": token}

    def acknowledge_stopped(session_id):
        """Release failed-session reservations only after this rank is no longer running."""
        try:
            _request(base_url, "POST",
                     f"/v0.3.1/sessions/{session_id}/stopped",
                     headers=headers, json_body={"node_id": node_id})
        except requests.RequestException as exc:
            print(f"stop acknowledgement warning: {exc}", flush=True)

    stop = threading.Event()

    def heartbeat_loop():
        while not stop.is_set():
            try:
                _, free = _memory_mb()
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
                        acknowledge_stopped(old_id)
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
                    # Fail the entire process group promptly instead of waiting
                    # for the coordinator to notice a missing heartbeat.
                    try:
                        _request(base_url, "POST",
                                 f"/v0.3.1/sessions/{session_id}/complete",
                                 headers=headers,
                                 json_body={"node_id": node_id, "result": None,
                                            "error": f"prepare: {type(exc).__name__}: {str(exc)[:300]}"})
                    except requests.RequestException as reporting_error:
                        print(f"failed to report prepare error: {reporting_error}", flush=True)
                    acknowledge_stopped(session_id)
                    shutil.rmtree(Path(cache_root) / session_id, ignore_errors=True)
                    if once:
                        return
                    time.sleep(poll_seconds)
                    continue
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
                                      cancellation_check=revoked,
                                      rss_limit_mib=job.get("reserved_memory_mb"))
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
                if exc.response is None or exc.response.status_code != 409:
                    raise
                # A peer may have failed AFTER this rank computed successfully.
                # Acknowledge local termination regardless of success/error.
                acknowledge_stopped(session_id)
                print(f"{name}: discarded late result for {session_id[:8]}", flush=True)
            else:
                if error:
                    acknowledge_stopped(session_id)
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
