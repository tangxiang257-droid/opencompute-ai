"""Local-only benchmark API. Do not expose without authentication and isolation."""
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from cluster_v03.api import create_router as create_cluster_router
from cluster_v03.store import ClusterStore
from cluster_v031.api import create_router as create_tp_router
from cluster_v031.coordinator import SessionCoordinator
from pydantic import BaseModel, Field
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

from compute.shards import prepare
from compute.topology_planner import plan

app = FastAPI(title="OpenCompute AI CPU research API")
cluster_db = ClusterStore(os.getenv("OPENCOMPUTE_DB_PATH", "./.opencompute/cluster.sqlite"))
tp_sessions = SessionCoordinator(
    cluster_db, os.getenv("OPENCOMPUTE_TP_ROOT", "./.opencompute/tp-models"),
    checkpoint_root=os.getenv("OPENCOMPUTE_TP_CHECKPOINTS", "./.opencompute/checkpoints"))
app.include_router(create_tp_router(tp_sessions,
    admin_key=os.getenv("OPENCOMPUTE_ADMIN_KEY", "")))
app.include_router(create_cluster_router(
    cluster_db,
    admin_key=os.getenv("OPENCOMPUTE_ADMIN_KEY", ""),
    join_key=os.getenv("OPENCOMPUTE_JOIN_KEY", ""),
))

@app.get("/v0.3/console", include_in_schema=False)
def cluster_console():
    """Local browser UI; protected cluster data is loaded via admin-key requests."""
    return FileResponse(
        Path(__file__).resolve().parent.parent / "cluster_v03" / "console.html",
        media_type="text/html"
    )



class Benchmark(BaseModel):
    workers: int = Field(default=2, ge=1, le=4)
    hidden: int = Field(default=512, ge=64, le=2048)
    batch: int = Field(default=16, ge=1, le=128)


@app.get("/health")
def health():
    return {"status": "ok", "version": "0.3.1"}


@app.post("/benchmarks")
def benchmark(payload: Benchmark):
    if payload.hidden % payload.workers:
        raise HTTPException(status_code=422, detail="hidden must be divisible by workers")
    with tempfile.TemporaryDirectory(prefix="opencompute-shards-") as tmp:
        prepare(tmp, payload.hidden, payload.workers)
        command = [sys.executable, "-m", "torch.distributed.run", "--standalone",
                   f"--nproc-per-node={payload.workers}", "-m", "compute.sharded_demo",
                   "--shards", tmp, "--batch", str(payload.batch), "--iterations", "3"]
        try:
            result = subprocess.run(command, capture_output=True, text=True,
                                    timeout=120, env={**os.environ, "OMP_NUM_THREADS": "1"})
        except subprocess.TimeoutExpired:
            raise HTTPException(status_code=504, detail="benchmark timed out")
        if result.returncode:
            raise HTTPException(status_code=500, detail="benchmark failed: " + result.stderr[-1000:])
        for line in reversed(result.stdout.splitlines()):
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                continue
        raise HTTPException(status_code=500, detail="benchmark produced no report")


class PlanRequest(BaseModel):
    hidden: int = Field(default=768, ge=1, le=16384)
    heads: int = Field(default=12, ge=1, le=256)
    layers: int = Field(default=12, ge=1, le=256)
    positions: int = Field(default=1024, ge=1, le=262144)
    inner: int = Field(default=3072, ge=1, le=65536)
    vocab: int = Field(default=50257, ge=1, le=500000)
    devices: int = Field(default=4, ge=1, le=32)
    memory_mib: float = Field(default=1024, gt=0, le=1048576)
    runtime_reserve_mib: float = Field(default=256, ge=0, le=1048576)
    prompt_tokens: int = Field(default=16, ge=1, le=262144)
    new_tokens: int = Field(default=16, ge=1, le=262144)
    latency_ms: float = Field(default=1, ge=0, le=60000)
    bandwidth_mbps: float = Field(default=100, gt=0, le=1000000)


@app.post("/v0.2/plan")
def plan_topology(payload: PlanRequest):
    """Dry-run topology estimates, not actual allocated workers."""
    try:
        return plan(
            {"hidden": payload.hidden, "heads": payload.heads,
             "layers": payload.layers, "positions": payload.positions,
             "inner": payload.inner, "vocab": payload.vocab},
            devices=payload.devices, memory_mib=payload.memory_mib,
            runtime_reserve_mib=payload.runtime_reserve_mib,
            prompt_tokens=payload.prompt_tokens, new_tokens=payload.new_tokens,
            latency_ms=payload.latency_ms, bandwidth_mbps=payload.bandwidth_mbps)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
