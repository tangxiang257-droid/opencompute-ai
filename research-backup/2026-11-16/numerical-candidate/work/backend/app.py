"""Local-only benchmark API. Do not expose without authentication and isolation."""
from fastapi import FastAPI, HTTPException, Header, Depends
from fastapi.responses import FileResponse
from cluster_v03.api import create_router as create_cluster_router
from cluster_v03.store import ClusterStore
from cluster_v07.api import create_router as create_gpu_router
from cluster_v031.api import create_router as create_tp_router
from cluster_v031.coordinator import SessionCoordinator
from cluster_v04.api import create_model_router
from cluster_v05.api import create_router as create_qwen2_queue_router
from cluster_v05.qwen2_service import Qwen2PipelineService
from backend.operations import create_router as create_operations_router
from backend.version import VERSION
from public_pool.api import create_router as create_public_pool_router
from pydantic import BaseModel, Field
import json
import hmac
import os
from pathlib import Path
import subprocess
import sys
import tempfile

from compute.shards import prepare
from compute.topology_planner import plan

app = FastAPI(title="OpenCompute AI operations & CPU research API", version=VERSION)
qwen2_queue = Qwen2PipelineService.from_env()

@app.middleware("http")
async def private_response_headers(request, call_next):
    response = await call_next(request)
    response.headers["Cache-Control"] = "no-store"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    return response


def require_admin(x_admin_key: str = Header(default="", alias="X-Admin-Key")):
    """Fail closed when a long independent admin secret was not configured."""
    secret = os.getenv("OPENCOMPUTE_ADMIN_KEY", "")
    if len(secret) < 16:
        raise HTTPException(status_code=503, detail="admin authentication not configured")
    if not x_admin_key or not hmac.compare_digest(secret, x_admin_key):
        raise HTTPException(status_code=401, detail="invalid admin credentials")

cluster_db = ClusterStore(os.getenv("OPENCOMPUTE_DB_PATH", "./.opencompute/cluster.sqlite"))
app.include_router(create_operations_router(cluster_db, qwen2_queue,
    admin_key=os.getenv("OPENCOMPUTE_ADMIN_KEY", "")))
tp_sessions = SessionCoordinator(
    cluster_db, os.getenv("OPENCOMPUTE_TP_ROOT", "./.opencompute/tp-models"),
    checkpoint_root=os.getenv("OPENCOMPUTE_TP_CHECKPOINTS", "./.opencompute/checkpoints"))
app.include_router(create_tp_router(tp_sessions,
    admin_key=os.getenv("OPENCOMPUTE_ADMIN_KEY", "")))
app.include_router(create_model_router(tp_sessions,
    admin_key=os.getenv("OPENCOMPUTE_ADMIN_KEY", "")))
app.include_router(create_qwen2_queue_router(
    qwen2_queue, admin_key=os.getenv("OPENCOMPUTE_ADMIN_KEY", "")
))
if qwen2_queue is not None:
    app.router.on_startup.append(qwen2_queue.start)
    app.router.on_shutdown.append(qwen2_queue.close)
app.include_router(create_cluster_router(
    cluster_db,
    admin_key=os.getenv("OPENCOMPUTE_ADMIN_KEY", ""),
    join_key=os.getenv("OPENCOMPUTE_JOIN_KEY", ""),
))
app.include_router(create_gpu_router(
    cluster_db, admin_key=os.getenv("OPENCOMPUTE_ADMIN_KEY", "")
))
app.include_router(create_public_pool_router(
    cluster_db, enabled=os.getenv("OPENCOMPUTE_PUBLIC_ADMISSION", "0") == "1",
))

@app.get("/v0.4/console", include_in_schema=False)
def model_console():
    """Model directory / experimental runner selection for trusted localhost."""
    return FileResponse(
        Path(__file__).resolve().parent.parent / "cluster_v04" / "console.html",
        media_type="text/html"
    )


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
    return {
        "status": "ok",
        "version": VERSION,
        "qwen2_pp_queue_enabled": qwen2_queue is not None,
    }


@app.post("/benchmarks")
def benchmark(payload: Benchmark, _: None = Depends(require_admin)):
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
def plan_topology(payload: PlanRequest, _: None = Depends(require_admin)):
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


# Browser dashboard (v0.5.1). The API remains same-origin; secrets stay in the browser session.
WEB_INDEX = Path(__file__).resolve().parent.parent / "web" / "index.html"

@app.get("/assets/dashboard.js", include_in_schema=False)
def dashboard_script():
    return FileResponse(WEB_INDEX.with_name("dashboard.js"), media_type="text/javascript")

@app.get("/", include_in_schema=False)
def dashboard():
    return FileResponse(WEB_INDEX, media_type="text/html")
