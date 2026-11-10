"""Local research API: cluster coordination and optional live Qwen2 PP queue."""
import os

from fastapi import FastAPI

from cluster_v03.api import create_router as v03_router
from cluster_v03.store import ClusterStore
from cluster_v031.api import create_router as tp_router
from cluster_v031.coordinator import SessionCoordinator
from cluster_v04.api import create_model_router
from cluster_v05.api import create_router as qwen2_queue_router
from cluster_v05.qwen2_service import Qwen2PipelineService


def _int_env(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


store = ClusterStore(os.getenv("OPENCOMPUTE_DB_PATH", "./.opencompute/cluster.sqlite"))
coordinator = SessionCoordinator(
    store,
    os.getenv("OPENCOMPUTE_TP_ROOT", "./.opencompute/tp-models"),
    checkpoint_root=os.getenv("OPENCOMPUTE_TP_CHECKPOINTS", "./.opencompute/checkpoints"),
)

bundle = os.getenv("OPENCOMPUTE_QWEN2_PP_BUNDLE")
qwen2_queue = (
    Qwen2PipelineService(
        bundle,
        max_pending=_int_env("OPENCOMPUTE_QWEN2_PP_MAX_PENDING", 128),
        max_pending_per_client=_int_env(
            "OPENCOMPUTE_QWEN2_PP_MAX_PENDING_PER_CLIENT", 16
        ),
        max_batch_size=_int_env("OPENCOMPUTE_QWEN2_PP_MAX_BATCH_SIZE", 4),
        batch_wait_ms=_int_env("OPENCOMPUTE_QWEN2_PP_BATCH_WAIT_MS", 12),
        max_new_tokens=_int_env("OPENCOMPUTE_QWEN2_PP_MAX_NEW_TOKENS", 3),
        max_kv_mib=_int_env("OPENCOMPUTE_QWEN2_PP_MAX_KV_MIB", 64),
        request_timeout_seconds=float(
            os.getenv("OPENCOMPUTE_QWEN2_PP_REQUEST_TIMEOUT_SECONDS", "120")
        ),
        execution_timeout_seconds=float(
            os.getenv("OPENCOMPUTE_QWEN2_PP_EXECUTION_TIMEOUT_SECONDS", "120")
        ),
    )
    if bundle
    else None
)

app = FastAPI(title="OpenCompute AI v0.5.0 localhost inference demo")
app.include_router(v03_router(
    store,
    admin_key=os.getenv("OPENCOMPUTE_ADMIN_KEY", ""),
    join_key=os.getenv("OPENCOMPUTE_JOIN_KEY", ""),
))
app.include_router(tp_router(
    coordinator, admin_key=os.getenv("OPENCOMPUTE_ADMIN_KEY", "")
))
app.include_router(create_model_router(
    coordinator, admin_key=os.getenv("OPENCOMPUTE_ADMIN_KEY", "")
))
app.include_router(qwen2_queue_router(
    qwen2_queue, admin_key=os.getenv("OPENCOMPUTE_ADMIN_KEY", "")
))

if qwen2_queue is not None:
    app.router.on_startup.append(qwen2_queue.start)
    app.router.on_shutdown.append(qwen2_queue.close)


@app.get("/health")
def health():
    return {
        "ok": True,
        "version": "0.5.0",
        "qwen2_pp_queue_enabled": qwen2_queue is not None,
    }
