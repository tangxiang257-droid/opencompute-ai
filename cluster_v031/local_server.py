"""Minimal localhost demo API; shared v0.3 / v0.3.1 auth and SQLite registry."""
import os
from fastapi import FastAPI
from cluster_v03.api import create_router as v03_router
from cluster_v03.store import ClusterStore
from cluster_v031.api import create_router as tp_router
from cluster_v031.coordinator import SessionCoordinator
from cluster_v04.api import create_model_router

store = ClusterStore(os.getenv("OPENCOMPUTE_DB_PATH", "./.opencompute/cluster.sqlite"))
coordinator = SessionCoordinator(
    store, os.getenv("OPENCOMPUTE_TP_ROOT", "./.opencompute/tp-models"),
    checkpoint_root=os.getenv("OPENCOMPUTE_TP_CHECKPOINTS", "./.opencompute/checkpoints"))
app = FastAPI(title="OpenCompute AI v0.4.1 localhost TP demo")
app.include_router(v03_router(
    store, admin_key=os.getenv("OPENCOMPUTE_ADMIN_KEY", ""),
    join_key=os.getenv("OPENCOMPUTE_JOIN_KEY", "")))
app.include_router(tp_router(
    coordinator, admin_key=os.getenv("OPENCOMPUTE_ADMIN_KEY", "")))
app.include_router(create_model_router(
    coordinator, admin_key=os.getenv("OPENCOMPUTE_ADMIN_KEY", "")))

@app.get("/health")
def health():
    return {"ok": True, "version": "0.4.1"}
