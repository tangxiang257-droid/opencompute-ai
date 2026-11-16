"""Bounded, administrator-only operational diagnostics; no remote probes."""
import hmac
import sqlite3
import time

from fastapi import APIRouter, Header, HTTPException
from backend.version import VERSION


def create_router(store, qwen2_service, *, admin_key):
    router = APIRouter(prefix="/v0.6", tags=["operations"])
    started = time.monotonic()

    @router.get("/system")
    def system(x_admin_key: str = Header(default="")):
        if len(admin_key) < 16:
            raise HTTPException(503, "admin authentication not configured")
        if not x_admin_key or not hmac.compare_digest(admin_key, x_admin_key):
            raise HTTPException(401, "invalid admin credentials")
        report = {
            "version": VERSION,
            "uptime_seconds": round(time.monotonic() - started, 1),
            "state": "degraded", "database": "unavailable",
            "workers": None, "tasks": None,
            "qwen2": {"enabled": False, "running": False, "reason": "bundle_not_configured"},
        }
        if qwen2_service is not None:
            source = qwen2_service.status
            # Select fields rather than including executor errors or bundle paths.
            report["qwen2"] = {k: source[k] for k in (
                "enabled", "running", "queue_pending", "queue_capacity",
                "active_requests", "batches_completed", "batches_failed")}
        try:
            status = store.status()
        except sqlite3.Error:
            return report
        nodes = status["nodes"]
        online = [n for n in nodes if n["online"]]
        report.update({
            "database": "ok", "state": "ready" if online else "waiting_for_workers",
            "workers": {"registered": len(nodes), "online": len(online),
                        "cpu_cores": sum(n["cpu_cores"] for n in online),
                        "ram_mb": sum(n["ram_mb"] for n in online)},
            "tasks": status["counts"],
        })
        return report

    return router
