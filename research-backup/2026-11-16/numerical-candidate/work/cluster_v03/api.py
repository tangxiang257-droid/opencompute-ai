"""Authenticated API for a private local/lan v0.3 research cluster."""
import hmac
from typing import Literal

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field

from cluster_v03.store import ClusterStore, LeaseConflict


class Registration(BaseModel):
    name: str = Field(min_length=1, max_length=64)
    platform: str = Field(default="unknown", min_length=1, max_length=64)
    cpu_cores: int = Field(ge=1, le=128)
    ram_mb: int = Field(ge=128, le=1048576)
    compute_score: float = Field(ge=0.01, le=1e6)
    slots: int = Field(default=1, ge=1, le=128)
    capabilities: list[Literal["sum", "sha256", "matmul_checksum", "tp_gpt2", "tp_qwen2"]] = Field(min_length=1, max_length=5)


class Heartbeat(BaseModel):
    free_ram_mb: int = Field(ge=0, le=1048576)
    cpu_load_pct: float = Field(ge=0, le=100)


class TaskRequest(BaseModel):
    kind: Literal["sum", "sha256", "matmul_checksum"]
    payload: dict
    required_cpu: int = Field(default=1, ge=1, le=128)
    required_memory_mb: int = Field(default=64, ge=1, le=1048576)
    max_attempts: int = Field(default=3, ge=1, le=5)


class Completion(BaseModel):
    node_id: str = Field(min_length=1, max_length=64)
    lease_token: str = Field(min_length=1, max_length=128)
    result: dict = Field(default_factory=dict)
    error: str | None = Field(default=None, max_length=500)


def create_router(store: ClusterStore, *, admin_key: str, join_key: str):
    router = APIRouter(prefix="/v0.3", tags=["cluster v0.3"])

    def check(secret, received):
        if not secret or len(secret) < 16:
            raise HTTPException(status_code=503, detail="cluster authentication not configured")
        if not received or not hmac.compare_digest(secret, received):
            raise HTTPException(status_code=401, detail="invalid credentials")

    @router.post("/nodes/register")
    def register(body: Registration, x_join_key: str = Header(default="")):
        check(join_key, x_join_key)
        try:
            return store.register(**body.model_dump())
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc

    @router.post("/nodes/{node_id}/heartbeat")
    def heartbeat(node_id: str, body: Heartbeat,
                  x_node_token: str = Header(default="")):
        try:
            return store.heartbeat(node_id, x_node_token, **body.model_dump())
        except PermissionError as exc:
            raise HTTPException(401, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc

    @router.post("/nodes/{node_id}/claim")
    def claim(node_id: str, x_node_token: str = Header(default="")):
        try:
            return store.claim(node_id, x_node_token)
        except PermissionError as exc:
            raise HTTPException(401, str(exc)) from exc
        except LeaseConflict as exc:
            raise HTTPException(409, str(exc)) from exc

    @router.post("/tasks")
    def submit(body: TaskRequest, x_admin_key: str = Header(default="")):
        check(admin_key, x_admin_key)
        try:
            return store.submit(**body.model_dump())
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc

    @router.post("/tasks/{task_id}/complete")
    def complete(task_id: str, body: Completion,
                 x_node_token: str = Header(default="")):
        try:
            return store.complete(body.node_id, x_node_token, task_id,
                                  body.lease_token, body.result, error=body.error)
        except PermissionError as exc:
            raise HTTPException(401, str(exc)) from exc
        except LeaseConflict as exc:
            raise HTTPException(409, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc

    @router.get("/status")
    def status(x_admin_key: str = Header(default="")):
        check(admin_key, x_admin_key)
        return store.status()

    def task_operation(operation, task_id):
        try:
            return operation(task_id)
        except KeyError as exc:
            raise HTTPException(404, "task not found") from exc
        except LeaseConflict as exc:
            raise HTTPException(409, str(exc)) from exc

    @router.get("/tasks/{task_id}")
    def detail(task_id: str, x_admin_key: str = Header(default="")):
        check(admin_key, x_admin_key)
        return task_operation(store.task_detail, task_id)

    @router.post("/tasks/{task_id}/cancel")
    def cancel(task_id: str, x_admin_key: str = Header(default="")):
        check(admin_key, x_admin_key)
        return task_operation(store.cancel, task_id)

    @router.post("/tasks/{task_id}/retry")
    def retry(task_id: str, x_admin_key: str = Header(default="")):
        check(admin_key, x_admin_key)
        return task_operation(store.retry, task_id)

    return router
