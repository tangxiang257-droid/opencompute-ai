"""Authenticated GPU inventory and task scheduling API for OpenCompute AI v0.7."""
import hmac
from typing import Literal

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field, model_validator

from cluster_v03.store import LeaseConflict
from cluster_v07.scheduler import GPUScheduler


class GPUDevice(BaseModel):
    index: int = Field(ge=0, le=15)
    uuid: str = Field(min_length=1, max_length=128)
    name: str = Field(min_length=1, max_length=128)
    vendor: Literal["nvidia", "amd", "intel", "unknown"]
    backend: Literal["cuda", "rocm", "xpu", "unknown"]
    total_memory_mb: int = Field(ge=1, le=1048576)
    free_memory_mb: int = Field(ge=0, le=1048576)
    utilization_pct: float = Field(ge=0, le=100)
    compute_capability: str | None = Field(default=None, max_length=16)
    supported_dtypes: list[Literal["fp32", "fp16", "bf16", "tf32", "int8", "int4"]] = Field(
        default_factory=list, max_length=6
    )
    runtime: Literal["unknown", "pytorch_cuda"] = "unknown"
    runtime_version: str | None = Field(default=None, max_length=32)

    @model_validator(mode="after")
    def validate_memory(self):
        if self.free_memory_mb > self.total_memory_mb:
            raise ValueError("free GPU memory cannot exceed total GPU memory")
        return self


class GPUInventoryReport(BaseModel):
    devices: list[GPUDevice] = Field(max_length=16)


class GPUTaskRequest(BaseModel):
    kind: Literal["matmul_checksum"] = "matmul_checksum"
    payload: dict
    required_vram_mb: int = Field(default=256, ge=64, le=1048576)
    required_vendor: Literal["nvidia"] = "nvidia"
    required_backend: Literal["cuda"] = "cuda"
    max_attempts: int = Field(default=3, ge=1, le=5)


class GPUCompletion(BaseModel):
    node_id: str = Field(min_length=1, max_length=64)
    gpu_index: int = Field(ge=0, le=15)
    lease_token: str = Field(min_length=1, max_length=128)
    result: dict = Field(default_factory=dict)
    error: str | None = Field(default=None, max_length=500)


def create_router(store, *, admin_key):
    router = APIRouter(prefix="/v0.7", tags=["GPU pool"])
    scheduler = GPUScheduler(store)

    def check_admin(received):
        if len(admin_key) < 16:
            raise HTTPException(status_code=503, detail="admin authentication not configured")
        if not received or not hmac.compare_digest(admin_key, received):
            raise HTTPException(status_code=401, detail="invalid admin credentials")

    def task_operation(operation, task_id):
        try:
            return operation(task_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="GPU task not found") from exc
        except LeaseConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @router.post("/nodes/{node_id}/gpus")
    def report(node_id: str, body: GPUInventoryReport,
               x_node_token: str = Header(default="")):
        try:
            return store.report_gpu_inventory(
                node_id, x_node_token,
                [device.model_dump() for device in body.devices],
            )
        except PermissionError as exc:
            raise HTTPException(status_code=401, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @router.get("/gpus")
    def inventory(x_admin_key: str = Header(default="")):
        check_admin(x_admin_key)
        return store.gpu_status()

    @router.post("/gpu-tasks")
    def submit_gpu_task(body: GPUTaskRequest, x_admin_key: str = Header(default="")):
        check_admin(x_admin_key)
        try:
            return scheduler.submit(**body.model_dump())
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @router.get("/gpu-tasks")
    def list_gpu_tasks(x_admin_key: str = Header(default="")):
        check_admin(x_admin_key)
        return scheduler.status()

    @router.get("/gpu-tasks/{task_id}")
    def gpu_task_detail(task_id: str, x_admin_key: str = Header(default="")):
        check_admin(x_admin_key)
        return task_operation(scheduler.detail, task_id)

    @router.post("/gpu-tasks/{task_id}/cancel")
    def cancel_gpu_task(task_id: str, x_admin_key: str = Header(default="")):
        check_admin(x_admin_key)
        return task_operation(scheduler.cancel, task_id)

    @router.post("/gpu-tasks/{task_id}/retry")
    def retry_gpu_task(task_id: str, x_admin_key: str = Header(default="")):
        check_admin(x_admin_key)
        return task_operation(scheduler.retry, task_id)

    @router.post("/nodes/{node_id}/gpu-claim")
    def claim_gpu_task(node_id: str, x_node_token: str = Header(default="")):
        try:
            return scheduler.claim(node_id, x_node_token)
        except PermissionError as exc:
            raise HTTPException(status_code=401, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @router.post("/gpu-tasks/{task_id}/complete")
    def complete_gpu_task(task_id: str, body: GPUCompletion,
                          x_node_token: str = Header(default="")):
        try:
            return scheduler.complete(
                body.node_id, x_node_token, task_id, body.gpu_index,
                body.lease_token, body.result, error=body.error,
            )
        except PermissionError as exc:
            raise HTTPException(status_code=401, detail=str(exc)) from exc
        except LeaseConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    return router
