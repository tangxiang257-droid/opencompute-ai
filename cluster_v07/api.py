"""Authenticated GPU inventory API for OpenCompute AI v0.7."""
import hmac
from typing import Literal

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field, model_validator


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

    @model_validator(mode="after")
    def validate_memory(self):
        if self.free_memory_mb > self.total_memory_mb:
            raise ValueError("free GPU memory cannot exceed total GPU memory")
        return self


class GPUInventoryReport(BaseModel):
    devices: list[GPUDevice] = Field(max_length=16)


def create_router(store, *, admin_key):
    router = APIRouter(prefix="/v0.7", tags=["GPU pool"])

    def check_admin(received):
        if len(admin_key) < 16:
            raise HTTPException(status_code=503, detail="admin authentication not configured")
        if not received or not hmac.compare_digest(admin_key, received):
            raise HTTPException(status_code=401, detail="invalid admin credentials")

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

    return router
