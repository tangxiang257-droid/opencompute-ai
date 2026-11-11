"""Bounded, anonymous admission endpoints for the public verification pool."""
import json
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from public_pool.store import AdmissionError, PublicPool


class StrictRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class ChallengeRequest(StrictRequest):
    resource: Literal["cpu", "cuda"]


class Challenge(ChallengeRequest):
    seed: str = Field(pattern=r"^[0-9a-f]{32}$", max_length=32)
    expires: int
    signature: str = Field(pattern=r"^[0-9a-f]{64}$", max_length=64)


class Resources(StrictRequest):
    cores: int = Field(ge=1, le=1024)
    ram_mb: int = Field(ge=1, le=4194304)
    vram_mb: int = Field(ge=0, le=1048576)


class JoinRequest(StrictRequest):
    challenge: Challenge
    resources: Resources
    checksum: int = Field(ge=0, le=100000000)


class LeaveRequest(StrictRequest):
    node_id: str = Field(pattern=r"^[0-9a-f]{32}$", max_length=32)


class HeartbeatRequest(StrictRequest):
    node_id: str = Field(pattern=r"^[0-9a-f]{32}$", max_length=32)
    seed: str = Field(pattern=r"^[0-9a-f]{32}$", max_length=32)
    checksum: int = Field(ge=0, le=100000000)


async def bounded_json(request: Request):
    if request.headers.get("content-type", "").partition(";")[0].strip().lower() != "application/json":
        raise HTTPException(415, "Use application/json")
    payload = bytearray()
    async for chunk in request.stream():
        if len(payload) + len(chunk) > 4096:
            raise HTTPException(413, "Public Worker requests are limited to 4096 bytes")
        payload.extend(chunk)
    try:
        return json.loads(payload)
    except (ValueError, RecursionError) as exc:
        raise HTTPException(422, "Invalid JSON request") from exc


def validated(model, data):
    try:
        return model.model_validate(data)
    except ValidationError as exc:
        raise HTTPException(422, "Invalid public Worker request") from exc


def create_router(store, *, enabled):
    router = APIRouter(prefix="/v0.8/public", tags=["Public Worker pool"])
    pool = PublicPool(store) if enabled else None

    def require_pool():
        if pool is None:
            raise HTTPException(503, "Public Worker admission is disabled")
        return pool

    def invoke(operation, *args):
        try:
            return operation(*args)
        except AdmissionError as exc:
            raise HTTPException(exc.status, str(exc)) from exc

    @router.get("/status")
    def status():
        if pool is None:
            return {"enabled": False}
        return {"enabled": True, **pool.status()}

    @router.get("/worker.py", include_in_schema=False)
    def download_worker(_pool=Depends(require_pool)):
        return FileResponse(Path(__file__).with_name("worker.py"), media_type="text/plain",
                            filename="opencompute-worker.py")

    @router.post("/challenge")
    def challenge(http: Request, data=Depends(bounded_json), active_pool=Depends(require_pool)):
        request = validated(ChallengeRequest, data)
        return invoke(active_pool.challenge, request.resource, http.client.host if http.client else None)

    @router.post("/join")
    def join(http: Request, data=Depends(bounded_json), active_pool=Depends(require_pool)):
        request = validated(JoinRequest, data)
        return invoke(active_pool.join, request.challenge.model_dump(),
                      request.resources.model_dump(), request.checksum,
                      http.client.host if http.client else None)

    @router.post("/heartbeat")
    def heartbeat(data=Depends(bounded_json), active_pool=Depends(require_pool),
                  authorization: str = Header(default="")):
        request = validated(HeartbeatRequest, data)
        scheme, _, token = authorization.partition(" ")
        return invoke(active_pool.heartbeat, request.node_id,
                      token if scheme.lower() == "bearer" else "", request.seed, request.checksum)

    @router.post("/leave")
    def leave(data=Depends(bounded_json), active_pool=Depends(require_pool),
              authorization: str = Header(default="")):
        """Opt-in CPU browser verification nodes can immediately leave."""
        request = validated(LeaveRequest, data)
        scheme, _, token = authorization.partition(" ")
        return invoke(active_pool.leave, request.node_id,
                      token if scheme.lower() == "bearer" else "")

    return router
