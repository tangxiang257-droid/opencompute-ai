"""Private v0.3.1 orchestration HTTP API; separate admin and node credentials."""
import hmac

from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from cluster_v03.store import LeaseConflict
from cluster_v031.coordinator import SessionCoordinator


class SyntheticRequest(BaseModel):
    workers: int = Field(default=2, ge=1, le=4)
    hidden: int = Field(default=16, ge=8, le=128)
    heads: int = Field(default=4, ge=1, le=8)
    layers: int = Field(default=2, ge=1, le=4)
    prompt_ids: list[int] = Field(default=[1, 2, 3], min_length=1, max_length=32)
    max_new_tokens: int = Field(default=3, ge=1, le=16)


class CheckpointRequest(BaseModel):
    checkpoint: str = Field(min_length=1, max_length=64)
    prompt_ids: list[int] = Field(default=[1, 2, 3], min_length=1, max_length=32)
    max_new_tokens: int = Field(default=3, ge=1, le=16)


class PruneRequest(BaseModel):
    older_than_seconds: int = Field(default=86400, ge=60, le=31_536_000)


class NodeAction(BaseModel):
    node_id: str = Field(min_length=1, max_length=64)


class NodeCompletion(NodeAction):
    result: dict | None = None
    error: str | None = Field(default=None, max_length=500)


def create_router(coordinator: SessionCoordinator, *, admin_key: str):
    router = APIRouter(prefix="/v0.3.1", tags=["managed CPU TP research"])

    def admin(received):
        if not admin_key or len(admin_key) < 16:
            raise HTTPException(503, "admin key not configured")
        if not received or not hmac.compare_digest(received, admin_key):
            raise HTTPException(401, "invalid admin credentials")

    def handle(func, *args, **kwargs):
        try:
            return func(*args, **kwargs)
        except PermissionError as exc:
            raise HTTPException(401, str(exc)) from exc
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from exc
        except LeaseConflict as exc:
            raise HTTPException(409, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc

    @router.post("/sessions/synthetic")
    def create(body: SyntheticRequest, x_admin_key: str = Header(default="")):
        admin(x_admin_key)
        return handle(coordinator.create_synthetic, workers=body.workers,
                      hidden=body.hidden, heads=body.heads, layers=body.layers,
                      prompt_ids=body.prompt_ids, new_tokens=body.max_new_tokens)

    @router.post("/sessions/checkpoint")
    def create_checkpoint(body: CheckpointRequest, x_admin_key: str = Header(default="")):
        admin(x_admin_key)
        return handle(coordinator.create_checkpoint, checkpoint=body.checkpoint,
                      prompt_ids=body.prompt_ids, new_tokens=body.max_new_tokens)

    @router.get("/sessions/{session_id}")
    def status(session_id: str, x_admin_key: str = Header(default="")):
        admin(x_admin_key)
        return handle(coordinator.status, session_id)

    @router.post("/sessions/{session_id}/retry")
    def retry(session_id: str, x_admin_key: str = Header(default="")):
        admin(x_admin_key)
        return handle(coordinator.retry, session_id)

    @router.post("/sessions/{session_id}/cancel")
    def cancel(session_id: str, x_admin_key: str = Header(default="")):
        admin(x_admin_key)
        return handle(coordinator.cancel, session_id)

    @router.post("/sessions/prune")
    def prune(body: PruneRequest, x_admin_key: str = Header(default="")):
        admin(x_admin_key)
        return handle(coordinator.prune, body.older_than_seconds)

    @router.get("/nodes/{node_id}/assignment")
    def assignment(node_id: str, x_node_token: str = Header(default="")):
        return handle(coordinator.poll, node_id, x_node_token)

    @router.get("/sessions/{session_id}/files/{filename}")
    def shard(session_id: str, filename: str, node_id: str,
              x_node_token: str = Header(default="")):
        location = handle(coordinator.authorize_file, node_id, x_node_token,
                          session_id, filename)
        return FileResponse(location, media_type="application/octet-stream")

    @router.post("/sessions/{session_id}/ready")
    def ready(session_id: str, body: NodeAction,
              x_node_token: str = Header(default="")):
        return handle(coordinator.ready, body.node_id, x_node_token, session_id)

    @router.post("/sessions/{session_id}/stopped")
    def stopped(session_id: str, body: NodeAction,
                x_node_token: str = Header(default="")):
        return handle(coordinator.stopped, body.node_id, x_node_token, session_id)

    @router.post("/sessions/{session_id}/complete")
    def complete(session_id: str, body: NodeCompletion,
                 x_node_token: str = Header(default="")):
        return handle(coordinator.complete, body.node_id, x_node_token,
                      session_id, body.result, body.error)

    return router
