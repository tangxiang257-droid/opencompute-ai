"""HTTP adapter for the bounded live Qwen2 two-stage pipeline queue."""
import hmac
import re

from fastapi import APIRouter, Header, HTTPException
from typing import Annotated

from pydantic import BaseModel, Field, StrictInt

from compute.request_queue_v049 import QueueFullError
from cluster_v05.qwen2_service import Qwen2PipelineService


_CLIENT_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}\Z")


TokenId = Annotated[StrictInt, Field(ge=0, le=2_147_483_647)]


class GenerateRequest(BaseModel):
    prompt_ids: list[TokenId] = Field(min_length=1, max_length=128)


def create_router(service: Qwen2PipelineService | None, *, admin_key: str):
    router = APIRouter(prefix="/v0.5", tags=["live Qwen2 pipeline queue"])

    def admin(received: str) -> None:
        if not admin_key or len(admin_key) < 16:
            raise HTTPException(503, "admin key not configured")
        if not received or not hmac.compare_digest(received, admin_key):
            raise HTTPException(401, "invalid admin credentials")

    @router.post("/requests")
    async def generate(
        body: GenerateRequest,
        x_admin_key: str = Header(default=""),
        x_client_id: str = Header(default=""),
    ):
        admin(x_admin_key)
        if service is None:
            raise HTTPException(
                503, "Qwen2 PP queue is disabled; configure OPENCOMPUTE_QWEN2_PP_BUNDLE"
            )
        if not _CLIENT_ID.fullmatch(x_client_id):
            raise HTTPException(422, "X-Client-ID must be 1..64 safe ASCII characters")
        try:
            return await service.submit(x_client_id, body.prompt_ids)
        except QueueFullError as exc:
            raise HTTPException(429, str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(504, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(503, str(exc)) from exc

    @router.get("/status")
    def status(x_admin_key: str = Header(default="")):
        admin(x_admin_key)
        if service is None:
            return {"enabled": False, "running": False, "reason": "bundle_not_configured"}
        return service.status

    return router
