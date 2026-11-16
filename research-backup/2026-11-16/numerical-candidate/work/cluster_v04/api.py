"""Model selection and explicit runtime dispatch for OpenCompute AI v0.4.

Only GPT-2 adapters are executable. Planned model families remain discoverable
but cannot be launched until dedicated inference engines actually exist.
"""
import hmac

from fastapi import APIRouter, Header, HTTPException, Query
from pydantic import BaseModel, Field

from cluster_v03.store import LeaseConflict
from cluster_v04.models import ModelUnavailable, list_models, model_by_id, require_runtime


class LaunchRequest(BaseModel):
    model_id: str = Field(min_length=1, max_length=96)
    mode: str = Field(default="tensor_parallel", min_length=1, max_length=48)
    checkpoint: str | None = Field(default=None, min_length=1, max_length=64)
    workers: int | None = Field(default=None, ge=1, le=4)
    hidden: int | None = Field(default=None, ge=8, le=128)
    heads: int | None = Field(default=None, ge=1, le=8)
    layers: int | None = Field(default=None, ge=1, le=4)
    prompt_ids: list[int] = Field(default_factory=lambda: [1, 2, 3],
                                  min_length=1, max_length=32)
    max_new_tokens: int = Field(default=3, ge=1, le=16)


def create_model_router(coordinator, *, admin_key: str):
    router = APIRouter(prefix="/v0.4", tags=["model registry & execution adapters"])

    def admin(received):
        if not admin_key or len(admin_key) < 16:
            raise HTTPException(503, "admin key not configured")
        if not received or not hmac.compare_digest(received, admin_key):
            raise HTTPException(401, "invalid admin credentials")

    @router.get("/models")
    def catalog(family: str | None = Query(default=None, max_length=64)):
        items = list_models(family=family)
        return {"count": len(items), "models": items,
                "note": "Catalog inclusion is NOT an inference-support claim; check capability."}

    @router.get("/models/{model_id}")
    def model_detail(model_id: str):
        try:
            return model_by_id(model_id).public()
        except KeyError as exc:
            raise HTTPException(404, "model not found") from exc

    @router.post("/sessions")
    def create_session(body: LaunchRequest, x_admin_key: str = Header(default="")):
        admin(x_admin_key)
        try:
            spec = require_runtime(body.model_id, body.mode)
            if spec.id == "gpt2-synthetic":
                if body.checkpoint is not None:
                    raise ValueError("synthetic model does not accept a checkpoint")
                output = coordinator.create_synthetic(
                    workers=body.workers or 2,
                    hidden=body.hidden or 16,
                    heads=body.heads or 4,
                    layers=body.layers or 2,
                    prompt_ids=body.prompt_ids,
                    new_tokens=body.max_new_tokens,
                )
            elif spec.id == "qwen2-synthetic":
                if body.checkpoint is not None:
                    raise ValueError("Qwen2 synthetic test model does not accept a checkpoint")
                output = coordinator.create_qwen2_synthetic(
                    workers=body.workers or 2,
                    hidden=body.hidden or 24,
                    heads=body.heads or 6,
                    layers=body.layers or 2,
                    prompt_ids=body.prompt_ids,
                    new_tokens=body.max_new_tokens,
                )
            elif spec.id in ("gpt2-local-checkpoint", "qwen2-local-checkpoint"):
                if not body.checkpoint:
                    raise ValueError("a pre-exported local checkpoint is required")
                if any(v is not None for v in (
                        body.workers, body.hidden, body.heads, body.layers)):
                    raise ValueError(
                        "checkpoint model dimensions/workers come from its manifest")
                output = coordinator.create_checkpoint(
                    checkpoint=body.checkpoint,
                    prompt_ids=body.prompt_ids,
                    new_tokens=body.max_new_tokens,
                    expected_engine=spec.engine,
                )
            else:
                # All runtime adapters must be registered explicitly.
                raise ModelUnavailable("no executable adapter registered")
        except KeyError as exc:
            raise HTTPException(404, "model not found") from exc
        except ModelUnavailable as exc:
            raise HTTPException(409, str(exc)) from exc
        except LeaseConflict as exc:
            raise HTTPException(409, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        return {"model_id": spec.id, "engine": spec.engine,
                "mode": body.mode, "session": output}

    return router
