"""Strict, immutable control-plane records. Layer/token ranges are half-open."""

from enum import StrEnum
from typing import Annotated, Literal
from pydantic import BaseModel, ConfigDict, Field, model_validator

Identifier = Annotated[
    str, Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
]
Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
Positive = Annotated[int, Field(ge=1, le=2**31 - 1)]
Index = Annotated[int, Field(ge=0, le=2**31 - 1)]


class State(StrEnum):
    QUEUED = "queued"
    PREPARING = "preparing"
    RUNNING = "running"
    VERIFYING = "verifying"
    INTERRUPTED = "interrupted"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


TERMINAL = frozenset({State.COMPLETED, State.FAILED, State.CANCELLED})
TRANSITIONS = {
    State.QUEUED: frozenset({State.PREPARING, State.FAILED, State.CANCELLED}),
    State.PREPARING: frozenset(
        {State.RUNNING, State.INTERRUPTED, State.FAILED, State.CANCELLED}
    ),
    State.VERIFYING: frozenset(
        {State.COMPLETED, State.FAILED, State.CANCELLED, State.INTERRUPTED}
    ),
    State.RUNNING: frozenset(
        {State.VERIFYING, State.INTERRUPTED, State.FAILED, State.CANCELLED}
    ),
    State.INTERRUPTED: frozenset({State.FAILED, State.CANCELLED}),
    **{state: frozenset() for state in TERMINAL},
}


class Record(BaseModel):
    model_config = ConfigDict(
        strict=True,
        frozen=True,
        extra="forbid",
        allow_inf_nan=False,
        revalidate_instances="always",
    )


class Epoch(Record):
    value: Positive


class ResourceLimits(Record):
    memory_bytes: Annotated[int, Field(ge=1, le=2**50)] = 1073741824
    max_inflight_tiles: Annotated[int, Field(ge=1, le=4096)] = 4
    cpu_fraction: Annotated[float, Field(gt=0, le=1)] = 1.0
    network_bytes_per_second: Annotated[int, Field(ge=1, le=2**40)] = 1048576


class Task(Record):
    task_id: Identifier
    owner_id: Identifier
    model_hash: Digest
    total_layers: Annotated[int, Field(ge=1, le=65536)]
    payload_ref: Annotated[str, Field(min_length=1, max_length=2048)]
    privacy_level: Literal["L1", "L2", "L3"] = "L1"
    state: State = State.QUEUED
    cost_cap: Annotated[float, Field(ge=0, le=1e12)] = 0.0
    deadline: Annotated[float, Field(gt=0, le=1e15)] = 1e15
    created_at: Annotated[float, Field(ge=0, le=1e15)] = 0.0
    resource_limits: ResourceLimits = Field(default_factory=ResourceLimits)


class Session(Record):
    session_id: Identifier
    task_id: Identifier
    owner_id: Identifier
    epoch: Positive
    model_hash: Digest
    layout_hash: Digest
    expected_tokens: Positive
    selected_nodes: Annotated[
        tuple[Identifier, ...], Field(min_length=1, max_length=1024)
    ]
    state: State = State.PREPARING

    @model_validator(mode="after")
    def unique_nodes(self):
        if len(set(self.selected_nodes)) != len(self.selected_nodes):
            raise ValueError("duplicate nodes")
        return self


class Stage(Record):
    session_id: Identifier
    stage_id: Index
    layer_start: Index
    layer_end: Positive
    model_hash: Digest
    layout_hash: Digest
    assigned_node: Identifier

    @model_validator(mode="after")
    def ordered(self):
        if self.layer_end <= self.layer_start:
            raise ValueError("empty/reversed layer range")
        return self


class Tile(Record):
    model_hash: Digest
    layout_hash: Digest
    session_id: Identifier
    epoch: Positive
    stage_id: Index
    tile_id: Index
    token_start: Index
    token_count: Annotated[int, Field(ge=1, le=1048576)]
    dtype: Literal["float16", "bfloat16", "float32"]
    shape: Annotated[tuple[Positive, ...], Field(min_length=1, max_length=8)]
    dependency_state: Literal["computed"] = "computed"

    @model_validator(mode="after")
    def bounded_tensor(self):
        size = 1
        for dim in self.shape:
            size *= dim
        if size > 2**31 or self.token_start + self.token_count > 2**31 - 1:
            raise ValueError("tensor/token range too large")
        return self
