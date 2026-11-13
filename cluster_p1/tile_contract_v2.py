"""Explicit batch-one, two-stage prefill declarations; no database or execution.

This version does not reinterpret legacy Tile, migrate records, authenticate a
caller, verify tensor bytes, or issue work/credit/readiness capabilities.
"""
from dataclasses import dataclass, field
from typing import Annotated, Literal
from pydantic import Field, model_validator, field_validator
from compute.bounded_json import parse_json_object
from .contracts import Record, Identifier, Digest, Positive, Index, Task, Session, Stage, Tile

MAX_TILES_PER_STAGE = 64
MAX_RECORD_BYTES = 65536

class ContractRejected(ValueError):
    def __init__(self):super().__init__('unsupported prefill declarations')

class DescriptorV2(Record):
    role: Literal['token_ids', 'hidden_states', 'full_logits']
    dtype: Literal['int64', 'float32']
    shape: Annotated[tuple[Positive, ...], Field(min_length=2, max_length=3)]

    @model_validator(mode='after')
    def supported(self):
        if self.shape[0] != 1:
            raise ValueError('batch one required')
        if self.role == 'token_ids':
            if self.dtype != 'int64' or len(self.shape) != 2:
                raise ValueError('invalid token declaration')
        elif self.dtype != 'float32' or len(self.shape) != 3:
            raise ValueError('invalid floating declaration')
        size = 1
        for dim in self.shape:size *= dim
        if size > 2**31:
            raise ValueError('tensor element bound')
        return self

class TileV2(Record):
    schema_version: Literal[2]
    task_id: Identifier
    owner_id: Identifier
    session_id: Identifier
    epoch: Positive
    model_hash: Digest
    layout_hash: Digest
    stage_id: Annotated[int, Field(ge=0, le=1)]
    tile_id: Index
    token_start: Index
    token_count: Annotated[int, Field(ge=1, le=1048576)]
    input: DescriptorV2
    output: DescriptorV2

    @field_validator('schema_version', mode='before')
    @classmethod
    def explicit_version(cls, value):
        if type(value) is not int or value != 2:
            raise ValueError('explicit integer version required')
        return value

    @model_validator(mode='after')
    def token_axes_and_roles(self):
        if self.token_start + self.token_count > 2**31 - 1:
            raise ValueError('token range bound')
        if self.input.shape[1] != self.token_count or self.output.shape[1] != self.token_count:
            raise ValueError('token axes mismatch')
        roles = ('token_ids','hidden_states') if self.stage_id == 0 else ('hidden_states','full_logits')
        if (self.input.role, self.output.role) != roles:
            raise ValueError('stage role mismatch')
        return self

@dataclass(frozen=True, slots=True)
class PlanAssessment:
    tiles_per_stage: int
    status: str = field(default='declarations_consistent', init=False)
    execution_allowed: bool = field(default=False, init=False)
    payload_verified: bool = field(default=False, init=False)

@dataclass(frozen=True, slots=True)
class RecordAssessment:
    status: str
    automatic_migration_allowed: bool = field(default=False, init=False)
    execution_allowed: bool = field(default=False, init=False)


def _need(ok):
    if not ok:raise ContractRejected()


def validate_prefill_plan(task, session, stages, tiles, *, hidden_size, vocab_size):
    """Validate complete declared tile-major order (A0,B0,A1,B1,...).

    task/session/stages and widths are supplied by the trusted planning caller;
    hashes are bindings, not proof of model provenance or current journal state.
    No assumptions about padding/packing, actual bytes or active producers can be
    inferred from this metadata check. Bounds are not memory reservations.
    """
    try:
        task=Task.model_validate(task);session=Session.model_validate(session)
        _need(type(stages) is tuple and len(stages)==2)
        stages=tuple(Stage.model_validate(s) for s in stages)
        _need(type(hidden_size) is int and 1<=hidden_size<=4096)
        _need(type(vocab_size) is int and 1<=vocab_size<=262144)
        _need((session.task_id,session.owner_id,session.model_hash)==(task.task_id,task.owner_id,task.model_hash))
        end=0
        for i,s in enumerate(stages):
            _need((s.session_id,s.stage_id,s.layer_start,s.model_hash,s.layout_hash)==
                  (session.session_id,i,end,session.model_hash,session.layout_hash))
            _need(s.assigned_node in session.selected_nodes);end=s.layer_end
        _need(end==task.total_layers and {s.assigned_node for s in stages}==set(session.selected_nodes))
        _need(type(tiles) is tuple and 2<=len(tiles)<=2*MAX_TILES_PER_STAGE and len(tiles)%2==0)
        prefix=0
        for i in range(len(tiles)//2):
            pair=tuple(TileV2.model_validate(t) for t in tiles[2*i:2*i+2])
            for sid,t in enumerate(pair):
                _need((t.task_id,t.owner_id,t.session_id,t.epoch,t.model_hash,t.layout_hash,t.stage_id,t.tile_id,t.token_start)==
                      (task.task_id,task.owner_id,session.session_id,session.epoch,session.model_hash,session.layout_hash,sid,i,prefix))
                _need(t.token_start+t.token_count<=session.expected_tokens)
                expected_input=(1,t.token_count) if sid==0 else (1,t.token_count,hidden_size)
                expected_output=(1,t.token_count,hidden_size if sid==0 else vocab_size)
                _need(t.input.shape==expected_input and t.output.shape==expected_output)
            _need(pair[0].token_count==pair[1].token_count)
            _need(pair[0].output==pair[1].input)
            prefix+=pair[0].token_count
        _need(prefix==session.expected_tokens)
        return PlanAssessment(len(tiles)//2)
    except (ValueError, TypeError, AttributeError, OverflowError):
        # Do not expose caller content or raw validation exceptions.
        pass
    raise ContractRejected() from None


def assess_record(raw):
    """Pure migration preflight: recognize, never convert or write anything.

    Legacy records cannot prove input/output roles, so even valid legacy records
    are explicitly non-convertible. V2 recognition is structural only; plan,
    authority/epoch freshness, journal binding and adjacent boundary checks still
    require validate_prefill_plan plus the future authoritative write path.
    """
    try:
        obj=parse_json_object(raw,MAX_RECORD_BYTES)
        if 'schema_version' not in obj:
            Tile.model_validate_json(raw)
            return RecordAssessment('legacy_unchanged')
        if type(obj['schema_version']) is not int or obj['schema_version']!=2:
            return RecordAssessment('unsupported')
        TileV2.model_validate_json(raw)
        return RecordAssessment('v2_structure_only')
    except (ValueError, TypeError, AttributeError, OverflowError):
        return RecordAssessment('unsupported')
