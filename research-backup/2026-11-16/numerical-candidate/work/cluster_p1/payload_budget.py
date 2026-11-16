"""Declared byte accounting only: no reservation, execution grant or payload proof."""
from dataclasses import dataclass, field
import json
import math
from .envelope import PREFIX, MAX_HEADER
from .handoff import MAX_FRAME_BYTES
from .local_attempt import MAX_BYTES, MAX_RESERVED
from .store import Conflict
from .tile_contract_v2 import validate_prefill_plan, TileV2
from framed_result_transport import CHUNK, MAX_OBJECT, MAX_FRAMES


@dataclass(frozen=True, slots=True)
class PayloadBudget:
    object_bytes: tuple
    hidden_bytes: tuple
    hidden_frame_bytes: tuple
    logits_bytes: tuple
    local_retained_bytes: int
    receiver_retained_bytes: int
    result_set_bytes: int
    result_set_frames: int
    framed_results_fit: bool
    output_semantics: str = field(default='full-logits-f32-le-c-batch1-v1', init=False)
    execution_allowed: bool = field(default=False, init=False)
    payload_verified: bool = field(default=False, init=False)

    def require_local(self, *, max_bytes=MAX_BYTES, max_reserved=MAX_RESERVED):
        # Overrides may tighten a limit, never increase the existing ceilings.
        if (type(max_bytes) is not int or not 0 < max_bytes <= MAX_BYTES or
                type(max_reserved) is not int or not 0 < max_reserved <= MAX_RESERVED):
            raise Conflict('invalid budget ceiling')
        if any(size > max_bytes for size in self.object_bytes):
            raise Conflict('object budget')
        if self.local_retained_bytes > max_reserved:
            raise Conflict('whole wave plan retained budget')

    def require_handoff(self):
        self.require_local()
        if any(size > MAX_FRAME_BYTES for size in self.hidden_frame_bytes):
            raise Conflict('complete hidden frame budget')
        if self.receiver_retained_bytes > MAX_RESERVED:
            raise Conflict('receiver retained budget')


def assess_payload_budget(task, session, stages, tiles, *, hidden_size, vocab_size,
                          wire_layout_version):
    """Full batch-one FP32 logits, exact tail; no truncation or route selection.

    Frame estimates follow Envelope v1 JSON encoding, including identities.
    A framed-result fit describes that transport only, not local executability.
    The receiver retains inbound full frames plus stage-one input/output copies.
    Runtime authorization, reservation and byte validation remain mandatory.
    """
    validate_prefill_plan(task, session, stages, tiles,
                          hidden_size=hidden_size, vocab_size=vocab_size)
    if type(wire_layout_version) is not int or not 2 <= wire_layout_version < 2**31:
        raise ValueError('wire layout version')
    tiles = tuple(TileV2.model_validate(t) for t in tiles)
    sizes = tuple((8 if d.dtype == 'int64' else 4) * math.prod(d.shape)
                  for t in tiles for d in (t.input, t.output))
    hidden = tuple(4 * math.prod(t.output.shape) for t in tiles[::2])
    logits = tuple(4 * math.prod(t.output.shape) for t in tiles[1::2])
    frames = []
    for t, size in zip(tiles[::2], hidden):
        header = dict(task_id=t.task_id, session_id=t.session_id,
                      execution_epoch=t.epoch, stage_id=t.stage_id, tile_id=t.tile_id,
                      token_range=(t.token_start, t.token_start+t.token_count),
                      model_hash=t.model_hash, layout_version=wire_layout_version,
                      tensor_shape=t.output.shape, dtype=t.output.dtype)
        length = len(json.dumps(header, sort_keys=True, separators=(',', ':')).encode())
        if length > MAX_HEADER:
            raise Conflict('envelope header budget')
        frames.append(PREFIX.size + length + size + 32)
    counts = tuple((size + CHUNK - 1) // CHUNK for size in logits)
    result_bytes, result_frames = sum(logits), sum(counts)
    fit = (all(size <= MAX_OBJECT for size in logits) and
           all(n <= MAX_FRAMES for n in counts) and
           result_bytes <= MAX_OBJECT and result_frames <= 127)
    return PayloadBudget(sizes, hidden, tuple(frames), logits, sum(sizes),
                         sum(frames) + sum(hidden) + sum(logits),
                         result_bytes, result_frames, fit)
