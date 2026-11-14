"""Lazy, private, one-token Qwen2 stage-1 projection backend.

This is real tensor code, NOT an execution grant or a completion implementation.
Only a trusted assembly may supply a pinned, exclusively owned loaded stage and
the same completion adapter used by its driver. No model is loaded here. Scope
registration, actual device fences/copies, resource admission and durable output
belong to external adapters. Tests AST-load this source with symbolic tensors;
they do not establish numerical equivalence, device completion or memory peaks.
"""
from contextlib import contextmanager
import hashlib
import inspect
import re
import struct
import threading


def _dependencies():
    # Deliberately deferred until a trusted assembly constructs a backend.
    import torch
    import torch.nn.functional as functional
    from compute.qwen2_core import forward_layers, rms_norm
    from compute.qwen2_tiled_stage import TiledQwen2Stage
    return torch, functional, forward_layers, rms_norm, TiledQwen2Stage


def _require(ok):
    if not ok:
        raise ValueError('one-token projection contract rejected')


def _synchronous(value):
    if inspect.isawaitable(value):
        if inspect.iscoroutine(value) and inspect.getcoroutinestate(value) == inspect.CORO_CREATED:
            value.close()
        raise ValueError('synchronous completion evidence required')
    return value


def _nothing(value):
    _require(_synchronous(value) is None)


class ProjectionPending:
    """Trusted completion owns this tensor until materialized; not a wire object.

    Completion must bind identity, scope and owner and clear its tensor reference
    after materialization. Public attributes are not a hostile-Python boundary.
    """
    __slots__ = ('tensor', 'scope', 'owner', 'column', 'count')

    def __init__(self, tensor, scope, owner, column, count):
        self.tensor = tensor
        self.scope = scope
        self.owner = owner
        self.column = column
        self.count = count


class Qwen2ProjectionAdapter:
    """Fresh stage 1, batch 1, token 0, one invocation and no retry/reset.

    Pin strings bind the expected spec, but do NOT prove weight provenance: the
    external loader must actually validate source bytes and exclusively transfer
    the stage. Completion methods acknowledge an external obligation, never a
    self-issued device-completion certificate. Quarantine retains this adapter
    and its references on errors; retire does not free or synchronize anything.
    """
    stage_id = 1
    _claim_lock = threading.Lock()

    def __init__(self, stage, spec, *, completion, expected_model,
                 expected_layout, expected_layer_range):
        _require(completion is not None)
        for value in (expected_model, expected_layout):
            _require(type(value) is str and re.fullmatch('[a-f0-9]{64}', value) is not None)
        _require(spec.model == expected_model and spec.layout == expected_layout)
        _require(type(spec.inputs) is tuple and len(spec.inputs) == 1)
        _require(type(spec.hidden) is int and 0 < spec.hidden * 4 <= 65536)
        _require(type(spec.vocab) is int and 0 < spec.vocab * 4 + 32768 <= 2097152)
        _require(type(expected_layer_range) is tuple and len(expected_layer_range) == 2
                 and all(type(n) is int for n in expected_layer_range)
                 and 0 <= expected_layer_range[0] < expected_layer_range[1])
        self._torch, self._functional, self._forward, self._norm, stage_type = _dependencies()
        _require(type(stage) is stage_type)
        self._stage = stage
        self._spec = spec
        self._digest = spec.digest
        self._input_digest = spec.inputs[0]
        self._hidden, self._vocab = spec.hidden, spec.vocab
        self._range = expected_layer_range
        self._completion = completion
        self._head, self._norm_weight, self._layers = stage.head, stage.norm, stage.layers
        self._old_caches = stage.caches
        self._meta = dict(stage.meta)
        self._device = stage.device
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None
        self._bound = False
        self._state = 'NEW'
        self._scope = self._handle = self._pending = None
        self._normalized = self._candidate_caches = None
        self._column = 0
        self._fresh_stage()
        with Qwen2ProjectionAdapter._claim_lock:
            _require(not hasattr(stage, '_private_one_token_owner'))
            stage._private_one_token_owner = self

    def _live(self):
        _require(not self._stop.is_set()
                 and self._stage._private_one_token_owner is self)

    def _fresh_stage(self):
        s = self._stage
        _require(s.stage_id == 1 and tuple(s.layer_range) == self._range
                 and self._range[1] == s.meta['layers']
                 and s.meta == self._meta and s.meta['workers'] == 1
                 and s.meta['hidden'] == self._hidden and s.meta['vocab'] == self._vocab
                 and s.meta['positions'] >= 1 and s.device == self._device)
        _require(s.cache_length == 0 and s.batch_size is None
                 and s.caches is self._old_caches and len(s.caches) == len(s.layers)
                 and all(c is None for c in s.caches)
                 and s.layers is self._layers and len(s.layers) == self._range[1] - self._range[0]
                 and s.head is self._head and s.norm is self._norm_weight)
        for tensor, shape in ((s.head, (self._vocab, self._hidden)), (s.norm, (self._hidden,))):
            _require(isinstance(tensor, self._torch.Tensor) and tuple(tensor.shape) == shape
                     and tensor.dtype == self._torch.float32 and tensor.device == s.device)

    @contextmanager
    def _owned(self):
        if not self._lock.acquire(blocking=False):
            self._stop.set()
            raise ValueError('projection backend busy; retired')
        try:
            _require(not self._stop.is_set())
            if self._thread is None:
                self._thread = threading.get_ident()
            _require(threading.get_ident() == self._thread)
            yield
            _require(not self._stop.is_set())
        except BaseException:
            self._stop.set()
            raise
        finally:
            self._lock.release()

    def check_binding(self, spec):
        with self._owned():
            _require(self._state == 'NEW' and spec.digest == self._digest)
            self._fresh_stage()
            self._bound = True

    def prepare_token(self, scope, value, token):
        with self._owned():
            _require(self._bound and self._state == 'NEW'
                     and type(token) is int and token == 0)
            _require(type(scope) is str and bool(scope))
            _require(type(value) is bytes and len(value) == self._hidden * 4
                     and hashlib.sha256(value).hexdigest() == self._input_digest)
            _require(all(bits[0] & 0x7f800000 != 0x7f800000 for bits in struct.iter_unpack('<I', value)))
            self._fresh_stage()
            self._scope = scope
            _nothing(self._completion.require_open(scope, self._spec, self))
            self._live()
            # Consume before any tensor allocation/enqueue. An exception without
            # a handle still belongs to the externally registered scope.
            self._state = 'PREPARING'
            with self._torch.inference_mode():
                x = self._torch.tensor(struct.unpack('<' + 'f' * self._hidden, value),
                                       dtype=self._torch.float32, device=self._device).reshape(1, 1, self._hidden)
                self._live()
                x, self._candidate_caches = self._forward(x, self._layers, self._old_caches, self._meta)
                self._live()
                self._normalized = self._norm(x, self._norm_weight, self._meta['rms_eps'])
            self._live()
            self._tensor(self._normalized, (1, 1, self._hidden))
            _require(type(self._candidate_caches) is list and len(self._candidate_caches) == len(self._layers))
            self._handle = object()
            self._state = 'PREPARED'
            return self._handle

    def _tensor(self, tensor, shape):
        _require(isinstance(tensor, self._torch.Tensor) and tuple(tensor.shape) == shape
                 and tensor.dtype == self._torch.float32 and tensor.device == self._device)
        # Real torch finite checking may itself synchronize: it is inside the
        # registered scope, but it does not certify other streams are finished.
        _require(bool(self._torch.isfinite(tensor).all()))

    def _materialized(self):
        _nothing(self._completion.require_materialized(self._scope, self))
        self._live()
        if self._pending is not None:
            _require(self._pending.tensor is None)
            self._pending = None

    def project_slice(self, scope, handle, column, count):
        with self._owned():
            _require(self._state == 'PREPARED' and handle is self._handle and scope == self._scope)
            _require(type(column) is int and column == self._column
                     and type(count) is int and 0 < count <= 8192
                     and column + count <= self._vocab)
            self._fresh_stage()
            self._materialized()
            with self._torch.inference_mode():
                # Slice the head BEFORE linear. No full vocabulary result exists.
                tensor = self._functional.linear(self._normalized, self._head[column:column + count])
            self._live()
            self._tensor(tensor, (1, 1, count))
            self._pending = ProjectionPending(tensor, scope, self, column, count)
            _nothing(self._completion.register_pending(scope, self, self._pending))
            self._live()
            self._column += count
            return self._pending

    def install_cache(self, scope, handle):
        with self._owned():
            _require(self._state == 'PREPARED' and handle is self._handle
                     and scope == self._scope and self._column == self._vocab)
            self._fresh_stage()
            self._materialized()
            _nothing(self._completion.require_finished(scope, self))
            self._live()
            # Reference installation only. No tensor operation or device enqueue
            # may follow external finish in this backend.
            self._stage.caches = self._candidate_caches
            self._stage.cache_length = 1
            self._stage.batch_size = 1
            self._state = 'INSTALLED'

    def retire(self, spec):
        self._stop.set()
        _require(spec.digest == self._digest)
        # Keep references for external quarantine, even after failed installation.


class Stage0ByteAdapter:
    """Single-token stage-0 prefill with externally completed hidden-byte return.

    Trusted loader owns stage/pins. The caller registers scope before operation
    and owns durable commit/quarantine afterward. Old prefill can advance cache
    before materialization fails: such instances are permanently retired, never
    reset/retried. This seam neither opens scopes nor authorizes execution.
    """
    stage_id = 0
    _owned = Qwen2ProjectionAdapter._owned
    _tensor = Qwen2ProjectionAdapter._tensor
    _live = Qwen2ProjectionAdapter._live
    retire = Qwen2ProjectionAdapter.retire

    def __init__(self, stage, spec, *, completion):
        _require(completion is not None and type(spec.inputs) is tuple and len(spec.inputs) == 1)
        _require(type(spec.hidden) is int and 0 < spec.hidden * 4 <= 65536
                 and type(spec.vocab) is int and spec.vocab > 0)
        self._torch, _, _, _, stage_type = _dependencies()
        _require(type(stage) is stage_type)
        self._stage, self._spec, self._completion = stage, spec, completion
        self._digest, self._input_digest = spec.digest, spec.inputs[0]
        self._hidden, self._vocab = spec.hidden, spec.vocab
        self._meta, self._device = dict(stage.meta), stage.device
        self._layers, self._caches, self._embed = stage.layers, stage.caches, stage.embed
        self._lock, self._stop = threading.Lock(), threading.Event()
        self._thread = None
        self._state = 'NEW'
        self._pending = None
        self._fresh_stage()
        with Qwen2ProjectionAdapter._claim_lock:
            _require(not hasattr(stage, '_private_one_token_owner'))
            stage._private_one_token_owner = self

    def _fresh_stage(self):
        s = self._stage
        _require(s.stage_id == 0 and s.layer_range[0] == 0
                 and 0 < s.layer_range[1] < s.meta['layers']
                 and len(s.layers) == s.layer_range[1]
                 and s.layers is self._layers and s.caches is self._caches
                 and len(s.caches) == len(s.layers) and all(c is None for c in s.caches)
                 and s.cache_length == 0 and s.batch_size is None
                 and s.meta == self._meta and s.meta['workers'] == 1 and s.meta['positions'] >= 1
                 and s.meta['hidden'] == self._hidden and s.meta['vocab'] == self._vocab
                 and s.device == self._device and s.embed is self._embed
                 and s.head is None and s.norm is None)
        _require(isinstance(s.embed, self._torch.Tensor)
                 and tuple(s.embed.shape) == (self._vocab, self._hidden)
                 and s.embed.dtype == self._torch.float32 and s.embed.device == self._device)

    def operation(self, scope, value, *, token_start=0, stage_id=0):
        with self._owned():
            _require(self._state == 'NEW' and type(token_start) is int and token_start == 0
                     and type(stage_id) is int and stage_id == 0)
            _require(type(scope) is str and bool(scope) and type(value) is bytes and len(value) == 8
                     and hashlib.sha256(value).hexdigest() == self._input_digest)
            token = struct.unpack('<q', value)[0]
            _require(0 <= token < self._vocab)
            self._fresh_stage()
            _nothing(self._completion.require_open(scope, self._spec, self))
            self._live()
            self._state = 'CONSUMED'
            with self._torch.inference_mode():
                tensor = self._torch.tensor((token,), dtype=self._torch.int64,
                                            device=self._device).reshape(1, 1)
                self._live()
                hidden = self._stage.prefill(tensor, token_start=0, stage_id=0)
            self._live()
            self._tensor(hidden, (1, 1, self._hidden))
            self._pending = ProjectionPending(hidden, scope, self, 0, self._hidden)
            _nothing(self._completion.register_pending(scope, self, self._pending))
            self._live()
            result = _synchronous(self._completion.materialize(scope, self._pending, self._hidden * 4))
            self._live()
            _require(type(result) is bytes and len(result) == self._hidden * 4)
            _require(all(bits[0] & 0x7f800000 != 0x7f800000 for bits in struct.iter_unpack('<I', result)))
            _nothing(self._completion.require_materialized(scope, self))
            self._live()
            _require(self._pending.tensor is None)
            self._pending = None
            _nothing(self._completion.finish_scope(scope))
            self._live()
            _nothing(self._completion.require_finished(scope, self))
            self._live()
            self._state = 'DONE'
            return result
