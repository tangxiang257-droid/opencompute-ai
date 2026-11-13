# Private pinned CPU bytes-to-stage seam

The actual `compute/qwen2_tiled_stage.py` now has one private module-level factory,
`_owned_cpu_prefill_bytes`. Existing imports/class/methods/signatures are unchanged
(AST checked against #21 source head 752d24e). Nothing calls this factory by default;
no service, gate, Worker, router or public endpoint is connected.

The factory accepts a trusted complete V2 preparing plan plus expected weight-file
and config-file SHA256 pins. It calls the existing actual from_safetensors loader
with explicit CPU and fixed split/stage, creates a new exclusive stage, and returns
ONLY a byte-operation closure, not the stage. It does not accept an arbitrary
caller-owned stage carrying a self-declared identity label. The loader verifies
file-byte identity relative to pins, NOT author authenticity or caller authority.
The model_hash must equal the expected weight-file pin for this narrow path; other
model-hash conventions are unsupported, not silently converted. Layout hash remains
a trusted plan identifier, not an independently certified model provenance claim.

Before loading, validate all declarations and per-tile input/output byte limits.
After loading, compare actual CPU device, stage/layer range, hidden/vocab/layer count,
empty cache and context capacity with the fixed plan. Actual loaded config bytes
are separately pinned by the existing loader. Caller must authenticate pins/plan
and provide live StageOwner authorization/commit fences; the factory is not one.
The plan's PREPARING state is only an initialization snapshot, not live authority.

The closure has the existing StageOwner operation signature. It looks up the
fixed tile by token_start; a caller cannot substitute a new descriptor per call.
It validates encoding v2, constructs an independent explicit CPU Tensor using
torch.tensor(tuple) and calls the existing prefill. Then it requires CPU/F32,
exact output shape and finite values before little-endian output encoding. Both
input and output remain at most 64 KiB per tile. No truncation, logarithm sampling,
compressed result or quota increase. Temporary Python tuples/lists/Tensors and
stage weights/KV are additional allocations; these limits are not total RSS or
model-resource approval. Wire layout/peer binding stays with the existing receiver.

A closure-local lock rejects concurrent calls. Any error after lock acquisition,
including conversion/shape/encoding failure AFTER prefill has advanced KV, makes
the closure permanently unusable. It never resets or repeats that tile. Calling
through StageOwner also retires that owner on failure. No migration/retry/recovery
is supplied. Successful output bytes are not a durable or verified result commit.
CPU operation completion is not thread join or memory erasure. Closure/exception
references can retain the stage and buffers; callers must honor the owner lifetime
and release their references after confirmed operation completion. Internal errors
must not be directly exposed to peers. Hostile Python introspection/monkeypatching
or mutable external authority is not secured by this private API.

## Evidence and untested boundary
Tests AST-extract the real new factory and use ordinary Torch/loader/stage stand-ins,
plus the real pure plan/bytes decoder and StageOwner state machine. They test actual
argument binding, plan/budget refusal, output shape/finite/device refusal, permanent
isolation after a simulated KV advance, encoding failure and concurrent rejection.
The original class AST is unchanged. NO Torch import, real weight read, real loader,
Tensor, model/forward, GPU or node is run in this change.

The separately authorized earlier artificial CPU experiment showed two-block vs
whole-prefill consistency and independent CPU Tensor copying under fixed versions.
It DID NOT run this factory, the full owner+loader chain, cancellation during real
forward or post-forward encoding failure. This patch is therefore an implemented
private seam with ordinary-data tests, NOT a validated real execution pipeline.
A new real-forward/loader experiment requires separately scoped authorization.

The existing loader still uses its original JSON parsing behavior. This increment
does not provide strict configuration JSON parsing or incorporate the other #19
configuration/export fixes. Pinned bytes alone do not establish model provenance.
