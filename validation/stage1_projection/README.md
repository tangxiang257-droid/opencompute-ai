# Stage-1 single-token projection seam — isolated validation only

Base 7b169f92c7bd234967db4e873b2fef006eda7937. No model/device/runtime backend,
real authority/resource service, durable sink, public route or execution adapter
is implemented. Dependencies default to missing and run denies before backend
work. All dependency objects in tests are ordinary doubles. Their method names,
None returns and rejection of Permit are NOT authentication or physical proofs.
Result.execution_allowed and device_completion_verified remain False.

## Fixed scope and real-code gap

Stage 1, batch 1, one token per call; fixed sequence of 1..3 tokens, exact hidden
width/vocab/input hashes and owner/request/session/epoch/model/layout/snapshot/
preprocessing/full-plan/token-digest bindings. Hash actual immutable input bytes,
validate size<=65536 and finite LE FP32. Actual Spec digest includes explicit
one-token object grouping. This is a NEW proposed plan shape, not permission to
split/reinterpret an existing multi-token object. Trusted real assembly must
resolve the full original plan and verify grouping, not merely compare supplied
hash strings. Spec equality is consistency, not provenance.

Each projection emits columns [0,8192), then next columns, exact final tail. No
full logits Tensor/list/bytes operation exists in this seam. For V151936 one token
has 19 projection slices (18*32768+17920), not automatically 19 transport frames
unless the authenticated object plan is exactly one token/object. No framing,
network, result export or transport publication occurs here.

Actual TiledQwen2Stage.prefill still performs full F.linear, advances caches and
returns a full Tensor; the CPU adapter still uses whole tolist/struct.pack. It
cannot be wired directly behind prepare_token without violating this contract.
No existing compute/StageOwner/BLOB/factory/identity file changed. Existing
validation PrivateProducer/Spool are also untouched: their whole-plan one-shot
consumption and no-refund working pool do NOT support this continuing sink.

## Required synchronous dependency contracts (none implemented for production)

Authority:
- hold_call(spec,token) context must hold authentic live user AND execution
  qualification/revocation fences; yields None, never a bool/Permit as permission.
- finalize(spec,token,digest,commit) must freshly revalidate under its LOCAL fence
  and invoke commit exactly once on the calling thread before returning None.
  It must not store/escape commit or treat DB state as new execution authority.
  No callback, duplicate, foreign-thread, escaped callback, swallowed failure or
  commit-then-error permits continuation. Driver verifies the persisted prefix.
- This cannot authenticate remote nodes or atomically commit multiple hosts.

Resources:
- reserve_request(spec,policy) reserves ONCE for the complete sequence; returns
  an opaque non-awaitable, non-boolean ticket. It may not silently reset per token.
- verify_token(spec,ticket,token) checks that same persistent aggregate promise.
- quarantine(spec,ticket,scope) accepts ticket=None when reserve may have committed
  but lost its response. Resolve stable request identity; retain charge and take
  ownership. It is not a refund or confirmation of physical completion.
- There is NO release method, success refund, context-manager cleanup or GC-based
  return of capacity in this seam. Real service must aggregate every request/mode
  against authorized pools; only a shared exhausted-pool DOUBLE is tested here.

Completion:
- open_scope(spec,token,scope_id) registers ownership BEFORE any backend call that
  could enqueue work. Scope ID already exists if registration throws partway.
- materialize(scope,pending,expected_bytes) waits for the exact producer streams,
  events and copy dependencies, then returns independently owned immutable bytes.
- finish_scope(scope) covers ALL token work/copies, including other streams. A
  Python Future done flag, coroutine, method return or journal state is not proof.
- quarantine(scope) accepts partial open_scope and enqueue-then-throw preparation
  without a returned handle. It takes responsibility for unknown work and keeps
  backend/cache/resources unusable; None only acknowledges this interface obligation,
  not that a real device stopped. A real verifier/supervisor is still missing.

Backend:
- check_binding(spec) checks pinned stage/weights/layout/device/source plan and
  cache position without untracked work. Resources must already be reserved.
- prepare_token(scope,input,token) performs layers and normalization ONCE and
  retains candidate hidden/new KV under scope; never full vocab projection.
- project_slice(scope,handle,column,count) produces only that exact vocab range,
  including all casts/contiguous copies in the declared resource/scope ownership.
- install_cache(scope,handle) only changes local references; it may NOT enqueue
  device work after finish_scope. Failure at this point retires the entire stage.
- retire(spec) disables this backend/cache logically, does NOT free resources
  while completion is unknown and does not assert physical device stop.

Sink:
- append(token,column,bytes) synchronously consumes each bounded block before the
  driver requests another. Caller/sink may retain aliases; RSS is not guaranteed.
- seal(token,digest) verifies immutable durable complete token bytes/metadata;
  fake test sink only records calls, so no real persistence/destination claim.
- quarantine(spec,token) preserves uncertain partial files/charges, no auto cleanup.
- A production per-token sink still needs request-wide cumulative accounting,
  durable binding/manifest, separate destination promise, and read authorization.

Awaitables are rejected on synchronous returns; newly created native coroutines
are closed to avoid treating a not-yet-run adapter as a completed operation.
Started async work cannot be safely closed by this seam; its creator owns it.

## Resource policy and unresolved real measurements

Positive exact integers only; unknown/zero/unsupported workspace -> reject.
Trusted planner declares resident stage/head bytes, simultaneous old+new KV and
workspace maximum for the LAST token, plus explicit host/device/source/dest caps.
Device minimum = resident + old/new KV + workspace + 2*hidden*4 + slice_bytes.
Host minimum = hidden*4 + 3*slice_bytes + 32768 metadata. These are logical minima,
not automatic detection of all allocations; real planner must include missing
attention expanded-KV/scores/softmax, allocator/alignment, stream overlap, copies,
pins and overhead in conservative requirements. If unknown, real assembly denies.
Per-side retained minimum = ALL token logits + 32768 metadata, each <=2MiB.
Three V151936 tokens require 1856000 source AND 1856000 destination bytes; four
reject. Per-side budgets are not a global 2MiB budget or distributed reservation.
Real shared-pool, disk/WAL overhead, physical RSS/GPU peaks and resource monitoring
remain unavailable; policy arithmetic alone must never enable actual computation.

## Continuity, completion and failure

Fresh local SQLite journal binds complete Spec, generated live driver instance,
input digests, consumed token and exact prefix. Same driver/thread can continue
only after exact input and new live checks. Same path cannot reopen; a second
driver cannot bind even the existing Journal object. Another/copied/rolled-back
journal is an external global/session-stage registry problem, not solved here.

Success order: live authority -> reserve/verify whole-request resources -> durable
ADMITTED -> register work scope -> binding/prepare -> sequential project/materialize/
append -> finish all token work -> seal -> no-device-work cache install -> authority
context successful exit -> live finalize callback CAS -> verify committed prefix.
Only then does the same driver advance its expected token. No reset method.

Any error/stop invalidates the whole stage and future tokens. Durable retirement
is attempted first; every applicable completion/resource/sink/backend quarantine
is then attempted even if earlier cleanup failed. Errors aggregate rather than
claim successful cleanup. Quarantine must be idempotent; retries here never refund
or start compute. On storage failure caller must quarantine the journal/storage;
no successful durable-retirement claim. Surviving ADMITTED and fixed instance still
block tested repeat execution. No power-loss or process-crash recovery implemented.
Checks have cancellation windows: an admitted operation can begin after concurrent
stop. Stop prevents later checked work; it cannot forcibly cancel a running kernel.
finish failure, SQLite retirement or callback exit does not prove physical stop.
Final commit can be durable before response is lost; entire stage still retires,
no replay. Cache may already have advanced; never reset it and re-project.

## Safe verification only

Tests use ordinary bytes, SQLite and event barriers. They cover correct sequence/
tails, resource declarations, shared-pool rejection double, ownership, input/order,
no-permit default denial, failure at every major boundary, no-handle scope failure,
reservation response loss, missing/double/escaped/foreign/suppressed commit,
awaitable misuse, blocked completion/sink, concurrent stop/busy call, quarantine
and retirement-write failure. Timeouts only detect deadlock; no sleep race oracle.
No test establishes numerical equivalence, real authentication/authorization,
actual durable sink, GPU completion, peak resources or readiness for production.
