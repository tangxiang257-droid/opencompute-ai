# Single-instance call ownership experiment

StageOwner is deliberately an ordinary-byte callable lifecycle test adapter, NOT
TiledQwen2Stage or a Tensor/prefill integration. It does not import Torch or create
models, nodes, Workers, sockets or real weights. There is no new journal execution
protocol. Its injected commit is a test/control-plane boundary, not a claim that
V2 receipt records can commit computation. All state snapshots deny execution
permission. Both input and result are exact bytes with a 64 KiB bound; token count
is declared metadata, not derived from bytes or verified model shape.

One instance fixes task/session/epoch/stage/model/layout. The owning event loop
admits one call at a time and requires exact next token prefix. It hashes the
immutable input bytes. Trusted authorize and commit callbacks are synchronous
and execute on that same owning loop, NOT in the compute thread. Callbacks must
not reenter this owner or block waiting for it. Async return values reject. The trusted callback contract transfers cleanup
ownership only for a fresh, unstarted native coroutine (CORO_CREATED), which is
closed without starting its body. Already-started/closed coroutines and other
awaitables are rejected without driving, closing or cancelling them; the creator
retains cleanup responsibility on its appropriate thread/loop. No concurrent
external mutation of returned coroutine state is supported.

Authorization is an explicit one-call ADMISSION decision. The trusted callback
must check current authority and grant the specified attempt at that point; it
cannot merely return a cached structural check. No await occurs between that
check and enqueue, so same-loop cancellation cannot interleave there. This is not
atomic with arbitrary external revocation or the later CPU start. The operation
thread checks a stop flag just before invoking; cancellation after that check is
an admitted running call and cannot be force-stopped. Systems requiring authority
to remain current at every instruction cannot use this boundary. Authentication,
external revocation serialization, actual attempt leases and node grants remain
unimplemented. A future real commit callback must recheck live authority/epoch
and persist its own event transaction; this experiment does not supply that.

The ordinary operation alone runs on a dedicated one-thread executor. We retain
the concurrent Future as actual completion evidence, not a cancellable asyncio
wrapper. On caller cancellation/close, new work is fenced, the stop flag is set,
and the owner awaits actual operation termination. It never calls reset. A late
result is discarded; no commit runs after an observed close/cancellation. Repeated
cancellation of call or close does not abandon the thread. No forced stop or
process timeout is claimed: a nonterminating callable keeps the instance retiring
and resources held. The owning loop must stay alive to supervise this. Arbitrary
loop shutdown/process death is outside the proof and cannot claim safe cleanup.

After ordinary operation success, commit runs synchronously. Commit failure means
unconfirmed, even if it wrote before raising; instance is permanently retired,
prefix does not advance, and blind recomputation is prohibited. Success advances
the prefix only after callback success. A cancellation arriving after synchronous
commit can be observed later; it cannot roll back that commit. On compute/auth
failure the instance is also retired. No reopen, reset, retry or resume is offered.
Close waits for operation completion and full call bookkeeping, then requests
owned executor shutdown(wait=False); this is NOT a thread-join guarantee. No
background work spawned by the operation is covered;
it does not erase caller-owned/result bytes or prove total memory reclamation.

Internal exceptions and causes are diagnostic only; never send them directly to
peers. This does not verify real KV, Tensor aliases, numerical correctness, memory
budgets for models, or actual prefill cancellation behavior. Existing runtime,
public gate, production and remote branches remain untouched.
