# Delivery / receipt boundary (isolated candidate)

DeliveryAttempt uses the real Delivery.release and real Receiver. It never calls
prefill, sets computed, or treats Delivery.commit as a business commit. Tests use
real Delivery and journal with an event-controlled ordinary channel release stub;
this is not TLS, live Channel or network acceptance. Parallel Channel evidence is
separate from these tests.

Construction must occur on the owning event loop. It accepts exact Envelope fields
of immutable builtin types, validates a 64 KiB bound, and snapshots deterministic
encoded bytes without awaiting. Later replacement of delivery.envelope cannot
change registration. Trusted same-loop, non-reentrant Python callers are required;
this is not protection against hostile monkeypatching, cross-thread mutation or
object.__setattr__. It does not authenticate the peer itself: the Receiver's
trusted fixed binding and authorization fences remain mandatory.

Delivery retains no original wire frame. Consequently the receipt is the digest
of canonical re-encoding, NOT an audit digest of original received wire bytes.
Different valid wire JSON encodings of the same Envelope deduplicate semantically.
Do not mix the raw receive_bytes mode and Delivery mode for one plan and claim
identical raw-byte replay semantics; the raw entry remains byte-exact. No payload
is durably stored and no compute permission or retry safety is conferred.

Caller owns a Delivery until run begins its synchronous claim. Constructor errors,
wrong-loop calls and cancellation before run starts leave ownership with caller.
After claim, a marker prevents a second adapter from claiming the same object.
Direct release or access through another protocol while claimed is forbidden.
Registration runs synchronously with current DB/fence checks. SQLite can block
this event loop (existing 15-second DB timeout); this is not a scalable async
service, and cancellation cannot interrupt that synchronous operation.

The adapter attempts release exactly once after registration succeeds or fails.
Outer cancellation is shielded while that release task actually finishes; repeat
cancellation does not abandon it. There is no timeout pretending release stopped.
If the underlying release never terminates, the adapter remains pending; transport
supervision is not implemented here. A failed/cancelled release is unconfirmed,
not retried: real Channel may already have debited local accounting and closed.
Failure to start the task is likewise unconfirmed and never reported as released.
Completed release is local API completion, not proof the peer received its credit.

A public immutable state snapshot separates registration (not_started/unconfirmed/
recorded/duplicate) from release (not_started/pending/completed/unconfirmed).
An exception can follow a successful registration, including cancellation during
release. Preserve the snapshot and original receipt identity; never infer rollback
or rerun computation. Owner/peer context exit failure can leave registration
unconfirmed despite a committed DB row. No automatic retry occurs. If both ordinary
registration and release fail, the original registration exception is re-raised
with release failure as its explicit cause; cancellation takes priority. If
registration and release both fail while the caller cancels, CancelledError still
propagates, with a BaseExceptionGroup cause retaining both original errors and
their own cause chains. This diagnostic graph can retain references and is not
public-safe or a memory-erasure guarantee. Errors
are internal diagnostics, not sanitized public HTTP messages.

No existing wire/channel/default behavior changed; no network, tensor framework,
model, node, public execution gate or remote branch is used or opened.

## Opt-in bound ordinary-byte consumption

An optional exact StageOwner can now consume the same immutable payload snapshot
whose canonical frame was registered. Constructor checks stage 1 and task/session/
epoch/model/layout against the fixed receiver context. It accepts no independent
consumer payload parameter. This does not connect Tensor conversion or prefill.
Authorize/commit remain trusted StageOwner callbacks with their own admission and
transaction requirements; receiving bytes grants neither computation permission
nor a real journal computation-commit protocol.

A nonduplicate receipt may be followed by one ordinary-byte call. The Delivery is
not released until that call and any cancellation-driven owner retirement finish.
Consumption is separately reported as completed or unconfirmed. There is an
explicit cancellation checkpoint after registration and before consumption:
receipt-present/never-consumed is possible. The owner is retired on this failure.
Duplicates are skipped, with prior consumption UNKNOWN, not assumed completed or
resumed. This deliberately leaves a recovery gap rather than recomputing blindly.

After processing/release resolves, the adapter drops its own frame, payload,
delivery and consumer references, including failure paths. It cannot revoke
external references or erase bytes. A canonical frame is an additional bounded
copy; channel credits account logical payload, not all Python buffers, results,
RSS or physical memory. A retained external alias remains valid after release.
The guarantee is that this adapter's last consumer read precedes release, not a
process-wide memory limit or an absence of aliases.

StageOwner's concurrent Future completion proves the submitted operation returned
or raised. It does NOT prove the executor thread joined; shutdown(wait=False)
starts executor teardown. No background work spawned by a callable is supported.
The loop and trusted callbacks must follow the declared ownership contract.

Existing primary cause/context graphs are never overwritten by cleanup failures.
If one already exists, a group retains the original operation and cleanup errors;
for cancellation a CancelledError wrapper keeps cancellation propagation while
its cause retains that group. Thus top-level exception identity/type (except
cancellation) is not guaranteed for compound faults. Original instances remain
in the diagnostic graph, which is internal only and must not be sent to peers.
