# Explicit local known-token prefill waves

This is an isolated ordinary-byte/SQLite experiment, based on serial handoff
commit e2244170b92308a7aa4938eb1082936e249c7957. Existing modules and default
stage-0/opt-in serial behavior remain unchanged. Nothing imports or enables the
new module by default. WaveAttemptJournal.create requires enable_wave=True and
a fresh path; no reopening, migration or production route is added.

## Dependency scope

Only known-token causal prefill is represented. A[k] is stage 0 tile k and B[k]
is stage 1 tile k. Required predecessors are A[k-1] for A[k], and both A[k] and
B[k-1] for B[k]. The conservative schedule is A[0], then waves (A[k+1], B[k]),
then final B[N-1]. It adds a wave barrier beyond the minimum DAG. Ordinary
autoregressive decoding must not use this schedule: its next token depends on
the downstream result. Each stage keeps the same single-worker owner through
the full request, with exact token_start/count including tail tiles. A common
journal prefix does not restore or prove model KV state.

## Binding and limits

The stage-1 facade obtains the next actual committed source itself; callers do
not supply payloads or token ranges. It holds those immutable bytes across the
call. Admission and output commit each revalidate plan, epoch, matching source
bytes, descriptor and source event in the same write transaction as the local
attempt event. Both live authenticated owner and execution contexts are still
required; they must supply trusted external revocation fences. Notifications,
readiness, prior receipts and owner strings cannot grant execution authority.

SourceNotReady before admission does not retire an otherwise unused owner.
An admitted call failure does retire it. No SQLite transaction spans an await
or ordinary worker operation. One facade tile can be bound at a time. Metadata
and stored input/output rows remain local and trusted against hostile same-UID
access; Python private members are not a security boundary.

The complete plan's retained logical input plus reserved outputs must fit the
unchanged 256 KiB shared budget before work starts, including stage-1 copies.
Every admission also checks actual shared usage atomically. Every object stays
within 64 KiB. All rows/reservations remain across waves; no GC or credits are
returned. This is not a bound on Python copies, SQLite/WAL size, weights, KV,
process RSS or physical devices. At most two operation tasks are scheduled in
a wave, with no unbounded delivery queue.

## Failure and cancellation

TaskGroup owns each wave. A failure cancels its sibling, and the owner waits
for a started ordinary operation to finish before its task returns. The
coordinator then retires both owners and drains cleanup even if cancellation
arrives during cleanup. An uncooperative operation can block indefinitely.
Operation completion is not thread termination or device shutdown proof.

No subsequent wave is dispatched on failure. Already committed outputs remain
committed even if the other wave member fails. ADMITTED means possibly executed,
not safe retry. Context exit can fail after a database commit. A new owner cannot
resume a nonzero KV prefix. There is no rollback of computation, automatic retry,
resume, recovery, epoch replacement, or alternative execution path. Internal
exceptions are diagnostics only and must not be returned to untrusted peers.

## Evidence

Eighteen new ordinary tests cover missing-source readiness, full-prefix-free first
handoff, two real ordinary worker invocations simultaneously held at event
barriers, same-stage prefix/tail ordering, single-tile drainage, two-sided cancel
waiting, downstream failure, partial commit surviving sibling failure, source
replacement including matching digest, stale epoch, live revocation, duplicate
admission, suppressing authorization, direct adapter budget preflight, shared
retained budget and output-event rollback. Thread waits use timeouts only as
deadlock guards, not as a speed or elapsed-time assertion.

The explicit regression allowlist passes 164 tests (146 existing, 18 new), with
framework imports blocked and ResourceWarning-as-error. Initial TDD RED had 12
missing-adapter failures. A separate targeted RED found the direct facade entry
lacked whole-plan/object budget preflight; that entry now performs the same
preflight. A test-setup event-loop lookup error was corrected before green.
No model, tensors, real forward, node, transport, GPU or performance benchmark
was run. Ordinary-thread overlap is not evidence of model numerical correctness,
model inference concurrency, network scalability, isolation, or speedup.

## Explicit non-atomic and provenance boundaries

A wave is NOT an atomic two-stage transaction or all-or-none reservation. Each
side admits/reserves and commits separately. One admission may succeed while
the other rolls back; one output may commit while the sibling fails or cancels.
The whole-plan check prevents deterministic logical-capacity exhaustion in the
normal isolated case, not transactional reservation of the entire wave. Failure
stops dispatch, retires/drains both owners and retains all partial history and
budget. Both operations may have advanced private state without two committed
outputs. No retry or compensation is inferred from absence of a receipt.

If trusted storage is altered BEFORE binding and its event digest is changed
consistently, the replacement can be accepted. Tests record this limitation:
local consistency checks do not prove source origin or authenticity against a
hostile same-UID writer. After binding, exact held-byte equality rejects such a
replacement. These are separate properties.
