# Fresh-only local attempts with opt-in serial stage 1

Explicit LocalAttemptJournal.create creates a new local journal and dedicated
attempt table; existing V2 creation, legacy tables and public gates are unchanged.
By default only stage 0 and complete fixed local token bytes are supported.
Explicit opt-in adds the serial stage-1 path described below. No stage-1 receipt,
transport delivery or remote peer is promoted into execution permission.
No model, node, forward or Tensor is exercised by this slice.

make_owner connects the real StageOwner callbacks to SQLite. It derives binding,
ranges and expected output shape from the registered plan and retains immutable
inputs. Admission validates actual input digest/count, live owner and an additional
required execution_authorize(spec) context, then stores actual input bytes and an
ADMITTED record in the SAME transaction. Execution contexts must be supplied by a
trusted caller, authenticate the principal and hold an external revocation fence
until exit. PREPARING alone never suffices; supplied owner strings are not proof.
These synchronous contexts must be nonreentrant and obey single-loop ownership.
The selected operation is trusted assembly, not authenticated by this module.

One stage instance ID is allowed in the attempt table; exact duplicates are denied,
not returned as permission. A new owner cannot restore KV. There is no reopen,
resume or automatic recovery. ADMITTED remains conservative after exceptions,
cancellation or crash: it means possibly executed, NOT safe to retry. There is no
ABORTED certainty claim. Input and eventual output are real BLOB bytes, not hashes
standing in for artifacts. Output shape/finite F32 bytes and current authorization
are rechecked; output and OUTPUT_COMMITTED are committed atomically. No automatic
GC/deletion. Per object <=64KiB; retained input plus reserved output <=256KiB, <=64
attempts. This bounds application BLOB lengths, not SQLite files/WAL/RSS or KV.

A cancellation committed before output denies output commit. Output committed
before cancellation is a historical result only. Exceptions from an authorization
context on exit may occur AFTER SQLite commit: no success is returned, owner is
retired, and a separately currently owner-authorized query can inspect the actual
record and output. Queries never grant execution or retry permission. Exceptions
stay internal; callers must not expose raw exception text/tracebacks to peers.

SQLite BEGIN IMMEDIATE uses the existing 15-second busy timeout and can block the
owning event loop. This candidate does not claim responsive cancellation while
blocked, process isolation, hostile same-UID resistance, power-loss durability or
an online deployment-ready execution service. Journal close is a fence, not owner
shutdown: caller must stop/await all owners before releasing their resources.
Initialization is multiple transactions; failure can leave a partial fresh file,
which must not be reopened as valid. No cleanup is attempted. Parent directory and
same-UID filesystem are trusted as for V2Journal; inode checks are observations,
not protection against adversarial replacement.

Tests use temporary SQLite, ordinary bytes and real StageOwner with ordinary
functions. They exercise transaction rollback, duplicate denial, concurrent owner
admission, cancellation, count/input mismatch, post-commit context exit failures,
actual retained BLOBs and fresh-only refusal. They do not validate real prefill.

Authorization context exception suppression is explicitly neutralized at every
local authorization boundary (including bootstrap): a body BaseException is retained
and re-raised if a context returns suppression. This prevents duplicate admission
or output mismatch from becoming successful authorization/commit. Context exit
errors still fail closed; post-commit exit errors remain uncertain and require query.
This does not protect against malicious callbacks that directly mutate the database
or perform other side effects; callbacks remain trusted and nonreentrant.

## Reconstructed opt-in serial stage 0 → stage 1

This increment was reconstructed from fixed PR #21 head
`7bfbe9f1c966442bfd1064817f5ad8ba86d49be5`. It is not a recovery or
revalidation of the unavailable earlier local candidate.

`create(..., enable_stage1=True)` creates an additional fresh attempt table.
The default remains false and creates no stage-1 table. `make_stage1_owner`
requires the complete stage-0 output prefix, reads actual immutable BLOBs,
checks their committed events and matching plan/epoch/descriptors, and binds
those exact bytes. At each admission and output commit it repeats the source
checks in the same SQLite transaction as the write, and compares them to the
fixed snapshot. A matching replacement digest alone cannot change bound input.
Callers must supply those actual committed bytes to the returned owner.

Both stages share the existing 256 KiB logical input plus reserved-output
budget. Stage-1 input copies count again. No quota increase, automatic release,
retry, reopening, recovery, migration, public gate or default executor change.
The maximum per object remains 64 KiB and per stage 64 attempts. Limits do not
bound SQLite file/WAL growth, Python snapshots/copies, process RSS or model KV.
Complete stage 0 must finish before stage 1 can bind: no interleaved pipeline,
network transfer, model inference, speedup or production isolation is proven.

Owner and execution contexts still must hold trusted external revocation
fences; SQLite does not create them. Same-UID storage and callbacks are trusted.
Body failures cannot be swallowed into successful admission. Output/context
failure after admission leaves a possibly executed attempt; an exit failure
may occur after output commit. Query historical state, never blindly retry.
Closing the journal does not stop owners. Synchronous SQLite retains its
15-second busy timeout; callers must stop and await owners before release.

Reconstruction tests use only ordinary bytes and temporary SQLite. The first
15 feature cases failed because the opt-in API was absent, then passed after
implementation; four additional boundary regressions cover stale epoch, live
execution revocation, output rollback and post-commit context-exit failure.
The scaled budget boundary is 968 bytes (520 upstream + 448 downstream), not
an exact 256 KiB stress or physical-memory measurement. A framework-import
blocker is applied; broad model/node test discovery is deliberately not run.
