# Isolated full-logits byte/spool prototype

Fixed base: e7b23bf1ade4e055e122cfdd7a525966c76e49ff. Experimental validation
code only. No model, compute adapter, Worker/node, socket or execution grant.
Existing 64KiB/256KiB local_attempt/StageOwner limits and identity denial remain
unchanged. This is not deployed big-logits support or a new result acceptance mode.

Production writes are canonical outer token / inner vocabulary slices, exact
bytes only, finite LE FP32, <=32768 bytes each. Callback must run synchronously
on its owner thread while the body is active; memoryview/backing-storage slices
are rejected. Metadata binds owner/task/session nonce/attempt/epoch/connection,
model/layout/input snapshot/token/preprocessing/plan digests, axes and semantics.
Bindings are synthetic equality declarations, not authentication or provenance.
Hashes do not prove that values were computed by the declared model or that a
producer supplied correct semantic values. Golden marker tests detect a known
permutation, not arbitrary malicious content with self-consistent metadata.

The source writes a fresh private spool and fsyncs it and its directory, computes
whole/object/fixed-frame digests, then seals the manifest in SQLite. Production
chunks can end at token boundaries. Readback preserves the planned token tile /
object boundaries and rechunks each object at exactly 32768 except its final
frame. Thus V=151936, three tokens as one object gives 56 frames, whereas three
one-token objects give 57. A 17920-byte token tail is not a legal nonfinal frame
of the three-token object. Original plan grouping is never changed implicitly.

Each synthetic Book has an explicit caller-supplied capacity <=2MiB. Each
reservation charges all payload plus 32768 manifest bytes before producing.
All records, roles and states count toward that Book's aggregate; stable id +
immutable plan allow idempotent reservation lookup, never another execution.
UNIQUE(owner,task) blocks splitting a request into more result sets or attempts
within that Book (it also disallows both roles for that request in one Book).
Source and destination use SEPARATE books and sequential commitments. There is
no atomic cross-node reserve, simultaneous-validity fence or real remote promise.
Revocation can occur between their checks. Partial and ambiguous reservations
remain charged. Both must be query-confirmed before production; this is a local
state-model condition, not production admission authority.

For three example tokens each book charges 1823232+32768=1856000 bytes; source
plus destination charge totals 3712000, not 2MiB. Four tokens exceed the per-plan
payload cap before any body call. Two 2MiB books provide at most 4MiB combined
logical capacity, not a global 2MiB limit. No cross-host resource accounting is
implemented. The books don't measure SQLite/WAL, filesystem allocation, Python
aliases, caller allocations, weights, hidden/norm, old/new KV, workspace or GPU
memory. At a read/serialization boundary multiple bounded copies can coexist.
The caller may retain returned frames; this API cannot bound caller RSS.

Lifecycle: RESERVED -> ADMITTED -> SEALED -> READING. No ACK/completion/release
or resumable reader API exists. A persistent retired flag is separate from a
SEALED historical fact. Write/fsync/commit failure or cancellation isolates;
commit-success/response-failure retains SEALED + retired=1. Reconstructed Spool
or reopened same-version Book cannot consume or read retired records. Body is
called at most once for a reservation; failed work is never replayed. New copied,
rolled-back or independent databases defeat these LOCAL-only constraints: real
persistent global/session stage qualification must be supplied separately.

Read admission consumes SEALED once. Iterator continuation stays on its first
consumer thread; failure/early close isolates. Even successful exhaustion stays
READING with all charges retained. Ordinary ACK, active readers, deletion or
fsync uncertainty cannot release quota because this prototype exposes no release
or delete method. Capacity exhaustion is intentional until a separately designed
supervised disposal protocol is proven. Test-harness temporary-directory cleanup
is not a product quota-reclamation implementation.

Private same-UID filesystem and direct Python callers are trusted. Writer fd and
read fd/path identities include device/inode/size/mtime/ctime. Whole-file digest
is checked before yield and each frame digest before yield, detecting tested
replacement and between-frame edits. chmod is not immutable storage or protection
against a hostile same-UID writer capable of modifying database metadata too.
File fsync and SQLite seal are not one atomic transaction; orphans retain charge.
No power-loss durability/reopen recovery claim; parent-directory creation and
filesystem faults need separate crash testing. Exceptions are internal only.

The tests invoke the REAL ObjectReceiver.validate with a controlled ordinary
context, bypassing constructor and replacing check. This verifies fixed-frame
format only: no authentication, TLS, network, actual receiver or node is started.
Actual spool frames are packaged by the existing payload() function and checked
by that method; this is not a production transport producer integration.

TDD evidence external to repository: missing-API RED, actual column-order / short
frame counterexamples, missing callback-owner RED, and review-derived cross-
instance retirement/file-replacement REDs, then guarded GREEN. Tests cover byte
boundaries, finite formats, object grouping, identity changes, concurrent shared
quota, cross-role charge, committed-reservation/lost-response query, write/fsync/
seal failures, thread barriers, source changes and no retry/release. Events and
barriers establish ordering; timeouts only detect deadlocks. No sleep race oracle.
No real KV/GPU is involved, so failure-state tests do not prove physical stop.

Run only the explicit test module using the external guard in the evidence pack.
Do not discover the repository's model/node test suites. This prototype is not
imported by any production path and is not published in this increment.
