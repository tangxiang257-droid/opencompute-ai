# Private producer contract — synthetic-only validation

Fixed base e4d1893021c25f5a5bcea7f4ebaa4ca0ca8b8f3b. This is an isolated
interface/state-machine experiment, not a production adapter or authorization.
No model/GPU/Worker/node/network is imported or started. Existing StageOwner,
local_attempt, factory, production identity and transport paths are unchanged.

PrivateProducer requires an exact frozen Spec, existing synthetic Spool and a
synchronous ordinary-byte callback. No authorize callback means denial. A test
context must explicitly yield an exact Permit for the fixed owner and actual
Spec digest. Permit/Result.execution_allowed are permanently False: these are
synthetic declarations, not authentic identities or real compute capabilities.
Direct Python callers, callback implementation and same-UID filesystem are trusted.
No production path imports this module. No model adapter may treat it as a grant.

The Spec digest includes canonical complete plan binding/object grouping, hidden
width, actual hidden-input hash and exact size, full output size/charges and fixed
working accounting. Plan binding includes input snapshot, token/preprocessing,
model/layout/plan digests, owner/task/session nonce/attempt/epoch/connection.
This actual declaration digest supplements, not proves, a supplied plan hash.
Input must be exact immutable bytes, <=65536, exact declared length/hash and finite
LE float32. A path/reference is rejected. The callback writes token-major chunks
through the existing bounded Spool writer and MUST return None. Returning bytes,
manifest/path or a coroutine is not an alternative output route.

Source/destination each must already reserve full payload+32768 metadata. Their
confirmations are independent snapshots, not an atomic cross-node promise. The
new interface never substitutes reference size for full output charges. Synthetic
607744-byte output is charged 640512 in EACH disk book. It is accepted only in
this explicit synthetic experiment, not by the old 64KiB complete-output API.
The tests actually call old StageOwner and prove it still refuses a 65537-byte
result and a string reference; MAX_BYTES=65536/MAX_RESERVED=262144 remain intact.

A separate source-DB working pool counts input_bytes + 3*32768 + 32768 per consumed
call, aggregate <=262144, with no caller override or release. This is conservative
logical block/copy/metadata accounting, not measured RSS/GPU/model/KV bounds. Since
every nonempty call costs >131072, the pool currently permits only ONE consumption
per surviving book before exhaustion. Source/destination disk charges and this
working category remain separate: no global physical resource limit is claimed.
Caller-held values, callback allocations, full Tensor backing storage, SQLite/WAL,
filesystem overhead, weights/KV/workspace cannot be bounded by this interface.

Construction registers the exact Spec against the existing source reservation id.
It does not grant work or create new source/destination reservations. A changed
input/plan cannot rebind that id. Invocation consumes FRESH -> CONSUMED once in a
source SQLite transaction that also charges the working pool. Duplicate calls,
reconstructed adapters and same-book retries cannot invoke the body again.
Failures retire the private row and source reservation in ONE source transaction,
retaining charges and a SEALED historical fact. Callback or authorization-context
exceptions cannot be swallowed by a suppressing context. Stop requests fence
later writes but do not physically stop a running callback/thread/device.

Only successful authorization-context exit followed by private DONE registration
allows export. The optional private binding is checked by Spool's SEALED->READING
transition in the SAME SQLite transaction as read admission. Ordinary unbound
spool prototype behavior is preserved. Tests pause context exit with events and
try another Spool instance: read admission rejects, with no bytes yielded. Source
retirement and read admission serialize on that local source DB, not across hosts.

DONE is the local publication point. Observers may see it before the caller receives
the Result response; durable commit and response delivery cannot be atomic. A late
failure cannot recall bytes already delivered. Never infer safe recomputation from
a missing response; query history only. A read admitted before later retirement may
already have yielded bytes. This is not instantaneous revocation or physical stop.
If retirement writes fail, the method raises and the caller must quarantine storage:
no claim of successful durable retirement is made. The tested exit-failure case
leaves CONSUMED+SEALED after rollback; export still denies because DONE was never
registered, and CONSUMED still prevents reexecution. No reopen recovery protocol.

The old prototype assumes private trusted storage. Copying/deleting/rolling back
books or constructing independent ledgers defeats LOCAL uniqueness. Existing
persistent global/session-stage admission must be supplied before any future real
adapter. Ordinary Permit values are forgeable data; trusted callbacks and this
synthetic local state machine are not security boundaries for hostile callers.
No automatic migration, public route, real execution authorization, release/GC,
resumable stream, distributed transaction or real producer is added.

Tests use only ordinary bytes, temporary files/SQLite, threads and event barriers.
They cover default deny, actual input binding, full quota, shared working exhaustion,
returns-as-reference denial, nonfinite input, concurrent duplicate calls, running
cancel, context enter/body/exit/suppression failures, blocked pre-DONE export,
atomic local retirement and retirement-write rollback. A separate rerun preserves
all 22 prior spool tests. Real numerical equivalence, peak resources and crash/
power-loss/device-stop tests require a later explicitly authorized acceptance.
