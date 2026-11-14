# Fresh V2 plan journal (experimental, not execution)

`V2Journal.create` is explicit opt-in and requires a nonexistent file in a
trusted, stable local parent. It does not open, upgrade or copy an existing DB.
It reuses P1Store's schema/lifecycle for one task/session and adds versioned plan
records; no old source code or old DB is modified. This is not a general-purpose
reopen API or mixed legacy/V2 store.

`authorize` is a trusted synchronous context-manager factory yielding the current
validated owner. The caller must hold its authentication/revocation fence until
context exit; an owner string supplied by an untrusted client is NOT authentication.
There is no HTTP/login implementation. The verifier must not reenter this object.
SQLite serializes DB state, not external session revocation. Creation and methods
require this boundary explicitly; successful historical replay is authorized anew.

Registration reads current task/session/stages inside BEGIN IMMEDIATE, requires
matching preparing state and exact epoch for new writes, then validates the full
V2 plan. Plan plus global event insert commit together. The original event ID is
required for replay; a different event cannot replace an existing epoch plan.
Receipt replay is explicitly historical and grants no execution permission, even
if the task has since cancelled/advanced. Registration is not receiving/computing/
verifying/committing tensors. No V2 transition/progress or Worker integration exists.

Fresh-only triggers reject INSERT/UPDATE into old tile/state tables for the V2
session even before plan registration. They are accidental-misuse protection,
not a security boundary against an administrator changing the schema. The private
P1Store is not a supported public escape hatch. Old prefix/progress/completion
methods cannot interpret V2 plans and are not exposed. Cancel remains task-wide,
not epoch-scoped; metadata cancellation does not prove computation stopped.

One per-object lock serializes calls and close; all SQLite connections are scoped
by existing transaction cleanup. Close fences later operations, deletes nothing.
Creation uses exclusive file creation, but bootstrap is multiple transactions:
if bootstrap fails, a partial file may remain and is explicitly not reopenable.
No automatic file deletion is attempted, including on cleanup errors. Operators
must inspect residual files separately. This deliberately chooses preservation
over claiming an unproven safe cleanup. Existing files remain untouched.

The inode check detects observed replacement, NOT concurrent TOCTOU prevention.
Stable trusted paths and non-adversarial same-UID writers are prerequisites.
No hostile path/SQLite tampering, power-loss recovery or cross-process auth claim.
If commit or authorization-context exit fails, the caller must treat the outcome
as uncertain, never infer absence or retry with a new identity. No persistent
recovery protocol is provided. This candidate is not production ready.

Tests use temporary SQLite, ordinary records, explicit fault injection and thread
events only; no models, GPU, tensor framework, nodes or network. They cover fresh
refusal, authorization on replay, cancellation ordering, stale epoch, global event
conflicts, rollback, trigger rejection, observed replacement, close and partial
bootstrap preservation. They do not exhaust all faults or certify durability.
