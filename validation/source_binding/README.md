# Source-only nonce registration, protocol v2

This opt-in mode extends the existing NonceCoordinator class and the SAME registry
SQLite transaction. It is NOT a second store, scheduler or execution capability.
The default v1 API and schema remain unchanged. `initialize(...,
source_namespace=<64 lowercase hex>)` creates a fresh exclusive protocol-v2 file.
The namespace is a trusted local provisioning declaration, stored with its owner
in protocol metadata; never derived from paths or supplied per source request.
It must identify the same source authority stably outside this module. Syntax and
persistence do not authenticate its origin or make it globally unique.

`bind_source_declaration(InputDeclaration, nonce)` validates every scalar/type,
model-record digest, inert status and the complete canonical declaration (<=4096
UTF-8 bytes). It requires the existing authenticated owner fence, then under its
existing registry lock/transaction inserts both sessions nonce occupancy and a
source row. The source primary key is `(namespace, owner, original task_id)`;
nonce and snapshot are NOT in that key. Nonce is independently unique across the
registry. Equal input bytes under different explicit task IDs are not deduped.

The result is an immutable data receipt with execution_allowed=False. It does not
create `_Reservation`, stage slot or journal. All v2 legacy register/reservation/
claim/sender/receiver entrypoints reject. This mode is intentionally NOT a bridge
into v1 execution. v12 text plan prohibitions remain untouched. InputDeclaration
is constructible data: accepted registration is a trusted caller's statement, NOT
proof that its source store exists, remains valid or authorized execution. Current
source/auth/model/tokenizer/layout/policy checks remain required by a future bridge.

Only one stable trusted unrolled-back file and trusted same-UID Python assembly
are in scope. The source namespace and owner are checked on every source operation;
exact v2 schema is checked too. Protocol, sessions and source rows have no-update/
no-delete triggers, no release, GC, quota reset or recovery API. This does not
protect against a hostile process replacing/rewriting the file, deleting triggers,
copying/restoring older databases, or creating another coordinator. Records can
grow without bound; no disk/RSS hard limit is claimed.

## Failure and reopening

A duplicate source OR nonce is an expected conflict, never successful replay.
Other source-write failures conservatively retire the live coordinator. Commit
failure before or after an actual commit, rollback/close errors and post-commit
authorization-exit failure do not return a receipt. If committed, both rows remain;
if not committed, neither is inserted. No source-store write or cross-store atomic
transaction is claimed. Do not mark source as executed, compensate by deleting,
recycle nonce or retry with another nonce after an unknown result.

`open_existing(..., source_namespace=<same pin>)` is ALWAYS history-only for v2,
even when a prior uncertain commit left no row. It cannot bind any source, old or
new. Otherwise an unknown pre-commit failure could be followed by a new nonce on
reopen. Protocol v1/v2 mismatch or wrong namespace is refused without migration.
A path rename fences the old live instance; opening the moved registry preserves
history-only behavior. Fresh initialization never overwrites an existing file.
Uncertain initialization can leave a partial unusable file; no repair is added.

The existing authorization provider must hold the live revocation fence through
the transaction. These are synchronous operations: SQLite can block (existing
1-second timeout). No distributed instantaneous revocation, physical operation
stop, source-auth atomicity or restart continuation is supplied.

## Verification scope

20 new tests use synthetic constructible declarations and ordinary SQLite/thread
barriers. Initial 19-case RED was missing source API, not a reproduced execution bypass.
A separate behavior mutation allowing writes after reopen reproduced a new-nonce
retry after an unknown pre-commit failure; the actual read-only reopen rejects it.
Atomic insert/rollback and commit-ack failures are injected, not real power-loss
or process-crash experiments. No models, workers, TLS, real network or production.

Run from repository root:
`python -B -W error::ResourceWarning -W error::RuntimeWarning validation/source_binding/test_source_binding.py`

Explicit previous ordinary suites were rerun separately (334 baseline, 29 download
and compatibility, 20 declaration tests). Broad full-repository test discovery is
not run because it would start restricted network/model experiments. This is not
CI success. Nothing is mounted or enabled by default, merged or deployed.

Independent review found a concrete strict-data validation ordering defect: calling
asdict on the model before checking primitive fields could invoke a field's
__deepcopy__ callback. A targeted RED observed two callbacks from a malformed
model field; fixed code validates original scalar types before constructing a
plain mapping or traversing the declaration. The regression now refuses without
callback or registry write. This is in-process data handling, not proof of a
remote code-execution exposure. Rollback and close-error behavior was inspected
but not independently fault-injected; do not claim exhaustive failure testing.
