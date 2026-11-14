# Read-only input declaration (not task admission)

An opt-in local `InputDeclarationReader` uses an existing ApplicationService and
its user verifier. Default construction rejects every operation. Bootstrap must
supply an exact ModelIdentity declaration and preprocessing version/contract pin;
there is no HTTP route, new application mount or client-selected owner/model.

On prepare: authenticate, acquire the existing Authority lock, reauthenticate,
verify its live/path trust gate and exact v12 ledger/schema, require an owner's
STORED text-origin task without a plan, validate source metadata, resolve canonical
stored binding, fully read/check its token snapshot, compare model RECORD digest
and contract/version pins, then reauthenticate after the read transaction exits.
The result is immutable metadata without raw text or tokens. Weights/config hashes
are separate from the full model-record digest. Current draft edits do not rewrite
historical source declarations. In particular text/render hashes are NOT rerun
render verification, model_status stays unverified_declared and tokenizer_status
stays internal_metadata_only. execution_allowed/computation_verified stay false.

This is metadata consistency, not tokenizer/model correctness, persistent source
identity, global uniqueness, a one-shot execution authorization or a P1 Task or
Session. No nonce/reservation/registry is created, queried or imported. A later
execution bridge still needs trusted tokenizer/config/layout/policy binding and
an atomic source claim in the existing coordinator; this module implements none
of those. No v12 deny/trigger, retention policy or production route is changed.

Only the existing transaction boundary and filesystem snapshot verifier are used;
reads still invoke fsync through read_input. No DB rows or artifact files are
written by prepare. File atime and OS caching are not covered by byte-hash tests.
This intentionally denies corruption without offering repair or new execution;
existing storage mutation APIs keep their prior behavior. Authority lock protects
supported writers in this one authority, not same-UID hostile writers, rollback,
concurrent outside DB handles, another process or multiple registries. Exact
private types/helpers couple this slice to the pinned source version.

Tests: synthetic finite metadata/tokens, real temporary SQLite/files, existing
manual test-only v12 assembly and verifier. No tokenizer, model, Worker, network,
TLS or deployment. Initial 17 tests failed for module absence (scaffolding RED).
The final suite has 20 tests. Two separate mutation REDs demonstrate that removing
lock reauthentication allows file read after revocation and removing record-pin
checking accepts a mismatched model; restored implementation passes. Three later
characterization tests cover cancellation, byte preservation and corrupted source
binding; do not misdescribe all 20 as separate observed initial behavior failures.

Run: `python -B -W error::RuntimeWarning -W error::ResourceWarning validation/input_declaration/test_declaration.py`
The test installs network/subprocess/framework import guards. The full repository
suite is deliberately NOT run because it includes network/model experiments.
