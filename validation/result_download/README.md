# Owner-authorized committed result download (offline candidate)

`result_download_api.create_app(service=...)` is an opt-in ASGI adapter, not mounted
by any existing application. Its default service is absent and denies. Only
`GET /tasks/<32 lowercase hex task ID>/result` with a Bearer user credential is
accepted. No owner, result-upload idempotency key, cookie, admin or Worker identity
can substitute. The existing Principal/verifier contract is reused; a trusted real
verifier must check issuer, audience, expiry and revocation. Tests use only synthetic
identities. Fresh CORO_CREATED native coroutine verifier returns are closed without executing;
already-started coroutines and other awaitables are rejected without close, await or
cancel. Their creator retains cleanup ownership.

A new service/authority read path authenticates before waiting, again after acquiring
the authority lock, after complete fetch and immediately before the adapter invokes
its first send. The owner must remain identical. Missing, foreign, unfinished and
policy-limit refusals have the same fixed 404 response and do not poison the shared
authority. File/binding corruption retains existing fencing. Send occurs outside
the lock; send errors propagate without retries. Once bytes are sent they cannot be
retracted. This is neither continuously revocable delivery nor receipt/claim ACK.

The authority selects the sole owner/task STORED artifact ticket internally, never
requires a user's knowledge of the Worker upload key, and invokes the existing
fetch validation of the canonical manifest, bindings and complete files. Fetch's
new optional size/metadata parameters leave its original no-keyword path unchanged.
Whitelist metadata is returned only after complete verification. Old read_result
is untouched. No new ledger or persistent state is created, no artifact deleted,
no downloaded/claimed flag or temporary-retention promise introduced.

The fixed `result.json` attachment is a bounded JSON/base64 download package, not a
replacement storage manifest or generated text. It includes prefill_logits_v1,
float32-le, axes, per-tile start/count/shape/byte length/hash/data and manifest digest.
Owner, filesystem paths, assignments and upload keys are omitted. The manifest's
validated total and the collector independently enforce 4 MiB raw bytes; final
encoded JSON is capped at 8 MiB. These are logical limits, not RSS guarantees.
Computation_verified and execution_allowed remain false: synthetic floats prove
storage/read authorization only, not correct inference.

The existing test-cookie page is NOT connected to this Bearer adapter. Real user
verifier, production HTTPS mounting and full product assembly remain separate.
v12 plan denial and text_source_no_plan trigger are unchanged. Full product goal
(submit task -> parallel pool computation -> temporary results -> owner download)
is not demonstrated by this isolated read capability.

## Reproducible offline validation

`PYTHONDONTWRITEBYTECODE=1 python -W error::ResourceWarning -W error::RuntimeWarning validation/result_download/run_selected.py`

Explicit modules only: 21 new download tests and 5 existing ordinary
text_registration/test_authority_preflight tests. The first module installs audit
guards rejecting bind/connect/DNS/subprocess and imports of model frameworks/Workers.
No TLS fixture, listener, real network, model or Worker is run. Fixture uses actual
SQLite and immutable synthetic finite float32 files, with direct trusted test-only
publication into the existing ledger; it is not a production publication path.

Evidence: initial missing-API RED 11 failures; behavioral RED removes only the
post-lock authentication check and shows two provisional tiles read after revocation
(the later recheck still rejects, but cannot undo reads). Restoring the check yields
GREEN. Tests also cover late read corruption, both later auth checkpoints, cancellation,
body/range/identity rejection, declared size preflight, collector/metadata refusal,
encoded cap, no mutation/repeatability and original fetch/read compatibility.

Source is a current no-git candidate copy, not an asserted Git commit. Source pins
include cached files for forensic copy tracking; release/change comparison excludes
__pycache__ and pyc. Only the explicitly changed source/test/doc files are proposed;
no caches, archives or credentials are proposed for publication.

Compatibility note: the existing application_service TLS test import allowlist now
includes stdlib types and inspect (one-line metadata amendment, verified by AST only). That
network suite was NOT imported or run; overall TLS-suite compatibility is not claimed.

A second behavioral RED showed that closing a suspended verifier coroutine executed
its creator-owned finally. GREEN limits close to CORO_CREATED and asserts suspended
state and untouched finally until the test creator explicitly closes it.
