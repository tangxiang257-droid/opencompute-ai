# Co-located C result download

Unpublished, fixed baseline 10993f7da3290349c4e7765484489898f1227dc2.

Trusted bootstrap explicitly calls `LocalCResultRoute.install(service, receiving,
authorize=dedicated_download_policy, enabled=True)` **after** C RESULT_STORED.
The route borrows the exact original importer/IMPORTED slot/issuer and C ledger.
It stores no second result record, reservation or permission. An absent route
retains the original source-ledger download path; a failing C route never falls
back. Same-task route replacement and aliasing a route under another task fail.

Original owner, current signed user admission, original confirmed export scope,
source revision, expiry, import context, original C paths/database identities and
committed manifest digest are rechecked. A dedicated `result-download` policy
is also mandatory: receiving/export permission alone is insufficient. Original
identity/capacity providers remain mandatory. The source authority async lock
covers local fetch only. Synchronous USER/task fences and callbacks do not span
network awaits. Checks run before/after payload/metadata callbacks, after policy
exit and before the HTTP adapter's first send. Data already sent cannot be
retracted; callback return and logging do not prove device completion.

This only supplies central co-located C routing for the same live experiment.
Restart, original credential expiry or loss of the issuer's live registration
still refuse. It is not a cross-host locator or a promise of indefinite offline
retrieval. Trusted bootstrap must supply a real dedicated download policy; test
LabProvider is not installed by this module. No listener, GPU or model is run.
Read retries are idempotent; execution, UNKNOWN recovery and quota release are
not provided. C manifest/files are validated by the existing `_Ledger.fetch`.

`test_c_download.py` keeps the original expected HTTP 200. `test_route.py` uses
the existing synthetic complete chain with real ledgers and direct ASGI messages;
`test_independent_route.py` preserves the independently discovered task-alias RED
and callback/slot/route replacement checks. The old 21 download tests remain
unchanged. The signed second-user test is last: LocalAdmission observes a new
credential by clearing its old cache, which can invalidate an active runtime.
Refreshing the old credential is not a recovery protocol.
