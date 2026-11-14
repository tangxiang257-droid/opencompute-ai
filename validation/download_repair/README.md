# Download regression restoration and original C acceptance failure

Historical evidence below describes the original RED. The current unpublished
candidate adds the explicit co-located route documented in
`validation/c_result_route/README.md`; the same HTTP 200 assertion is retained.
No source authority is replaced and no rows are copied.

Base 10993f7da3290349c4e7765484489898f1227dc2 plus the unchanged seven-file
C service candidate c7147da61ad9c3d92c5ce2e9120e85cde743ad635012d7eeb0c9761e567d99a7.
No candidate is published.

The old controlled_authority fixture scans ALL AST imports and checks exact sets.
Only snapshot_task_ledger's set was stale: _commit_published_input now locally
imports result_artifacts._Ledger to restrict explicit task-ID import to the exact
result-only type. Removing that production check would weaken isolation.
This repair adds only that dependency, keeps exact set equality, and additionally
checks the precise function/conditional/local-import form, name and absence of
alias. Negative tests reject unreviewed imports and moving/renaming that import.
The model/socket guards remain. No existing download assertion was weakened.

The original 21 download cases pass both on the fixed baseline with this fixture
repair and with the C candidate overlaid. They cover identity, other owners,
revocation, budgets, corrupt files, late read failure, response collection and
send failure. They publish ordinary fixtures directly into their application's
own ledger, so they cannot establish C integration.

The combined test_c_download.py preserves the REQUIRED HTTP 200 assertion and
originally FAILED with HTTP 404 after successful C ACK/RESULT_STORED/RECEIVED and
actual byte retrieval. It verifies the same current authenticated owner. C writes
the exact result_artifacts._Ledger while source ApplicationService._download_result
queries its own managed source ledger; the derived execution task is absent there.
The original download API has no result-location resolver or C authority binding.
This is an identified assembly gap, not proof of lost bytes or unauthorized access.
Do not skip, mark expectedFailure, change 200 to 404, copy DB rows, reassign the
application's private authority/ledger, or widen the result-only exact-type gate
in order to make this acceptance test pass.

Next minimum safe work: a reviewed read-only route from the authenticated user
and derived execution task to the original C result authority/ledger, bound to
current identity, task visibility, immutable result identity and actual storage
ownership. Prefer an explicit trusted central result proxy to the owning C service;
source and result authorities must remain distinct. If cross-host, use a separate
authenticated purpose-specific query transport, no execution grant and no history
query that recreates qualification. Do not auto-enroll a C peer or invent a resource
provider. Verify owner/refusal/revocation, corruption/no-partial-result, disconnect,
unknown commit and retained resource responsibility before enabling any endpoint.

The original repair alone did not implement the ownership/permission route.
Its RED evidence remains archived; the subsequent route is tested separately.
All candidates remain unpublished. No actual listeners, nodes, devices, models or credentials were used.
