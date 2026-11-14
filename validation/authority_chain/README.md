# Actual action authority in the isolated one-token chain

Fixed base: bf280ff79853d39482153bce3734e6484ff08db5 (PR #21).
This increment does not enable real execution or change v12/v13 restrictions.

`private_experiment_control.create_action_server` connects the original live
installation and `CoordinatorControl` to the existing `ActionAuthority` and
`ActionServer`. No action replies are synthesized by this test. A single
`control-bound` row is inserted into the existing `short_actions` table, under
the original user/task/policy and central resource checks. It is permanent
binding history, not a permission, new quota reservation, or portable token.
The same-boot live binding is published only after all contexts exit. Failures,
unknown commit, cancellation, or restart cannot reconstruct that binding.

The coordinator pins its authenticated role identity separately from the
role-observed controller peer hash; their connection generations are local and
must not be conflated. Stage B binding obtains the actual authenticated
`CommittedBInput.read_committed` / `input-ready` reply, which checks the original
role's committed received bytes and resource ID. The coordinator never reads
B's local database. Network awaits do not retain local identity/resource/SQLite
contexts. START and COMMIT still use the original action ledger and deadlines.

The fixture composes actual confirmation ASGI calls, source/import records,
online installation, separate A/B persistent resource and control records,
launchers, action servers/gates, A committed bytes, B receipt proof, C independent
receiving activation/ACK gate, spool/frames, persistent result and owner fetch.
Both action requests and replies traverse memory Channels. Each stage has
`control-bound`, START and COMMIT rows on success. C authorization is distinct
and uses its explicitly confirmed exact input-copy context.

## Evidence boundary

This is a synthetic integration test, not a deployed two-host experiment:

- Signing uses RAM-generated LAB keys; no real issuer is trusted here.
- TLS streams/certificates, clock error, user-scope resolver, outer task/node
  policies, central capacity promise and device/backend/loader are LAB providers.
- A/B use actual `RoleResources` and SQLite records, with LAB capacity/authority
  providers. Their presence proves no physical GPU allocation or exclusive use.
- Output is ordinary constant FP32 bytes, not model logits or numerical evidence.
- No model, Torch, weights, GPU, real node, network connect/bind or subprocess
  is permitted inside the guarded test. No private key is persisted.
- No new ledger/schema migration, listener, production provider discovery,
  execution permission, retry, refund, or distributed transaction is added.

`thin_host.run_once` still refuses real execution unconditionally. A deployment
still requires trusted identity/TLS configuration, a real task-policy and central
resource verification provider, actual capacity/exclusivity evidence, trusted
clock bounds and device/backend completion validation. A method named `hold`
is not proof those dependencies are available.

## Failure and concurrency semantics

Tests cover missing providers; wrong peer; scope/authority replacement; original
connection loss; one-shot/replay; resource denial; revocation before START;
revocation during authenticated prepare; cancellation while waiting; committed
mapping/START with lost acknowledgement; callback exit failure; B resource-proof
mismatch; history-only reopening; and no local locks across wire waits.

An Event-barrier test revokes while the ordinary stage-A thread is running.
The thread may finish and local effects may exist; central COMMIT is denied and
control becomes UNKNOWN. Cancellation, lease expiration, or thread/log status
never proves physical GPU stop. A previously admitted action may win before
revocation; no instant remote revocation is claimed. Unknown resources remain
charged, and query history is not re-execution eligibility.

The existing 5-second authority / 4-second role / 1-second RPC policy remains
hardcoded experiment policy, not measured latency or externally configurable
capacity. SQLite ordering assumes one controlled authority path/host; independent
copies, rollback and multiple authority roots are outside that guarantee.

Tests use events rather than sleep to decide races. Timeouts are failure bounds.
Run `python validation/authority_chain/test_chain.py` with repository and `tests`
on PYTHONPATH; the evidence bundle also includes the stricter import/audit guard.
