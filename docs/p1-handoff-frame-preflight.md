# Complete-envelope handoff preflight (private, no remote runtime)

Based on PR #21 `27aa7c25f7ec8c3495d0a03704c49d04ad8ff894`.
The local attempt accepts an output up to 65536 bytes, whereas Receiver accepts
an entire serialized envelope up to 65536 bytes. Those are different budgets.
`send_handoff(channel, envelope)` now uses the unchanged receiver cap. Channel
serializes exactly once before reserving credit or writing, measures actual
header + prefix + payload + digest, and sends those exact checked bytes.
Oversize is refused; there is no truncation, splitting, hidden quota increase,
or fixed guessed header allowance. Header identity lengths and numeric widths
can change the largest acceptable payload. Outer Channel kind/length framing
is not included in Receiver's envelope limit and is not confused with it.

The optional Channel.send max_frame_bytes is a stricter complete-envelope cap;
default generic Channel size policy stays unchanged. Handoff callers must use
the wrapper: this is not an automatic cap on unrelated generic Channel users.
Successful send is not a persisted receipt, computation result, execution grant,
or retry permission. This patch does not implement a two-node model path.

## Next two-ledger contract (design only)

Use separately provisioned CA/certificate/key configuration, mandatory mutual
TLS verification, client hostname verification and unique certificate-fingerprint
to node-ID mapping. Bind both endpoints to a trusted immutable task/session,
owner, epoch, plan digest, model/config/layout pins and stage assignment. Bind
each receipt to source peer, tile/token range, dtype/shape, payload digest and
a stable attempt nonce agreed in the trusted session plan. No message self-claim
or Python mock constitutes authenticated identity. Existing Envelope has no nonce;
its future encoding/binding version must be explicit, never silently changed.
Connection generation is not a fresh execution grant. Same identity/same bytes
may query a receipt, never rerun; same identity/different bytes must fail.

Sender local output commit and receiver receipt commit are separate transactions.
Receiver uses its own persisted receipt-bound StageOwner admission and completion
fences, its own quota, and consecutive local KV state; no shared-database source
query or distributed atomicity claim. Receipt/credit release does not grant execute.
A lost acknowledgement or disconnect leaves execution potentially unknown: stop
new admissions, retire both instances, only query matching durable events. Do not
retry computation or resume KV from receipt prefixes. Remote cancellation is not
proof of physical stop: require drained work or verified worker-process exit.
Resource declarations/Channel credits are not enforced RSS, disk or CPU limits.
Real identity, isolation, network and model-combination acceptance remain future
work; this patch uses only ordinary serialization and temporary SQLite tests.

## Review hardening

Treat dynamic encode overrides as untrusted serialized input: require exact
immutable bytes, decode and fully validate the frozen frame, then check grants
and calculate credit from the decoded wire identity/payload, not caller instance
fields. Invalid frames or unauthorized wire identities are rejected before debit.
A different but valid authorized wire payload is charged at its actual byte size.
This is not a sandbox for hostile Python code or arbitrary monkeypatching of
modules/channel state; it closes the mismatch between encoded wire and caller
fields, including an exact Envelope instance with an overridden encode method.

Pre-encoding and decoding consume CPU and temporary memory before rejection.
The ordinary Envelope encoder can serialize/hash up to its generic 16 MiB
payload limit, so the 65536-byte handoff admission cap is NOT a transient-memory
hard limit or a zero-cost rejection. A hostile overridden encoder is arbitrary
in-process code and is not resource-bounded by this helper. This change neither
claims nor implements a process memory sandbox or pre-allocation size predictor.
