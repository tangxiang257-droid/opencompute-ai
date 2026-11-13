# Private fixed-byte receipt probe: local manifest and two roles

This candidate adds only `receipt_probe_manifest.py`, `receipt_probe_roles.py`,
their two ordinary test modules and this document. It reuses the reviewed Channel,
NonceCoordinator, PreparedReceiptConnection, send_handoff, bounded JSON parser and
strict contract validators. Existing default code and wire messages are unchanged.
Source baseline/candidate-file verification remains a trusted outer launcher
precondition. These helpers do **not** verify source pins or authenticate themselves.

## Local interface and ownership

```
m = validate_probe_manifest(raw_bytes, trusted_manifest_sha256=local_expected_hash)
sender_report = await send_probe(raw, local_expected_hash, reader, writer, tls_config,
                                 emit=local_callback)
receiver_report = await receive_probe(raw, local_expected_hash, reader, writer,
    tls_config, coordinator=local_coordinator, fresh_journal_path=local_path,
    emit=local_callback, prepare_fence=optional_local_async_barrier)
joined = join_probe_reports(raw, local_expected_hash, sender_report, receiver_report)
```

Each role takes ownership of its already-authenticated stream at entry and closes
it on both success and failure. Neither role opens, connects or listens on a socket,
creates credentials, launches processes, reads model weights or executes a model.
The sender has no coordinator, reservation, journal or execution callback. The
receiver owns registration of the complete manifest with the supplied live local
coordinator, then calls PreparedReceiptConnection.prepare with its fixed internal
execution denial. Only the stage-1 receiver slot is claimed. The coordinator itself
is caller-owned and is not closed by the role.

The expected raw-manifest SHA256 must arrive from trusted LOCAL assembly, never
from an untrusted peer message. There is no signature or online manifest negotiation.
Raw SHA and public certificate fingerprints are declaration bindings, not peer
or model authentication. The stream must already have completed trusted TLS
certificate verification. Both roles check actual SSL metadata: CERT_REQUIRED,
TLS version, actual peer DER hash and the exact local peer-map entry. Every client
endpoint additionally checks the actual `ssl_object.server_hostname` equals the
manifest's expected name and hostname validation is enabled. Server endpoints do
not claim to perform client hostname validation. Fake metadata tests prove only
local checks, not cryptographic or end-to-end TLS behavior.

## Strict fixed manifest

The bounded JSON object is at most 65,536 bytes. Duplicate and unknown fields,
nonfinite numbers, wrong types, omitted normally-defaulted contract fields, bad
raw hashes and inconsistent declarations are rejected before H or database setup.
Complete Task, ResourceLimits, Session, Stage and Tile records are explicit. The
manifest includes config/layout declarations, ordered sender/receiver nodes,
different public certificate fingerprints, expected server DNS name, fixture
payload hash and fixed budget. Local paths, private keys, reservations and permits
are not fields. The Task payload reference is the literal fixed-sample label.

Only a fixed ordinary-byte fixture is supported: three batch-one float32-one
payloads, token shapes `(1,2,24)`, `(1,2,24)`, `(1,1,24)`, followed by a repeat of
the first frame. Unique payload total is 480 bytes; four transmissions total
672 payload bytes. The fixture hash covers payload0 || payload1 || payload2,
excluding the repeat and all Envelope headers. Every complete outgoing Envelope
including header must fit 65,536 bytes. Channel payload capacity is 65,536 bytes
and one tile. The existing prepared receiver also checks its complete-frame cap.
These logical bounds are not process RSS or total filesystem bounds.

The 32-hex Session nonce is this version's run identifier and is copied explicitly
into each report as both `session_id` and `nonce`. Syntax, matching raw digest and
matching reports do not prove randomness, global uniqueness or freshness. Trusted
outer assembly must choose and distribute a fresh one-time run/nonce and collect
its two role results. A reset/copied coordinator or reports reused from different
connections with the *same* manifest are outside a pure join's freshness proof.

## Readiness, receipt and failure semantics

The sender uses existing H/D/C messages; its first send waits for the prepared
receiver's H. The receiver claims and registers its fixed plan/ledger before H,
and does not wait for peer H or first D to prepare. A D header observed while
unprepared is rejected without reading its body. Buffered data first processed
after readiness cannot reveal when the sender originally sent it.

Each of the four receives uses the actual BoundReceiptConnection and fixed
coordinator-backed journal. Task/owner/peer/plan fences are rechecked by the actual
receipt operation. Read-only SQL snapshots retain only per-frame hashes/lengths,
receipt plan/tile/digest, event-field hashes, attempt-row counts and release count;
no arbitrary DB payload or exception text is exported. The fixture permits
independent reconstruction of all expected frame and receipt/event hashes. Success
requires three retained frames/receipts, four releases and zero attempt rows. No
sender computation provenance is asserted.

Credits are consumer capacity releases, **not** durable receipt acknowledgments.
At the sender's C4 completion checkpoint a live stream with no observed failure
may qualify. A closed stream qualifies only for empty EOF at the observed next
header boundary. A private, exclusive readexactly proxy tracks header/body phase
without additional reads or byte transformation: an empty EOF in a D body of
length five is not mistaken for header EOF. Reentrant reads poison the observation
state. Already observed protocol errors, partial headers/bodies, timeouts and
reader cancellation fail even if C4 arrived. The checker also sees proxy failures
before Channel's own error propagation. This checkpoint says nothing about future
unprocessed bytes or future peer behavior.

Pre-registration rejection may leave no coordinator session or slot. Once local
registration is committed, the nonce cannot be re-registered. Post-claim failure,
cancellation or uncertain receipt outcome does not free the stage-1 slot or cause
resend/rebinding to another path. Partial files/commits are not represented as
rolled back. Failed reports can contain earlier validated checkpoints; their
absence of a later record is not proof that no later side effect occurred.

## Local reports, cancellation and join

`emit(kind, safe_dict)` is a synchronous LOCAL evidence callback. It receives
copies of small allowlisted observations, not authorization objects, paths, raw
exceptions or arbitrary DB contents. Its output is not a remote wire protocol or
an execution grant. An emitter failure fails that role; there is no retry of
unknown evidence/receipt side effects. Final `role_done` emission occurs only after
owned cleanup settles. The function's returned report remains authoritative for
local joining if a callback wrote something and then threw; a callback record by
itself is not a successful return.

Ordinary failures return safe failed reports. Cancellation first waits for owned
cleanup and then raises `ProbeCancelled`, an asyncio.CancelledError subclass whose
`.report` carries the safe cancelled outcome. A caller collecting both tasks must
retain both independent outcomes, not replace one failure with the other's success.

Join revalidates the trusted manifest, exact role, digest, explicit nonce/session,
fixture/frame mapping, receipts/events and both role outcomes. It also requires
complementary TLS client/server observations and equal negotiated TLS versions.
Missing, failed or mismatched reports cannot pass. A sender reporting C4 cannot
substitute for the receiver's durable SQL evidence. Reports are unsigned local
observations: matching them is not proof of the same physical TLS connection,
authenticated time, remote witness, global replay prevention or fresh authority.

Existing send/receive/credit and stream-close I/O waits use three-second limits.
The trusted optional preparation barrier and synchronous emit callbacks have no
hard wall-time bound. Cleanup waits for owned reader cancellation and stream-close
settlement and does not intentionally detach work on another cancellation; this
is not an absolute total-process deadline. A trusted external supervisor is needed
for hard wall/CPU limits. No such supervisor is added by this module.

## Product and evidence limits

The eventual user acceptance path is: submit a task to the server → the compute
pool runs the same task in parallel → temporarily store the result → the user
retrieves it. This receipt-only seam is only intermediate ordinary-byte wiring;
it implements or proves none of those end-to-end product capabilities. It assumes
neither a result-retention duration nor privacy guarantees.

Verification here is offline only: real Channel/coordinator/temporary SQLite,
paired in-memory streams and SSL metadata stand-ins. Audit/import guards prohibit
socket bind/connect/DNS, subprocesses and model frameworks. Existing 279 ordinary
tests are retained. Separate RED logs include permissive missing manifest fields,
shared fingerprints/oversize frame checks, incompatible report joins, closed-first
C4 handling, C4 followed by malformed/incomplete frames, and read-proxy reentry.
No real network, certificates, nodes, model weights or GPU were used.
