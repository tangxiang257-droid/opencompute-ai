# Private Channel-bound receipt identity (no network acceptance yet)

Channel now snapshots peer node, SHA256 of the actual getpeercert(binary_form=True)
DER result, and locally generated random connection generation AFTER its existing
TLS checks: CERT_REQUIRED, TLS1.2+, client hostname check, and a unique configured
fingerprint-to-node mapping. Mutable public peer_node is not the snapshot. Actual
certificate chain/hostname verification is performed by the SSL connection setup,
not by a message, PeerIdentity field, fingerprint string or this adapter. Reader,
writer, SSL context and Python assembly remain trusted. Ordinary test stand-ins
validate this extraction contract, not a real handshake or cryptographic proof.

BoundReceiptConnection accepts the real Channel type, fixes its snapshot and
provides peer_authorize for ReceiptAttemptJournal. Journals remember the exact
external provider identity. Receiving through a different adapter/journal or a
Delivery from another Channel is rejected. It uses real DeliveryAttempt with
receipt-only semantics. It performs no connect/accept, certificate issuance,
model loading, task execution or default public import. Existing generic Channel
send/receive wire protocol and limits are unchanged.

## Liveness, revocation and ownership

identity_fence is synchronous and owning-loop-only. It holds an RLock while
checking fixed identity/generation, revoked/closed state and writer.is_closing().
No fence is held across network awaits or operation bodies. revoke_identity and
Channel abort mark the identity irreversibly revoked and close the writer. An
external thread must schedule revoke_identity with the owning loop, not call it
directly. This is not instant distributed revocation; synchronous work/SQLite's
existing busy timeout can delay that loop. Remote disconnection is known only
when local transport/reader state observes it. There is no heartbeat, OCSP/CRL
polling, certificate expiry timer or force-stop guarantee.

Trust configuration and grants are snapshots. Editing TLSConfig.peers does not
hot-revoke a live connection. The trusted controller must explicitly revoke the
old Channel on trust/grant changes; reconnect has a new generation and cannot
reuse an old journal. No dynamic trust reload is implemented.

Receipt source preparation and transactions use journal/owner -> peer -> optional
execution -> SQLite lock order. Bootstrap checks peer alone, releases it before
base initialization, then persists its context under journal/owner -> peer ->
SQLite. Failure at the later check can leave a fresh partial file, never execution
permission. Owner construction releases preparation locks before the underlying
factory reacquires journal/owner. Admission/commit still independently recheck
all authority and bound inputs. There is no peer -> journal acquisition edge in
these paths. External providers must not reverse this order or synchronously wait
for the owning loop. Tests reproduce the old two-thread cycle with event barriers
and diagnostic failed lock acquisition, then show the same schedule succeeds;
timeouts are only test safety guards, not the repair. SQLite remains synchronous.

receive_once first validates the journal/provider binding and live identity.
After awaiting receive it checks actual Delivery._channel identity. The Receiver
then checks the live peer fence again inside the receipt transaction: a queued
message is not accepted merely because it arrived before close/revocation.
Concurrent receive_once on one adapter is refused. Any cancellation, malformed
construction, receive/receipt/release failure or foreign delivery retires and
awaits Channel.close; repeated caller cancellation does not abandon that close.
No receive/release is retried. Construction failures can retire the stream with
credits still accounted: they do not pretend to send credit restoration. Foreign
Delivery ownership is not claimed or released by this adapter. Channel.close is
transport cleanup only, never proof that a remote operation stopped.

## Replay scope and remaining prerequisites

Each Channel can bind one receipt adapter; each adapter permits one journal
creation attempt, even if initialization fails. These process-local latches avoid
accidental rebinding, not malicious Python or global replay. The trusted provisioner
still supplies a fresh unique session nonce and fixed plan, local node identity,
config pin and execution authorization. No global nonce registry, recovery or
reconnect continuation is implemented. A new process/database could still reuse
an externally misprovisioned nonce; this module does not solve that problem.

Tests use ordinary SSL/writer metadata stand-ins, real serialized bytes and
separate temporary SQLite journals; no certificate files, sockets, handshake,
model or node are started. Genuine two-device mTLS identity and end-to-end model
acceptance remain required future work. Matching node IDs do not prove distinct
physical devices, and timing of local callbacks does not prove device parallelism.
