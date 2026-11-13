# Fresh serial receipt-source execution seam (explicit opt-in)

This increment has no socket, TLS handshake, credential, loader, node or public
route. It adds ReceiptAttemptJournal, requiring enable_receipt_stage1=True.
Receiver defaults and LocalAttemptJournal defaults remain unchanged; no migration,
reopen, recovery or implicit execution is added.

## Actual seam and reuse

Use separate sender LocalAttemptJournal and receiver ReceiptAttemptJournal files.
Sender stage0 output is serialized into Envelope. The receiver's make_receiver()
returns the real Receiver, explicitly enabling payload retention and V2 decoding.
Its existing receipt/event/edge transaction also persists the complete bounded
frame. Failure anywhere rolls back all of them. Identical duplicate frame returns
the receipt without extra storage or execution; changed bytes fail. A receipt
whose retained payload has disappeared cannot be repaired by re-delivery.

DeliveryAttempt can own Delivery.release while receiving these frames, WITHOUT
consumer. Only after ALL upstream tiles have receipts does the trusted caller
construct make_stage1_owner and call it in token order. This reuses actual
LocalAttemptJournal admission/commit and StageOwner rather than a new executor.
The optional DeliveryAttempt consumer is deliberately not used here: replaying
existing receipts skips consumption and is not a way to bootstrap execution.
This first seam is serial full-prefix receipt collection, not network waves.

The receiver checks source frames, descriptors, receipt digests, receipt events,
fixed edge and plan/epoch at binding, admission and publication. It never treats
a local stage0 row as remote provenance; stage0 execution is disabled on this
journal. Stage1 input/output are stored as actual BLOBs. A prior attempt prevents
a new owner from claiming KV continuation. Queries remain history only.

## Authentication is an external prerequisite, not a test string

Trusted provisioning fixes EdgeBinding, expected PeerIdentity (node ID, SHA256
certificate fingerprint, connection generation), local receiver node and config
pin. peer_authorize(expected) is mandatory and must authenticate its live channel
and hold its revocation fence until exit. It must return an exact matching
PeerIdentity. Production assembly must obtain that identity from actual mTLS:
CA validation, client hostname verification, peer certificate possession and
unique certificate-to-node mapping. The dataclass, fingerprint string, local_node
and test contexts do NOT perform or prove any such authentication. There is no
default peer allow, credential creation, TLS setup or node claim from payload.

A separate owner fence and execution_authorize(spec) remain mandatory. The peer
fence surrounds bootstrap/receive and each execution transaction; owner, peer and
execution fences are held through local admission or output commit. Receiving a
receipt or releasing transport credit does not grant forward permission. A body
admitted before revocation can run; later publication may be rejected. These are
local transaction/revocation boundaries, not a distributed atomic authority check
at CPU body entry. Trusted external fence providers must enforce real revocation;
ordinary tests only verify the callback contract and refusal behavior.

The config pin is persisted in the fixed trust context, NOT verified against a
model here. Future CPU assembly must compare actual config bytes to that same
pin and pass the exact existing planned operation. This seam can run ordinary
callbacks; it does not independently authenticate their code or model math.

## Nonce/replay and storage trust

The trusted provisioner, never the peer message, chooses a fresh random 128-bit
nonce represented by 32 lowercase hex characters as the session_id BEFORE both
plans are fixed. This existing Envelope identity field binds the nonce without a
silent wire-format change. Syntax is checked here; randomness and global reuse
prevention across fresh databases are external requirements, not proven by this
module. Test fixtures use deterministic nonce strings and prove no entropy.

Connection generation is pinned to this journal. Reconnect does not change its
binding or authorize recovery. Old epoch/session, different node/certificate or
connection identity fails. Receipt duplicates never execute automatically. An
operator can defeat replay protection by reprovisioning the same session into a
new database: this design intentionally does not claim global replay protection.
Same-UID storage and assembly are trusted. A consistent replacement of frames,
receipt/events/context before owner binding is not cryptographic proof of original
computation; after binding actual payload comparison fences changed source bytes.

## Budgets, failures and stop

Each full frame remains <=65536 bytes; each input/output retains its original
64 KiB cap. The receiver 256 KiB logical budget includes retained full frames plus
stage1 input copies and reserved output. Full-stage budget is checked before
owner construction; each admission checks actual used budget again. Sender and
receiver quotas are independent. There is no shared/distributed atomic reservation,
credit-based persistent-data GC or RSS/disk/KV bound. Frame/JSON copies cost extra
memory. Receipt rows/context/events are not physical-byte accounted.

A receive transaction can succeed then fail on external context exit; query only,
not a new execution permission. Duplicate receipt lookup and stage1 explicit
admission are separate actions; any unknown admission/commit retires the owner,
leaves history and cannot be retried. Shutdown/connection loss does not stop a
running thread. Caller must stop admissions, retire owners and await started work;
physical process termination and independent resource limits remain deployment
work. No atomic two-ledger commit, durable KV recovery or real network numerical
acceptance is claimed.

## Independent-review hardening

The complete initial Stage declarations (not only assigned nodes) are fixed in
memory and in the receipt trust-context record. Current declarations are compared
on every receive/source read, including source rechecks inside admission/commit.
This closes a legal layer-split change that the existing tile-plan digest alone
does not bind. Missing frame with existing receipt and orphan frame with missing
receipt both fail closed; re-delivery cannot silently repair either half.
Opt-in retention also requires the exact journal-provided peer fence callback;
direct Receiver construction with a legacy string-only callback is rejected.
Trusted Python assembly may still deliberately mutate private internals, which
is outside this boundary; enforcing callback identity is not TLS verification.
