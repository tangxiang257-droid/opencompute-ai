# Private single-coordinator nonce provisioning

This opt-in module creates no sockets, credentials, certificates, model, remote
endpoint or public route. Its Linux advisory lock and SQLite ledger coordinate
one trusted Python provisioning authority, not a distributed consensus service.
Existing low-level journals and BoundReceiptConnection remain available for
experiments. They do NOT gain a cross-database guarantee when used directly.
Trusted assembly must route ALL provisioning in its authority domain through one
stable registry and its create_sender/create_receiver methods.

## The concrete gap

The existing Channel adapter latches apply to one Channel object; journal session
and attempt primary keys apply to one file. A regression against the unchanged
low-level implementation used two fake SSL Channels, two new receiver databases,
the same nonce and plan, and an ordinary callback. It executed the same stage-1
range twice. Different connection generations do not by themselves prevent that.
The low-level behavior is retained and is explicitly outside this new entrypoint.

## Scope and records

NonceCoordinator.initialize exclusively creates a new registry. open_existing
requires an existing regular file, takes a lifetime nonblocking flock on that
inode, checks protocol/owner/schema availability and SQLite quick_check, and
never creates a missing file. Trusted stable parent directory, local filesystem,
same-UID storage, callbacks and Python assembly are prerequisites. These checks
are not an adversarial filesystem sandbox or a full schema/rollback attestation.

register accepts an externally chosen fresh 32-hex nonce and validates the full
Task, Session, Stage declarations and V2 plan. It records their canonical contents,
plan digest and config pin under a UNIQUE session nonce. Changing epoch, nodes,
plan, task or config cannot reuse that nonce. The syntax check does not establish
entropy, certificate provenance or randomness. No model is loaded or authenticated
by these pins. The current private receiver assembly explicitly uses wire/input
version 2; it does not silently upgrade arbitrary wire mappings.

The only returned reservation is a process-local object tied by identity to this
live coordinator. It is not a remotely transferable grant, signed token or
sandbox against hostile Python introspection. An existing registration is never
reissued, even when identical. Query returns history and execution_allowed=False.

Two predefined slots, stage 0 and stage 1, allow the intended two-stage session,
not two independent sessions. create_sender and create_receiver atomically claim
their slot before touching an execution journal, recording its absolute path and
(for the receiver) the fixed Channel peer node/certificate/connection generation.
Neither API exposes a reusable slot-permit token. The same factory call owns the
claim and constructs/registers the journal against the stored exact plan. A
second invocation for that stage fails, even with a fresh path and connection.
The sender uses LocalAttemptJournal; the receiver uses BoundReceiptConnection and
ReceiptAttemptJournal. Existing per-tile attempts still prevent duplicate calls
within the one bound journal. Execution_authorize remains mandatory and separate.

## Failure, concurrency and lock boundaries

Sessions and slots are permanently burned. Registration and slot claim each use
BEGIN IMMEDIATE plus a database uniqueness constraint. A slot commits BEFORE its
journal factory runs. Journal creation or plan-registration failure leaves the
slot consumed and may leave an unusable partial file. There is no cross-database
atomic transaction, automatic cleanup, retry, grant transfer, release or repair.

If COMMIT throws, including a test-injected exception after successful commit,
the coordinator retires and returns no new reservation/journal. External owner
context exit failure after a committed mutation also retires it. Observing an
exception is not proof of rollback. Reopening can inspect committed history, but
cannot turn it into permission. If commit truly rolled back and no permission
escaped, later registration may succeed; this does not authorize repeating an
already started operation.

Normal acquisition order is journal -> external owner -> registry lock -> peer
-> execution fence -> local journal SQLite. Registry mutation never calls a
journal factory while holding registry/owner locks. Initial creation uses a new,
unpublished journal; no existing journal lock is awaited under the registry.
External authority callbacks must respect the same order. Admission, receipt and
commit consult the live registry declaration/slot fence, not only a saved token.
No registry fence is held over a model/ordinary-operation body or network await.
SQLite calls are synchronous with a one-second registry busy timeout; this is not
an instantaneous revocation guarantee. The pre-existing journal can still block
its owning loop on its own longer SQLite timeout.

## Restart and what this does NOT establish

Closing/retiring a coordinator fences its old objects from future local receipt,
admission and commit. open_existing has no old in-memory reservations: previously
registered nonces, including those with no claimed slot, are history only. A new
nonce can be registered. There is no KV restoration, old connection continuation,
old permit deserialization or restart admission. The old operation may still be
running elsewhere; reopening, changing generation or rejecting its later commit
does NOT prove that an old device or thread physically stopped.

The guarantee is confined to one unchanged, unrolled-back authoritative ledger.
Copying the database to another independent authority, restoring an old backup,
deleting/reinitializing it, or deliberately using another registry bypasses that
guarantee. Inode checks detect replacement for the current live object only, not
historical rollback or same-UID malicious edits. No global nonce registry,
distributed leader fencing, external monotonic authority, authenticated control
protocol or hardware identity is provided. There is no GC, TTL, physical disk/RSS
bound or durable-KV recovery. Permanent records can grow; operational retention
and disk-capacity policy remain future work and must not recycle nonces.

## Evidence boundary

Tests use temporary SQLite, ordinary callbacks, thread barriers and existing SSL
metadata/writer doubles only. They cover duplicate nonce/changed epoch, independent
stage slots, cross-connection/new-database rejection through the new factory,
competing stage claims, claim-before-factory failure, committed-but-return-lost
registration/claim, owner context exit failure, close/reopen, missing/corrupt and
replaced files, exclusive-open refusal and unauthorized mutation. The baseline
replay regression demonstrably invokes two ordinary stage bodies. The coordinated
regression allows only one body and rejects the second factory before a new file.
Fault injection around COMMIT is NOT a real process crash, power-loss test or
storage durability certification. No actual TLS handshake, node, process restart,
network, real model, credential or deployment experiment was performed.

Independent review found that the initial sender path recorded full Stage
configuration in the registry but did not compare it against the sender journal:
a later legal layer-split change still executed. A targeted ordinary RED reproduced
that defect. The coordinated sender now checks complete Task/Session/Stage records
and exact plan digest at owner preparation, admission and commit. This is a private
sender subclass; ordinary low-level LocalAttemptJournal behavior is unchanged.
Since it compares the fixed records exactly, changing even their state can fence
coordinated sender execution/history queries; registry query remains history-only.
No attempt is made to reinterpret changed state as safe recovery.

A second independent RED showed receiver Task privacy/cost metadata was not
compared to its initial full declaration: changing privacy_level and cost_cap
still allowed stage-1 execution. ReceiptAttemptJournal now snapshots the complete
Task and Session records at creation (including privacy, payload reference, costs,
deadline and resource limits), persists them in its receipt trust context, and
compares the actual current records inside the same SQLite transaction before
receipt commit, owner preparation, admission and output commit. It normalizes only
the existing mandatory Task QUEUED -> PREPARING initialization transition. This
strengthens the ordinary low-level receipt journal too, but does not give that API
cross-database nonce uniqueness. Its full Stage snapshot remains unchanged.

New negative tests cover privacy mutation before receipt with both receipt/frame
writes rolled back, combined privacy/cost/deadline/payload-reference mutation
before owner creation, mutation after owner binding before admission with zero
operation calls, mutation inside the ordinary body with ADMITTED retained and no
output commit, and changed Session declarations. A body that was already admitted
can execute before such a change is observed; rejecting publication is not proof
of undoing prior computation or reversing a privacy exposure. Live external owner,
peer and execution authority remain necessary; this fix does not weaken privacy
fields into advisory metadata.
