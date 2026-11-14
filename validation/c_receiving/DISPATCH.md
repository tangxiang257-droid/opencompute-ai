# C ACK before installed B dispatch (private validation increment)

Base: 123ba86b0752844ac25787458a998fac676753d1. No model/GPU/node/listener is
started or authorized by this module. External identities, user task consent,
capacity and device completion remain mandatory. LAB fixtures are not providers
suitable for deployment; confirmation is explicitly synthetic in the combined
pipeline (the existing independent confirmation tests are separate evidence).

InstalledStageRole stage B now creates an explicit version-2 ControlJournal.
Before RESERVED -> CONSUMED, ControlService itself requires its exact original
BDispatchGate to obtain an online ACK. Raw ControlClient execute cannot bypass
this by ignoring CoordinatorControl ordering. Prepare/input-ready remain safe
before C activation. Legacy generic validation ControlService/version-1 records
are unchanged and do not become installed B production roles.

Bootstrap supplies a separate authenticated bounded B/C Channel plus current
identity/dispatch-ack policy. B binds the exact installed operation, committed
input, command/reservation, full C descriptor and fresh challenge. CDispatchGate
requires the exact ReceivingRole, its original live authority connection,
independent current receive permission/capacity, and RECEIVING after the actual
ResultIngress was attached. ACTIVE alone is insufficient. Only ordinary frames
cross the process boundary; no copied C handle/B permission is a receiving grant.

C durably occupies one gate slot before writing ACK. B persists that response
once; ACKED -> CONSUMED and control-call consumption share B's SQLite transaction.
The live original handle/channel, fixed binding/nonce/recipient and persistent
row are checked before consumption and again before the body, after asynchronous
credit release and after external provider exits. Query, reopening, retransmission
or a new gate wrapper cannot restore the live invocation. Unknown outcomes retain
all charges and occupied slots. There is no distributed atomic reservation.

C shutdown/revoke/receive failure aborts its independent ACK connection. A callback
observes original C control-reader termination even when nobody calls serve_once
or close. B denies after it observes disconnect. Remote C revoke after ACK but
before B observes it can still race with B computation; no distributed atomic
revocation or physical device-stop claim is made. A historical C row may still
say RECEIVING after control EOF until owner cleanup writes UNKNOWN; this is a
snapshot, never a permission. Completed/committed side effects are not undone.

C history uses a distinct purpose-specific query codec and independent SQLite
mode=ro/query_only connection. Trusted local bootstrap chooses the store; wire
requests cannot choose paths. Current principal authorization sees the original
immutable descriptor, not a rewritten connection. Directory/file inode, schema,
incarnation and process checks pin the local store. Recheck after credit release
and inside the actual writer fence denies revoke while waiting for the write
lock. Replies explicitly deny execution, receiving and resource qualification.
Only local C stores are supported; issuer shared-store history is not added.

No silent migration: old control v1 stores keep old history; opening a B v2 store
requires require_receiving_gate=True and remains history-only on reopen. Local C
stores add a gate table only when freshly created. Filesystem clone/rollback,
malicious same-process mutation and globally unique admission are outside the
trusted local-filesystem boundary. Extra SQLite metadata is not measured physical
capacity; payload/manifest charges and existing hard frame/BLOB limits remain.

Each new B/C ACK Channel endpoint has 32,768 bytes of logical credit (65,536
across the two endpoints), in addition to frame headers, queues, SQLite records
and other retained buffers. Deployment must cover these in the same existing
host/transport resource commitments; the synthetic provider's successful hold
is not evidence of physical capacity or a complete memory reservation. No new
capacity service or automatic quota increase is supplied by this increment.
