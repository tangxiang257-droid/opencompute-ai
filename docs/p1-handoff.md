# Experimental stage 0 -> 1 byte receipt

This connects actual Envelope decode to a fresh V2Journal plan and transaction.
It does NOT call prefill, Channel, Worker, tensor conversion, or computation.
PREPARING is deliberately the only accepted state: this is pre-execution metadata
receipt staging, not a running pipeline. Out-of-order planned tiles may be recorded;
no prefix, KV readiness, received/computed/verified execution state is inferred.
Payload bytes are NOT persisted, so the journal is not a recoverable input store.

Trusted callers explicitly provide EdgeBinding: session/task/epoch/plan digest,
layout_hash -> layout_version mapping and directed assigned-node pair. Layout
version is not inferred from the hash. Envelope.stage_id means producer 0 here;
recipient is exactly stage 1. First accepted receipt pins this edge mapping in
SQLite; later rebindings reject. Peer authentication is a synchronous injected
context factory, not a node name supplied by the message. It and journal owner
authorization must hold their external revocation fences through transaction exit.
This interface is internal only; no production authentication/TLS is implemented.

Each call uses real decode with expected task/session/epoch and an immutable full
frame limit of 64 KiB, including header/digest. In one BEGIN IMMEDIATE it rechecks
owner, peer, current task/session PREPARING state, exact epoch, plan hash and all
V2 declarations, assigned nodes, model, token range, tile and output descriptor.
The input counterpart was validated by the full-plan equality check. Original V1
wire protocol remains unchanged. The trusted mapping, not wire self-assertion,
provides the missing layout-hash/role context. Only float32 hidden output is used.

Unique (plan_digest,tile_id) records hash of the entire frame. Identical bytes are
a duplicate; different bytes, including alternative header encoding, conflict.
Receipt, pinned edge and global event insert are atomic. No raw payload is stored.
Digest is not authentication, numerical correctness, or proof of computation.
Duplicates still check live authorization/state/epoch; unlike historical plan
registration, cancelled/stale receipt duplicates reject. If a fence's __exit__
fails after commit, outcome is uncertain; identical delivery under valid current
state can resolve as duplicate, but cancellation can prevent this check. No
recovery/retry safety or absence claim is issued.

This bytes-only interface does not acquire or release transport credits. Caller
retains exactly one Delivery.release obligation regardless of acceptance, rejection
or duplicate. Delivery.commit is NOT business commit. No integration claim about
credit lifecycle is made until the separate transport experiment is incorporated.
No production gate, prior schema, default behavior or remote branch changed.
Fresh initialization adds two tables only for newly created candidates; previous
fresh experimental files remain unsupported for reopen/migration. All trusted
path, same-UID, bootstrap partial-file and no automatic deletion limits still apply.
