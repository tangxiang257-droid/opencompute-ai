# Bound control exchange and permanent stage consumption

Base: 36e8ca6c932f688607fa6098bc87760f58a63125. Private opt-in code only;
no public route, socket/listener, real identity provider, execution grant,
model/device load or resource allocator is started by default.

The existing one-token entry accepts an explicit ControlPair. Stage 0 and stage
1 bodies use the SAME existing loader/owner/driver/source/receipt/spool objects,
but only the control service invokes them, after its durable CONSUMED record.
The client receives ordinary wire history/digests, not a permit followed by local
recomputation. ControlPair still owns both endpoints in a local bootstrap; it
passes trusted operation closures locally. Neither closures nor local journal/
Result objects are remotely serialized. A real multi-host launcher remains absent.

## Wire and qualification

Two separate authenticated Channel pairs carry prepare/execute/query requests
and replies as canonical, versioned JSON in uint8 Envelopes. Payload <=32768,
complete envelope <=65536, one credit per endpoint; all four control payload
capacities total 131072 bytes and are included in pipeline_host admission.
Actual Channel grants, immutable peer metadata and fixed device/instance/task
scope are rechecked. These channels do not share readers with the data plane.
Duplicate keys, noncanonical JSON, boolean integer fields, changed sequences or
receipt bindings reject. No arbitrary code, paths, model payloads or credentials
are accepted as control commands.

Binding includes owner, task, session, epoch=1, stage, model, layout, tile plan,
full coordinator declaration digest, input snapshot, token digest, preprocessing,
actual stage input digest, device, instance and authenticated peer identity hash.
The full declaration covers the coordinator's Task/Session/Stage/config record;
a tile-plan hash alone is insufficient. Both provider contexts and output flags
remain explicit: callable providers/None returns are not authentication proofs.

ControlJournal atomically charges a stable command/reservation and permanent
(owner,task,stage) slot before checking an existing resource commitment. It starts
UNKNOWN, becomes RESERVED after verification, commits CONSUMED before invoke and
COMMITTED after successful result registration. All states retain logical byte
charges. Changed bindings/IDs cannot recover a stage; repeated requests query
history and never substitute a new operation. Query is not execution permission.
Fresh file creation alone enables writes that can advance qualification. Reopen
is history-only: even RESERVED cannot resume; query and conservative retirement
remain available. Copied/rolled-back files are outside this local uniqueness scope.

ControlJournal resources.hold only verifies an ALREADY EXISTING stable-ID
commitment. It cannot implicitly allocate, release or refund. The pipeline first
obtains its existing whole-request ticket; after actual input is known it uses
ControlPair.describe (metadata only), then explicitly awaits resources.bind_control
(spec,ticket,stage,descriptor) before dispatch. That provider must bind the
already reserved ticket to the exact stage RID/input once. Unknown binding reply
quarantines the original whole ticket, not a new reservation. This resolves the
fact that stage-1's digest-derived RID is unknown until hidden data is committed.
The integration fixture uses an exact pre-registered commitment map, not a hold
that accepts arbitrary RIDs. It is still synthetic, not physical resource proof.

Each stage's declared logical charge must cover the complete device + admitted
pipeline_host + source + destination logical demand before any dispatch. This is
conservative double accounting across the two roles, not measured GPU/RSS usage.
Real providers must cover weights, KV, workspace, copies, TLS/OS buffers, metadata
and retained failures. Book/output/BLOB caps and receipt-only execution deny are
unchanged. Independent local commitments are not a distributed atomic reservation.

## Failure and lock rules

Identity, permission, resource and SQLite fences contain only short synchronous
checks/transitions. They do not span Channel send/receive/release or an awaited
operation. In control mode, the outer pipeline and stage-0 authority scopes are
short pre/post checks; actual StageOwner admission/commit and synchronous stage-1
Driver fences remain. Production providers must still implement real per-task
permission, freshness and revocation policy; no instantaneous remote revocation
or force-stop guarantee is added.

After consume, credit release may await: the service revalidates complete bound
CONSUMED/non-retired state and the original resource commitment afterward, then
invokes without another await. Authorization-context exit failure clears invoke
eligibility and retires any durable intent; history cannot turn it into success.
Pipeline bodies also recheck source/receipt state immediately before loading,
so a source change during control waits cannot trigger stale stage-1 loading.
Cancellation/disconnect owns and settles service/body tasks, retains fees and
never retries uncertain work. A committed result followed by lost reply remains
historical; query does not rerun it. Cleanup/retirement may fail and stays unknown.
Connection closure or journal terminal state never proves physical device stop.

This version supports wire query through a live pair. After successful pipeline
completion the caller retains that pair for queries and must await pair.close()
when finished; failure closes and settles it. Disconnected pairs retire;
new connections do not recover old execution. Reopened journal history can be
read by trusted local administration but cannot be attached to a new service to
resume/query over the wire. An authenticated reconnect/history-query endpoint is
still future work and must remain separate from execution continuation.

## Validation boundary

Tests use actual Channel code, temporary SQLite, real existing one-token pipeline
journals/storage and in-memory streams with invented SSL metadata. Computation,
identity/permission/resource/device providers are explicit substitutes. The main
pipeline stores/fetches all 607744 synthetic bytes after both service-side stage
consumptions. Event barriers exercise release/retirement and source replacement;
no sleep determines ordering. Import/audit guards reject framework imports,
network connect/bind/DNS and subprocess/system actions.

This proves local protocol/ordering/storage composition, not real TLS handshake,
remote deployment, model numerics, physical reservations, crash/power-loss recovery,
GPU completion or global replay prevention. The next increment must provision
real role-local bootstrap and genuine providers on this same path, preserving
stable IDs and UNKNOWN retention; it must not restore qualification from receipts.
