# One owned C dispatch ACK to persistent result

Base 10993f7da3290349c4e7765484489898f1227dc2. Unpublished candidate.

ReceivingLauncher.serve_result_once is disabled unless enabled=True. It consumes
one local attempt after existing C pending/activation, attach_result and attach_gate
have succeeded with their independent identity, authorization and resources.
A real exact _Ledger, current check and publication fence are mandatory. The
existing owner runs original gate.serve_once followed by original role.receive_result
as ONE task, preserving the original ResultIngress, frame validation, reservation,
file publication and manifest ACK. No listener, new permission ledger, provider
factory, automatic retry or C execution permission is introduced.

The existing authority_chain fixture now has a delivery hook; the c_service test
uses it to start this actual C service before B dispatch and await the persistent
manifest afterwards. Original caller behavior remains covered separately. The
positive chain uses actual installation hosts/launchers/ActionAuthority/control
and SQLite stores, with injected socket/TLS boundaries and LAB compute/providers.
It is not a deployment, numerical-model or real-device acceptance test.

Before ACK, under the original receiving and ACK write fences, the service checks
its immutable spec, complete descriptor/bindings, original control/data generations,
publication permission and actual imported source bytes. It reuses the read-only
source part of _Ledger._binding; no SQL/state machine is duplicated. Task must be
STORED, attempt PENDING; session/epoch/plan/snapshot/token/preprocessing/vocabulary
and B's namespaced declared device/instance claims must match. Provider exits are
followed by fixed-binding checks; removing the ACK hook cannot restore legacy
behavior. Spec/descriptor replacement and wrong source fail closed.

Model-record digest and weights digest have distinct namespaces. The actual
snapshot header binds the former. The original user-confirmed admission bound
that snapshot to the model declaration, and this service preserves the original
snapshot while checking spec.model against the fixed C descriptor weights hash.
It does not infer model provenance from equal-length hashes or a model filename.

No output digest/key is fabricated before metadata. Destination reservation and
publication still happen in original ResultIngress after real metadata arrives.
ACK does NOT guarantee remaining capacity, physical preallocation, global quota,
cross-node atomic reservation, or that B can safely execute after later remote
revocation. Existing resource/receiving fences remain mandatory on both sides.

Old serve_gate/receive_result entry points refuse competition once this combined
attempt starts; a previous active gate/receiver prevents taking the combined path.
Control revocation remains available. Failure/cancel calls original launcher close;
unknown records and charges remain. Close failures retain their original exception
objects. A noncooperative operation may still block cleanup: no device stop,
process isolation, retry, refund or timeout-based release is supplied here.

Tests cover the complete chain, default refusal, actual ledger cancel, provider
exit deleting ACK checks, competing entry points/cancellation and close failure
with retained charge. One initial cancellation test used an invalid direct SQL
transition and was rejected by an existing trigger; it was corrected to the real
cancel API. That setup error is not claimed as a runtime defect. Initial model
namespace and placement-claim mismatches are retained in diagnostic logs.

Remaining: real purpose-specific control/data/C listeners and independent process
supervision; trusted issuer/node certificates, current user permission, platform
capacity and device completion providers. thin_host.run_once and v12/v13 bans
remain unchanged. No real listener/connect, node, model or device was run. The
historical first-disconnect cause remains uncertain.
