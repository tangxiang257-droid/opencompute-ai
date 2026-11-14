# Role control/action dial: design and plan

Base 7cb6483ab2ed45007987028f3bf63f9a760e4d88; unpublished.
Goal: connect the installed role's existing operation to its original coordinator
control and separate action authority without caller-built Channels. Add an
explicit private `dial_control_ready` to existing role_dial, then call existing
bind_control_ready. No new ledger, permit, automatic dispatch, listener or CLI.

Choice: implement role outbound assembly first. A new generic server framework
would add lifecycle surface before we have all endpoint policies; merely wrapping
thin_host would still always refuse. This narrow path has an actual caller: role
operation creation immediately followed by control bootstrap in the complete chain.

Trusted pinned public config binds launcher config hash and separate control/action
endpoints, hostname, peer certificate and immutable material digests. Control peer
must be coordinator_node; action peer must be original issuer node/certificate.
TLSConfig peers are exact single-entry maps; actual TLS and existing Channel peer
checks remain. Never use config.context or a started Channel to re-read TLS paths.
Missing providers/clock deny before dialing. Original installation session, held
resource ticket, operation and provider methods are pinned and rechecked around
every await. No local lock spans connection waits.

A permanent launcher attempt flag is set before first dial await. Pending caller
is pinned; competing dial, bind or attach calls reject without closing the owner.
Dial tasks/late writers are retained through cancellation, partial success closes
all unadopted streams; adopted streams belong only to launcher. Existing external
close must not cycle with a caller waiting on the same close. Cleanup errors stay
attached and resource liability remains UNKNOWN; no refund or qualification reset.

Plan (native implementation, independent review):
- [x] RED real chain hook calls absent dial entry; no model/network in test.
- [x] Implement strict config + existing TLS loader + two raw dials + original bind.
- [x] Events test cancel/late second stream, missing policy, wrong pins, competition,
      identity/resource loss between dials and failed writer cleanup.
- [x] Run existing installation dial, role entry, C HTTP200, cleanup regressions;
      independently review full delta and rerun ordinary-data tests.

Tests inject open_connection and TLS material loading only. Their identity,
capacity, clock/backend are LAB providers and prove no real authority. Actual
network/listener/model/device operations remain forbidden during this task.
Thin host CLI remains REFUSED; central control/action listeners, A→B hidden and
B→C dedicated result endpoints, supervision and real platform configuration are
still prerequisites. Known action lease seconds are experimental, not measured.

## Updated end-to-end critical path

| Step | Existing callable code | Still required for two hosts |
| --- | --- | --- |
| User / task consent | ExperimentConfirmationApp, ExperimentAdmission; LocalCInputImporter and CReceivingPolicy | trusted issuer and explicit user confirmation policy; mounted authenticated UI/API; no offline automatic consent |
| A/B installation | InstallationHost.start(enabled=True), role_dial.dial_stage, StageLauncher.install | private endpoint configuration, live certs/revocation, actual RoleResources and device/backend; keep installation session alive |
| Role control / action client | **dial_control_ready** creates separate raw mTLS streams and calls bind_control_ready | corresponding central control/action accepting hosts; coordinator cert pin distinct from issuer pin |
| Central control / action | receive_control, assemble_actions, actual ActionAuthority/ActionServer | authenticated accepted streams, bounded host ownership/service loop and process supervision; no borrowed role Python objects |
| Hidden handoff | HiddenSender, PreparedReceiptConnection, HiddenReceiver | dedicated A→B authenticated socket endpoints; B operation remains dependent on actual committed input |
| C admission / result | ReceivingController, ReceivingLauncher.serve_result_once, ResultIngress, original C ledger | C control/gate/metadata/result accepting hosts, live receiving permission and both storage commitments |
| User retrieval | LocalCResultRoute.install + result_download_api.create_app | explicit download policy and same live central C experiment; no cross-host locator or restart recovery |
| Process execution | thin_host preflight exists; run_once / CLI still REFUSED | concrete trusted per-host assembly and supervisor before actual GPU invocation |

New API: `await dial_control_ready(launcher, raw, sha256, tls_by_purpose,
materials_by_purpose, enabled=True, identity=..., authorize=...,
action_identity=..., clock=...)`. Keys are `control` and `actions`; the strict
public document schema is exercised in test_dial.py. All providers are supplied
by trusted local bootstrap. No provider is loaded by name, inferred from a JSON
flag, created from a test fixture, or defaulted to an allow function.

Transport ownership is handed to existing bind without an intervening await.
The original unstarted constructor has no success-path await between creating its
reader and returning it; there is no additional externally schedulable gap there.
Tests pause actual first/second dial or ANNOUNCE; they do not invent a success-path
await inside that constructor. A non-cooperative dial/cleanup can still fail to
settle: the caller/outer supervisor must retain responsibility; no physical-stop
or bounded total cleanup guarantee is made. TLS/channel buffers, allocator copies
and workspaces still require physical platform capacity evidence.

Provider drift is checked against the original startup runtime before any next
connection or operation.ready callback. This does not create a hostile-Python
sandbox: arbitrary same-process mutation of an unbound operation's cleanup fields
can prevent notice to its original provider. We do not temporarily restore fields
or claim such a provider was notified. The original persistent charge remains
HELD/UNKNOWN, never refunded; normal cancellation/resource loss uses the existing
local retirement path. A wider guarantee against arbitrary object rewriting needs
a separately reviewed pre-bind operation responsibility contract.
