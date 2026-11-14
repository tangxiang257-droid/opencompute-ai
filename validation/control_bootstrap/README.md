# Original control connection bootstrap

Base: 3c7cee202c7ef5e694a0c2936dcf8119f05784c9.
The concrete missing link was `CoordinatorControl(client, role.descriptor)` in
an in-process fixture. A separately hosted coordinator cannot read that role
object or infer the role's randomly generated reverse connection identity.

`private_experiment_topology` replaces this object handoff with bounded ordinary
metadata on the original authenticated control Channel. This is runnable protocol
composition, not a process runner, socket listener, permission issuer or device
backend. The test uses real protocol/state components and LAB memory streams.

## Data flow and readiness

1. Central user confirmation and original A/B online installation remain required.
   Each role owns its existing local records, resource ticket and startup.
2. A attaches its original control Channel and independent action gate; both H
   advertisements follow existing startup code. No second H or wrapped reader is
   created by bootstrap. B's hidden ingress retains its original prepare/no-early-H
   behavior, independent of control bootstrap.
3. Central sends **HELLO** with a fresh challenge. The exact installed role replies
   **ANNOUNCE**, echoing the challenge with its canonical RoleDescriptor and resource
   ID. Central verifies original confirmation/installation/role certificate,
   placement, full source/model/plan binding and locally configured charge, then
   sends **ACK** of the exact announcement digest. Role validates it and sends
   **READY**; central validates/rechecks before returning CoordinatorControl.
4. Bootstrap uses sequence 0/1 in each direction; actual control requests start at
   sequence 2. Actual ControlClient and ControlService reject ordinary business
   calls during the one-shot pending phase. They cannot race the bootstrap reader.
5. Existing configured ActionAuthority/ActionServer assembly then uses this
   imported controller. No action grant is created by the announcement.
6. After A commits and sends hidden bytes, B reads its actual receipt and builds
   its local operation/control. Only then can B announce. Its announced digest is
   still an authenticated role statement; the later actual `input-ready` proof is
   independently required. C's separate receiving authority and ACK gate remain
   mandatory before B executes; B's metadata/result frames go to C's existing
   persistent result path.

Issuance, action, hidden, C gate, metadata and result connections remain separate
purposes. Only bootstrap and subsequent control business share this ORIGINAL
control Channel, with a defined phase transition. The descriptor's `binding.peer`
is the controller as seen by the role, not the coordinator's reverse role identity.
No local role object, ticket, database handle or operation callback crosses the
central import API.

## Trust, ownership and failure

Real configured identity/task/resource providers remain mandatory. The central
identity and original resource service must be explicitly supplied; expected
charge comes from trusted local configuration, not the announcement. The new
`bootstrap-control` policy action must be explicitly allowed by real providers;
unknown actions should remain denied. Endpoint method shape does not prove trust.

Schemas are exact and canonical, bootstrap payloads at most 8 KiB, existing credit
32 KiB/one tile and complete-frame cap 64 KiB. No new Channel, quota reservation or
permission table is created. Extra serialization copies and metadata still need
real host capacity accounting; these bounds are not RSS/GPU peak evidence.

Before take, reject used/aliased/wrong-role channels, missing providers, wrong
context and already-started business. Channel AND writer occupancy is permanent;
a pending bootstrap stream cannot be repurposed as an action stream. After take,
errors/unknown close this original control path, retain state/charges, and do not
retry with a new challenge/connection. Central source/user/policy/resource fences
and role original installation/control/resource rows are checked again after
callbacks and at the actual write fence. No such local lock spans network awaits.

The role's existing launcher owns entered exchange work and cleanup. Only errors
before `_run` takes ownership trigger supplementary close; nested launcher jobs
are rejected before take. This avoids recursive cleanup waiting on the same task.
Central cleanup retains its owned close task through repeated cancellation. Close
or timeout never implies physical device stop or permission to refund resources.

READY is NOT atomic distributed handoff: the coordinator can receive READY before
the role's final writer drain/recheck succeeds. A failure then closes the original
connection; subsequent actual control/action checks must fail. Lost ACK/READY
never authorizes a replay. The old path that never opts into descriptor bootstrap
is retained for compatibility; public real `thin_host.run_once` remains refused.

## Validation and remaining dependencies

The ordinary-data fixture imports A/B descriptors through the wire, constructs
actual action authorities, validates B committed input and reaches C ACK/result
persistence. It covers one H, sequence transition, pending business refusal,
wrong challenge/placement/charge, callback drift, revocation, cancellation,
post-claim cleanup and purpose aliasing. Events decide races; timeouts only bound
failure. Independent review includes actual RED/GREEN defects and records two
intermediate cleanup hangs as incomplete tests, never PASS.

This has not run separate real role processes or real TLS/networks. User issuer,
outer policies/capacity/clock, device/backend and certificate streams are explicit
LAB substitutes. No actual model/weights/GPU, real node, listener or network connect
was run; no production credentials/private keys were generated/exported.

Real per-host bootstrap, authenticated endpoint provisioning, remote policy/
resource services, platform allocation/exclusivity, clock bounds and device
completion remain missing. SQLite ordering is local, not clone/rollback-proof;
source/destination reservations are not distributed atomic transactions. Existing
5/4/1-second action policy remains experimental, not measured performance.
