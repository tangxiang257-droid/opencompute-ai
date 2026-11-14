# Phase 1 online action admission — isolated candidate

Base: `0aff1440074f2b904a8b7b242572c375d1255168`. Not published or enabled.
Requirement source: OpenCompute AI Codex Development Brief v1.0, Library
`libfile_aa11dd28731c819189c67315d9f724cd`, version 2. The first one-token
serial smoke is a prerequisite, **not Phase 1 acceptance**: acceptance also
needs two real GPUs on different public networks, correct results and measured
overlapping work. No model, GPU, node, listener or actual TLS connection ran here.

## Architecture selected

One trusted coordinator is the authority for each fresh experiment. No HA,
database copying, failover or restart restoration of execution eligibility.
The coordinator owns the existing `ExperimentAdmission` database, user identity
admission, explicit confirmation and online issuer. A and B retain separate
local installation/control/resource journals. C has independent receiving and
export authority. Tensor data stays on the existing A→B and B→C paths.

The new explicitly confirmed `phase1-online-public-short-actions-v1` profile is
online-only and L1 public/non-sensitive-only. Nodes can see plaintext input,
activations, KV and output. TLS is not confidential computation. No L2/L3
fallback, offline autoexecution, automatic billing or credit issuance. The
original positive budget cap and privacy context remain unchanged. This does
not make hardware free or confer platform resource ownership.

Existing signed user identity is separate from task consent. Existing TLS
verification, certificate pins and connection generations are separate from
node/task/resource permission. Installation receipts are not bearer permits.

## Actual code path in the candidate

1. `ExperimentConfirmationApp.prepare(..., runtime_profile=...)` stores the
   exact profile in the reviewed/confirmed body and its digest. Old bodies gain
   no new permission. Existing CSRF/credential/owner/one-use confirmation remains.
2. Existing admission dispatch and `installation_service` consume the original
   online A/B stage slots. Actual source records, input digests, plan, model,
   placements, receiver and C-copy consent remain bound.
3. `ActionAuthority` requires that original live installation, confirmed body,
   a complete control scope, explicit current authorization and trusted clock.
   It directly rechecks the SAME issuer resource provider and existing resource
   ID for each action; it neither allocates nor duplicates a reservation.
4. A separate supplied authenticated Channel carries bounded canonical bytes
   between `ActionGate` and `ActionServer`. The server pins the original role's
   node/certificate. Each endpoint also requires independent identity policy.
   No connection, certificate or provider is synthesized by this code.
5. `ActionGate.attach` pins an already-bound `ControlService`. After existing
   durable consume and B receiving gate, service awaits `start`; it then
   revalidates existing fences and consumes the short action immediately before
   invoking the original operation. No local lock spans network/body awaits.
6. On operation return, service computes its existing bounded result digest,
   obtains a DIFFERENT `commit` permission bound to that digest and exact start
   scope, then calls the existing journal completion inside the local gate.
   It does not thereby authorize C publication/export or a model/device callback.

The ordinary-data test uses actual confirmation, original online installation,
Channels, SQLite control consumption and completion. Its signed issuer, clock,
identity policy, resource capacity and ordinary body are LAB substitutes. It
does **not** implement a production factory, remote resource proof, B committed
input proof provider, C execution permission, or physical completion provider.
The thin host remains explicitly disabled. Old ungated paths retain their old
independent authorization requirements; this is an additional restrictive gate,
not a replacement provider or a global enable switch.

## Time and cancellation semantics

Fresh admission schema 3 adds `short_actions` in the SAME database as `revoked`
and `experiments`; schema 1/2 reopen only for history. No migration or mutation
of old records. The original issuer DB is an extra check, not an atomic second
participant. All fresh-profile cancellation goes through admission revoke;
existing USER/source revocation is additionally ANDed with current fences.

`start` and `commit` inserts are permanent unique `(experiment, stage, action)`
rows. Their transactions linearize **authorization admission**, not GPU start
or remote physical persistence. Revoke first: deny. Action admission first:
the original start/commit may be entered within its deadline after revoke;
already-started work may continue afterwards. A start does
not grant commit; cancel after start and before commit admission blocks commit.
An admitted commit binds the exact original command, reservation, input, plan,
device, instance, peer and output digest. This is not distributed atomic commit.

Authority deadline is fixed at admission, at most 5 seconds and no later than
task expiry minus the trusted clock error. It is checked after provider exits
and inside the actual reply write fence. Remaining TTL cannot extend it.
Role deadline starts BEFORE sending/queueing, subtracts a 1-second safety margin,
and is at most 4 seconds. The entire request has a 1-second timeout. No renewal,
retry, reconnect permission or new nonce recovery. These are conservative
experimental limits, not measured public-network latency guarantees.

Each trusted clock sample supplies wall time, monotonic time and a verified
absolute wall error <=250ms. Both sides retain samples across actions; observed
rollback or wall/monotonic delta disagreement >500ms rejects permanently. A
trusted implementation must attest rate/suspend/uncertainty behavior; supplying
`time.time()` with an invented zero error is NOT acceptable. The 1s margin
does not establish a guarantee on an arbitrary unverified clock or malicious
host. Missing such a provider disables execution.

Observed disconnect rejects new actions immediately. A silent partition is
only bounded by deadline for *new actions*. Already running work is not stopped
by TTL or logs. Cancellation cannot recall sent input or a committed result.
Unknown commit, lost reply, provider exit failure and timeout retain consumed
slots and resource liability. Query/cleanup never recreates a permission.
Stop/wait/quarantine maintenance remains separately authorized and is not gated
by an expired execution lease; no automatic release occurs here.

## Concrete deployment gaps and preparation order

1. Configure a real user credential issuer, pinned public verification key,
   issuer/audience, trustworthy clock and revocation policy using existing
   SignedIdentityAdapter/LocalAdmission. No real keys are generated here.
2. Supply explicit user confirmation of a public non-sensitive test input,
   fixed model/config/weight hashes and license, exact plan, budget, expiry,
   A/B placements and independent C input-copy/retention policy.
3. Configure verified mTLS node certificates, hostname/pins, purpose-specific
   endpoints and original connection ownership. Do not reuse the install or
   control Channel for action exchange. No listeners are added by this patch.
4. Implement the trusted runtime authorization adapter that checks the exact
   install→control mapping/current original control peer and, for B, actual
   committed A input. `authorize.hold(full_context, action)` is REQUIRED and
   contains action/output digest. A `yield None` double is not this adapter.
5. Feed the SAME existing resource service with authoritative current platform
   allocation/lease, GPU UUID/instance, memory and disk/file budgets. A/B source
   and C destination commitments remain separate, never cross-node atomic.
   Platform exclusivity, allocator API, watchdog and privileges are unknown;
   do not assume Slurm, root, profiler access or exclusive IDC AI GPUs.
6. Install the actual backend/completion provider and loader only after those
   checks; verify loading, compute, D2H and asynchronous failures separately.
   Wire C's distinct receive/export checks into its existing ingress/ACK gate.
   Do not map this control admission to permission for all device operations.
7. Only after explicit later authorization, test real mTLS with ordinary bytes
   in separate processes, then platform preflight, then a fixed one-token GPU
   smoke. Compare numerical output and measure actual completion/resource use.
   Later run multi-token overlap and the complete Phase 1 acceptance matrix.

The safe deliverable here is the confirmed-context→durable action→authenticated
byte transport→existing control-journal path with default-deny dependencies.
It is not yet a runnable two-host GPU deployment. Cloned/rolled-back databases,
malicious privileged hosts, remote instantaneous revocation and global replay
prevention are outside the single-authority local guarantee.
