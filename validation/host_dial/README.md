# Raw role installation dial — disabled by default

Current material-loading increment: dial_stage additionally requires explicit
canonical material bytes and their independent pin, and uses tls_material's
sealed snapshot loader. This suite stubs that loader; validation/tls_material
tests its real local OpenSSL path and the same A/B/C memory chain. The historical
context-boundary description below refers to the original 72cfaec candidate.

Fixed base: 72cfaec. `cluster_p1.role_dial.dial_stage` takes trusted local endpoint
parameters, pinned LaunchConfig/TLSConfig, existing RoleInstallStore/RoleResources
and the same explicit role runtime providers. It calls raw asyncio.open_connection
with a hostname-verifying client SSLContext, then exactly one StageLauncher.create.
There is no intermediate Channel reader or initial H. Installation, the active
session policy and control/action readiness still use their existing APIs.

This closes a concrete composition gap: both connect and _connect_unstarted
already construct a Channel reader; unpacking either into StageLauncher.create
would create another reader. No provider registry, test-mode CLI, listener,
credential issuance, model load, install, dispatch or default execution is added.
Public thin_host.run_once remains unavailable.

The existing create preflight is shared and includes deep runtime layer/factory
checks and refuses an already occupied local claim before dialing (the later
atomic claim still detects races). Missing provider methods, wrong current resource ticket/record, bad issuer
pin and absent hostname reject before dialing. This checks actual local records
and calls existing resource authority/capacity contexts; it does not prove that
an arbitrary Python provider represents a genuine platform. Trusted bootstrap
must supply real providers and approved configuration; LAB tests do not do so.

Config/store/provider object and method pins (including claim, task authority
and resource verification), immutable snapshots and resource leaf checks bracket
the network wait. No RLock, resource context or SQLite transaction spans await.
After external callbacks return, leaf checks recheck ticket ownership, poison,
exact descriptor and HELD record; TLS context verify/hostname/minimum-version
settings must also remain unchanged. The original Channel verifies the actual
peer metadata before returning the launcher. Local pinning does not prove a
real handshake occurred in a synthetic experiment.

The dial task is retained. Cancel/timeout cancels and settles it; a late stream
returned despite cancellation is closed without creating a Channel or claim.
Repeated cancellation cannot abandon this cleanup. A distinct late dial failure
is retained together with cancellation/timeout; only the identical original dial
error or plain expected cancellation is deduplicated. A noncooperative dial/closer
can delay settlement indefinitely: an external supervisor must isolate it, and
elapsed timeout never authorizes quota release or qualification restoration.
Before StageLauncher takes a writer, this entry closes it on failure (including
claim-file conflict). After takeover, existing owners retain cleanup; no new
child task recursively joins launcher.close. If an external close owns pending
work it retains that join obligation. No automatic reconnect/retry is provided.
A TLS-only failure before local claim does not consume an execution slot; this
is not evidence that a later installation/execution attempt can be retried.

Tests replace only raw dial/context boundaries with memory streams and LAB peer
metadata, identities, capacity and backend. Actual SQLite, Channel, launcher,
control readiness, ActionAuthority, C gate, result storage and retrieval remain
in the complete-chain test. No real socket, DNS, TLS handshake, certificate/key
creation, model/GPU/node/listener is exercised. Timing tests verify explicit dial
deadline behavior, not physical stop or performance.

Still missing for a deployable independent process: trusted concrete provider
construction/configuration, endpoint topology and inbound ownership, approved
user confirmation and experiment scope, actual certificate/issuer distribution,
platform allocation/exclusivity/UUID/storage evidence, and real device-completion
validation. This callable dial is one installation leg, not a full process host
or proof of two-host deployment. Existing v12/v13, logical limits, short leases
and UNKNOWN non-retry rules remain unchanged.
