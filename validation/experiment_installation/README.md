# New experiment to existing online installation

Fixed baseline bbda44864b1f31ac467c50576c667e92a96d052a. This removes the first
concrete new-experiment startup gap, not the remaining remote execution gap.
No production host, provider factory, listener, network socket or device is enabled.

Actual wiring: configured ExperimentConfirmationApp installation_authority ->
ExperimentAdmission original successful same-boot registration -> installation_service
constructs existing IssuanceService -> its existing authenticated claim_online call
-> existing IssuanceAuthority.claim_stage durable stage slot -> existing IssuanceClient
verified delivery -> RoleInstallStore ACCEPTED. These are the actual issuance/client
components called by RoleStartup. This test increment stops before runtime journal,
loader, model, GPU or execution; it does not claim full RoleStartup.install acceptance.

The installation authority defaults to None. Its hold must validate the confirmed
full context, current purpose/node/device/instance/resource allocation, current peer
certificate/connection, budget/privacy/expiry and consent at entry and exit. Endpoint
identity/task permission and resources remain separately mandatory. The resource
service must be the same object pinned by the original issuer, not a new reservation
or second fixture. No provider is synthesized or selected from strings/config imports.

Each stage gets one original endpoint. Current checks bind original live registration,
issuer incarnation, source revision and actual snapshot, complete context digest,
exact placement and original connection/provider identities. New endpoint write fences
recheck after waiting for the Channel writer lock. User/task fences never span network
awaits. The shared synchronous source validation is complete; old locked callers still
must hold the Authority async lock. Trusted same-process callbacks/objects remain part
of the boundary, not a sandbox against malicious local code.

The durable state machine remains issuer slots plus RoleInstallStore. Failed/unknown
claims burn eligibility; committed slots are never deleted/reissued. Lost response can
leave a committed slot without an accepted role handle. History/restart/copied SQLite
or copied registration objects cannot create a new qualification. Creating an endpoint
may occupy its local slot even if no network exchange occurs. Supplied Channel ownership
stays with the caller on factory failure. Resource promises are checked, never refunded.

Tests use actual ASGI confirmation, issuer/role SQLite and existing bounded Channel
protocol with ordinary in-memory streams, invented SSL and LAB identity/resources/
installation/completion providers. They prove code mechanisms, not real issuer authority,
platform allocation, mTLS or cross-host operation. Stage B installation acceptance is
not actual committed hidden-input proof; the existing B receipt/prepare gate remains.
Direct claim/install/load/execute calls still fail; only the specific original-service
claim callback may enter the synchronous issuer leaf. v12/v13 bans and host.run_once
refusal remain unchanged. Installation responses keep execution_allowed=False.

Next critical dependency: authenticated current execution permission/revocation for
new-experiment roles, wired to existing RoleResources/ControlService/StageA/BOperation/
DeviceCompletion. Old RuntimeConsent is local and old-task-specific; copying it to A/B
is not valid. Real user issuer/audience/public key/session mapping, node certificates
and endpoints, allocation lease/device UUID/exclusivity and disk commitments require
trusted operator/platform information. Do not invent a C identity to bypass co-location
constraints, derive authority from free VRAM, or treat LAB providers as that information.

Independent RED cases covered replacement of the actual service authorization
provider and replacement of its nested parent during callback entry. Both are fixed
by pinned dependency checks before/after callback entry, body and exit, including
USER exit and the actual write fence. Observed drift seals admission permanently.
Revocation cannot retract a response already sent or undo an already committed slot.
Role-side current execution permission remains an independent missing requirement.
