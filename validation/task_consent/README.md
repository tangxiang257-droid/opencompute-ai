# Private explicit task consent, fresh v13 only

Baseline 32dd0217d415fafb09da6ca64b31edbd7fa51bea. Unmounted, not production.

`open_consent_service` uses the existing ApplicationService/Authority assembly,
existing directory trust, flock and SQLite connection, with explicit fresh v13
ConsentLedger. No v12 migration or reinterpretation. All TextInputLedger schema,
including text_source_no_plan, is inherited. Default USER verifier rejects.
InputDeclarationReader permits exactly the original TextInputLedger or this
ConsentLedger, never arbitrary subclasses.

TaskConsent requires the SAME service LocalUserFence. Tests use actual Ed25519
LAB signatures and LocalAdmission, plus real stored metadata/token snapshots.
Actual issuer trust and deployment configuration are not established by tests.

Prepare validates the complete existing P1 declaration, model/config and source,
then binds budget in integer microunits, currency/pricing policy, privacy/policy,
actions, deadline (max 300 seconds), offline=False and each stage's node/device/
instance/resource. The immutable stored proposal digest is what the separately
trusted explicit-action provider must confirm. That provider is deliberately not
shipped: no default confirmation, automatic storage-trigger or HTTP route exists.
A callback returning Confirmation is a trusted integration boundary, not proof of
real user interaction. Caller dicts are frozen before awaiting locks.

PENDING is durable before confirm; CONFIRMING consumes the single attempt before
external user interaction. No Authority or USER lock spans that await. Afterwards,
the original provider/method, current identity, stored source, task, revision,
expiry and record state are rechecked. ACTIVE gets live membership only after
commit and all USER fences exit. Unknown/failure seals the attempt; no reissue.
Task-level permanent revoke covers PENDING/CONFIRMING/ACTIVE AND an existing task
without any proposal. It serializes on the same Authority lock. Revoke failure
seals all consent handles sharing that Authority; no success or physical stop is claimed.

History and check_context return only a consent fact with execution_allowed=False.
They neither implement a runtime authorization provider nor open the existing v12
execution gate. Existing issuance/control/resource/device/connection checks still
need their real providers. Each proposal is bounded to 32 KiB and one per existing task; this metadata is not
a new payload quota or a capacity reservation. No budget allocation, device capacity or cross-host
atomic operation is introduced. B's actual derived input remains a later existing
committed-edge check, not user-provided hidden bytes.

Fresh-only initialization rejects existing databases. A new TaskConsent cannot
restore ACTIVE rows from history (new boot + empty live set). Cooperating local
instances remain under the existing directory flock; these are not global replay
protection. Copying databases and maliciously copying live in-process state is
outside this trusted-process boundary and is demonstrated in a test. Trusted
single-authority deployment and rollback/replica coordination remain prerequisites.
A host/process replica can independently mint consent for cloned tasks if supplied
its own trusted explicit-action provider; no claim of global uniqueness is made.

Tests use ordinary bytes, SQLite, memory barriers, CPU signature verification and
synthetic explicit confirmations. No model/GPU/node/listener is run. Actions may
already be queued or data sent before revocation; neither logs nor host thread
exit prove GPU physical stop. No public app imports this optional module.
