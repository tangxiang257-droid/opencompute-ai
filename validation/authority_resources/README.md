# Verified source provenance and persistent role-local logical resources

Fixed baseline cbe633a4c1a00d53767a5728103c1fe74d1c566c. No model, GPU, Worker,
node, real network/listener or production service runs in this validation.

RegisteredInputSource uses the actual InputDeclarationReader, current v12 stored
metadata and immutable snapshot files under Authority's lock and the SAME user
fence as ApplicationService. It derives stable task source ID and snapshot/token/
preprocessing hashes; the issuer leaf callback works only on the active synchronous
stack. External authorization callbacks and scope exits require fresh source checks.
Unknown post-commit outcomes retire the issuer and do not return qualification.
The source ledger remains unchanged. No bearer, raw token bytes or file path is
included in persisted descriptors.

This is SOURCE PROVENANCE ONLY. Existing source status remains
internal_metadata_only/unverified_declared/execution_allowed=False. It does not
supply the independent issuer.authorize or device resources. LocalAdmission has no
production IdP adapter here, and v12 text-source plans remain explicitly forbidden.
No production user authority was implemented or inferred from owner strings.
Canonical usr_+64hex owners also exceed the current Spec/Book owner64 bound; this
known future integration blocker is reported rather than silently truncating,
rehashing or widening identifiers. Source unit tests can register provenance;
they do not claim v12 source-to-compute execution readiness.

RoleResources is an actual SQLite authority for ROLE-LOCAL LOGICAL budget records.
The same node service checks installation, control binding and stage operation.
A/B on different nodes use separate services; within one node all stages and tasks
must share its pool. No cross-node atomicity, host/device allocation, measured RSS,
GPU capacity, global clone/rollback protection or genuine permission is supplied.
External authorization and capacity providers remain mandatory and default missing.

Reserve permanently charges device/host/source/destination UNKNOWN before external
verification. HELD returns only an exact live local Ticket. Immutable owner/task/
stage occupancy prevents alternate-RID retries. bind_control fixes the already held
promise to one actual binding/charge; B input is fixed at that point. hold_control
never allocates. Quarantine charges remain; failed UNKNOWN writes still poison the
local reservation so an old HELD row cannot resume. Reopen is entirely history-only.
Queries cannot issue tickets, recover work, verify physical capacity or refund.

Policy.requirements preserves existing producer limits. Required extra_host_bytes
adds explicitly configured per-reservation transport/control/metadata overhead to
the role pool; it must be provisioned from the real assembly, not guessed as RSS.
Integration doubles conservatively count both directions for issuance32768,
control32768, hidden65536, plus B result32816 and metadata32768, plus32768 overhead.
Their separate role pool is1MiB; existing producer host policy remains262144 and
byte/frame/Book limits remain unchanged. Disk/db/file-count and physical-memory
isolation remain external provisioning obligations, not proven by these byte pools.

Tests use real v12 reader/SQLite with a synthetic USER fence only for source cases.
Resource integration uses the existing legacy synthetic-input result ledger and
synthetic external authority/capacity/device/backend, replacing only in-memory
resource dicts with this persistent provider across actual startup/control/A/B.
It does not bypass the v12 no-plan gate or substitute synthetic authorization in
production. A successful ordinary-byte chain is not real compute acceptance.
