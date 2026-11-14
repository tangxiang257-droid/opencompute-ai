# Installed-role action attachment (unpublished candidate)

Base: `76c3eac291a4468b8df1b55a64a474f518791151`.
The earlier short-action patch is published in draft PR #21; this new increment
is separate and not published. It adds no listener, model/device execution
permission, provider factory, retry, resource reservation or release.

## Reusable components and actual missing dependencies

| Component | Actual code available | External facts still missing |
|---|---|---|
| User identity | `private_signed_identity.SignedIdentityAdapter` verifies Ed25519 custom-format credentials; `LocalAdmission` persists local revocation | Real authorized issuer/public keys, issuer/audience, clock and remote revocation operations; not a credential issuer or JWT implementation |
| Task authorization | Explicit experiment confirmation, existing source reader, original online installation, `ActionAuthority` and per-action transaction records | Trusted control-to-install/current peer mapping at the coordinator; actual B committed-input evidence; public-input/user confirmation for a real run |
| Device completion | `cluster_p1.device_completion.DeviceCompletion` and `_TorchRuntime` contain loading/execution/materialization/completion fences and actual Torch calls | Real device/backend/authority/capacity configuration and physical verification; no CUDA/GPU run has validated these here |
| Resource service | `RoleResources` has persistent HELD/UNKNOWN promises, exact ticket, control mapping and `hold_device` view; preallocation code reserves actual local file extents | Current platform allocation/lease, GPU UUID/exclusivity and true capacity; logical bytes or file preallocation do not allocate a GPU or prove global availability |
| C receiving | Existing independent C input import, receiving registration, ACK gate, `ResultIngress`, committed artifact storage and export fences | Actual C identity, permission/capacity and remote provider configuration; B permission is not C permission |

The most useful safe wiring without host facts is **the existing installed role
and its actual local control/resource records**, rather than a new permissive
implementation of any missing provider. This candidate implements that wiring.

## Call path

1. Existing `StageLauncher.create/install/build_operation/attach` yields the
   actual `InstalledStageRole`, `RegisteredRole` and original `ControlService`.
2. Explicit `await launcher.attach_actions(unstarted_channel, ...)` requires
   verified authority identity policy and clock, takes a purpose-specific owned
   channel and permanently occupies the attachment attempt. No dynamic imports
   from configuration or fallback provider are accepted.
3. `ActionGate.attach_installed` derives scope from the role's own descriptor;
   the caller does not supply command, reservation or input digests. It pins
   original startup/store/resources/ticket/role/operation and control service.
4. Every gate check calls the original installed-role checks (including B's
   actual committed received input), compares the actual `install_control`
   record, and invokes the SAME `RoleResources.hold_control` on the already
   held ticket and control reservation. No new reserve or duplicate charge.
   After the last external resource callback exits, a callback-free leaf check
   compares original database records, runtime/role/ticket references, in-memory
   poison status and original channel identities. Failed UNKNOWN persistence
   cannot be hidden by an unchanged HELD SQL row.
5. Existing ControlService invokes online start/commit gates at their original
   positions. Existing task/device/completion/identity/resource checks remain
   additionally required. B's independent C receiving/ACK gate is unchanged.
6. Attachment in progress cannot dispatch. Failed or duplicate attachment seals
   the launcher. EOF is watched by existing lifecycle management; launcher close
   joins owned work and closes the additional channel. UNKNOWN retains resource
   responsibility. Expiry/close is not proof of physical stop.

This proves local installation→control→resource linkage, **not** a remote proof
that the central ActionAuthority can independently trust. No local SQLite file
or Python object is copied to another host as an authorization credential.
The actual coordinator authorization adapter remains mandatory and disabled
when absent. `thin_host.run_once` remains denied; old v12/v13 bans are unchanged.

## Evidence boundary

Tests use actual launcher, installed-role, Channel, SQLite and C storage code,
ordinary bytes, existing synthetic backend and memory SSL. The full A/B/C test
uses an explicitly LAB action-response implementation; it is not a real issuer
or the ActionAuthority. Previous short_actions tests separately exercise real
confirmation→original installation→ActionAuthority transport with ordinary body.
These are complementary integration tests, not one real deployed identity chain.

Covered: exact mapping, missing providers, repeat attach, changed install mapping,
UNKNOWN resource mapping, action-channel EOF, pending attach vs dispatch/close,
unreachable authority, resource-exit mapping/poison changes and the existing
A→B→C stored-result chain. Event barriers
control races; timeouts bound waits, not prove ordering. No listener, socket
connection, real node, model or GPU is used.

## Publication safety clarification for the previous patch

SQLite ownership is scoped to one actual database path/inode and supported
application methods. O_EXCL chooses its fresh creator; PID/inode checks, SQL
transactions and unique keys preserve that local domain. Ordinary process tests
show one application writer for the same path but two for two independent paths.
Reopened connections use SQLite `mode=rw`: application-level history-only is
not OS/SQLite read-only protection against direct SQL or a malicious same-UID
process. No distributed/global uniqueness, clone or rollback protection is claimed.

The 5-second authority / at-most-4-second role / 1-second RPC values are fixed
experimental profile and code constants today. They are neither externally
configurable nor measured public-network performance guarantees. Changing them
requires a reviewed/versioned profile and fresh explicit confirmation, never a
silent runtime extension of an existing lease. Admission/cancel ordering is
authorization ordering; work admitted first may enter before its deadline and
continue after it, with a separate commit admission required.

Remaining real-run order: trustworthy issuer/clock/node/platform configuration;
current coordinator task/mapping/committed-input adapter; role device/backend
and C permission/capacity configuration; separately authorized real mTLS byte
exercise; platform preflight; one-token GPU smoke; full Phase 1 numerical,
overlap and different-public-network acceptance. No host-specific facts are
invented and no production execution is enabled by this increment.
