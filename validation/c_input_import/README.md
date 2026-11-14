# Private co-located C input import

Fixed baseline db6dfeb87459052f34f25e1cd12ebb8ca1a5f2b0. This is a private
central-host wiring increment, not a remote C protocol or deployed service.

Call path: ExperimentConfirmationApp explicit owner review/POST -> fresh
ExperimentAdmission.confirm/dispatch -> actual online issuer registration ->
LocalCInputImporter.import_registered -> publish_input preserving InputBinding ->
exact result_artifacts._Ledger._import_registered_input -> existing record_pending.
Synthetic result bytes can then exercise the existing result storage core. This
is not a receiving grant, C ACK, execution permission, or model verification.

New fresh admission databases/context use version 2, adding a receiver resource_id
and permanent c_import_slots. Version 1 databases remain history-only; no migration,
ALTER, replay, or new eligibility on reopening. Existing v12/v13 execution bans,
old IDs, hashes and authorizations remain unchanged. Explicit IDs are confined to
the exact result ledger; the shared default source path still generates random IDs.

C input copying requires the original current USER/source consent, explicit copy
purpose/destination/snapshot, the original same-boot registered issuer, independent
C identity and capacity scopes. Missing providers refuse. Trusted synchronous hold
providers must validate the complete context on entry AND recheck current authority
on exit, raising on invalidation or unknown. No network awaits occur inside these
scopes. Method shape and passing tests do not prove a provider is authoritative.

Capacity must verify an existing persistent commitment for the confirmed resource_id,
node, identity, retention, target directory/database identities and requested byte/file
budget. The request includes actual canonical input file size, output, 32768 manifest
bytes and explicit overhead. A caller's overhead number is not measured disk capacity;
provider must reject insufficient metadata/SQLite/WAL/orphan/file-count coverage. This
increment neither acquires new capacity nor proves global accounting or filesystem
reservation. Scoped node/device/instance digests are labels, not physical uniqueness.

CLAIMED commits before publishing input. Any later failure retires admission/importer
and attempts UNKNOWN; files and charge remain occupied even if UNKNOWN persistence
fails. Query/history never restores eligibility. Separate source/C SQLite commits are
not atomic. Snapshot/DB inode checks assume a trusted private host and do not defeat
malicious same-user filesystem replacement or cloned/rolled-back databases.

Tests use ephemeral LAB signatures and synthetic identity/capacity providers, real
SQLite/files/snapshots and in-process ASGI messages. No listeners, network, Torch,
models or devices. A low-level storage test is not receiving/export authorization.
Real issuer/configuration, C remote authorization/revocation, capacity allocation,
C receiving/ACK gate and fresh execution authority remain required and absent.
