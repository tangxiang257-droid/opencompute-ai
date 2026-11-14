# Private separated role control and read-only history

Fixed base: 7f9912ffae8119e171743b7285458d2366884a4c. No automatic import,
listener, role process, real provider or execution enablement is added.

The coordinator-side object owns only its ControlClient and immutable ordinary
command descriptor. Trusted role-local bootstrap registers the operation with
its own ControlService; service tasks are driven by the role, never by a client
holding a service reference. No code/closure, model path or object reference is
serialized. Existing ControlPair and private_one_token remain compatible and
unchanged: the old complete pipeline is NOT yet a cross-host launcher.

Registration requires explicit identity, authorization, existing-resource and
completion providers. The latter must bind all work to the exact local operation
scope, verify completion, and quarantine failure. Ordinary provider return values
are not physical device evidence; tests use substitutes. No receipt restores
qualification; ambiguous operation or completion remains charged/isolated.

History uses a separate authenticated Channel and SQLite read-only connection.
The original full binding (including historical peer hash) is immutable. A current
identity provider resolves the new authenticated connection to a principal;
current read authorization must explicitly permit that principal to query the
original binding. Reconnection is not an instruction to rewrite historical peer
identity. Missing read identity or permission denies; no compute resource provider
is required merely to read history, and responses never claim resources verified
or execution allowed. Unknown IDs do not create records. Prepare/execute and
extra/invalid fields reject. The endpoint never owns an operation or execution
service. Trusted filesystem/callbacks remain assumptions; copy/rollback attacks
are not solved by SQLite read-only mode.

Only ordinary in-memory stream/synthetic-provider tests are permitted. Actual
Channel code and SQLite do not imply real TLS, remote node or GPU validation.
No model/forward/real network listener is run. Local task settlement and journal
terminal state do not imply physical device stop. This increment is private
protocol wiring, not deployment approval or distributed atomic resource reserve.

Caller owns explicit close of each endpoint. Coordinator cancellation settles only
its local transport reader; it cannot join an independently driven role task.
Role bootstrap owns service task lifetime and device quarantine. History responses
are authorized snapshots, not promises to retract bytes once transmission begins.
Read-only mode and inode/schema checks are not atomic anti-tamper snapshots.
