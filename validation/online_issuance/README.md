# Private online once-only issuance

Base967b79bc06e278d08814fe37dd0772424ec3d47b. This work uses supplied actual
Channel objects, independent temporary SQLite stores and synthetic providers in
validation. No listener, process, model, GPU, certificate or production provider
is launched, loaded, installed or implicitly trusted.

Parallel design reviews preceded implementation; see delivered evidence package.
Installation qualification is a live local runtime fact plus durable consumption,
not a transferable receipt. Central source/task/nonce/stage ownership is permanent.
Role acceptance is separate from INSTALL_CONSUMED, which must precede creating
any execution journal. Unknown commit, failed creation, reconnect and restart do
not restore it. All persisted receipts deny execution. Reopened stores are history
only. Same-process malicious mutation or copies/rollback require external protection.

Each end fixes its own exact Channel and identity; opposite ChannelIdentity peer
and random generation values are not equal. Bounded hello/issue/issued messages
bind issuer incarnation, both challenges, full declaration and request transcript.
Credit-release await is followed by fresh local identity/permission/resource checks.
No local identity/SQLite fence spans network await. Observed disconnect invalidates
eligibility; no instantaneous detection of remote restart is promised.

Installation of stage1 binds a full fixed plan and original input snapshot. Actual
hidden digest is bound later once from the authenticated persisted receipt in the
existing control/operation path, never guessed from token input. Installation is
not execution: RegisteredRole/ControlJournal still consumes before the same actual
A/B operations and backend completion scopes. Bootstrap device readiness checks
are not per-operation device completion proofs. Missing providers remain deny.

Dedicated online receipt preparation must preserve _deny_execution and withhold
H until local install consumption, journal/plan creation and peer validation are
complete. Legacy local mode remains separate; failures do not fall back between
modes. Existing byte, frame, Book and resource retention limits remain mandatory.

Tests use real protocol/storage code with plain synthetic bytes, not model numerics,
real TLS, distributed atomic reservations, physical GPU/RSS capacity or stop proof.
A deployment still needs genuine task authority, source resolution, identity,
resource service, device completion implementation and authorized role startup.

Installation fixes input_snapshot, token_digest and preprocessing from the trusted
source resolver. Device, instance and layer range must match the installed role.
A permanent local association records bootstrap reservation, control reservation,
full actual binding and charge before provider verification. The mandatory
hold_control provider verifies the existing commitment; it must not allocate anew.
Failure burns the association and retires eligibility. Online begin additionally
requires the exact local CONSUMED control record. This is not distributed atomic
reservation. The issuance registry currently exposes local history queries only;
no reconnect query response recreates installation qualification.
