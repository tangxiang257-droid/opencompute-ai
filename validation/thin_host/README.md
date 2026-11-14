# Thin host preparation / synthetic lifecycle supervision

Base deba1d7b9d4a159d013e5677909cd7f4dc9d0d72. This increment is NOT a
cross-host deployment program or a production provider implementation.

`python -m cluster_p1.thin_host preflight --config PUBLIC_JSON --sha256 PIN`
reads bounded public configuration, reuses LaunchConfig's host-local checks and
requires the directory to belong to the process user with no group/other mode
permissions. It creates no claim and returns REFUSED / execution_allowed=false
with missing real dependencies. This is a Unix mode check, not proof of ACL,
mount/namespace, physical storage or distributed allocation isolation.

`run-once` is explicit but currently refuses for every configuration, exit code 2.
There is no JSON provider import, environment override, test-mode option or fallback
identity. The public async run_once also refuses. No real assembly exists yet for
fresh experiment execution admission, remote current permissions, device action
authority, platform capacity/monetary budget, C authenticated task import and the
connection topology. v12/v13 bans remain unchanged. Candidate validation does not
make any of these providers real.

_OwnedRun is a private synthetic harness, NOT reachable from the CLI/run_once. It
runs exactly one callback, registers actual StageLauncher/ReceivingLauncher
instances, seals new work, cancels/joins work and closes launchers before borrowed
services. It adds no persistent claim, authorization or retry state machine.
A cancellation-resistant work task or asynchronous close may outlive the bounded
wait. UNKNOWN retains tasks/services and never releases quota. Eventual task exit
does not change that report into execution success or restore qualification.
Synchronous event-loop blocking cannot be interrupted by asyncio timers; a future
real host requires an external platform-approved supervisor. No os._exit/kill,
automatic restart/new nonce or resource release is performed here. A future
supervisor must not treat process exit as proof of physical device stop.

OS signal registration is deliberately deferred to the future concrete trusted
process host: this denied CLI installs no handlers and must not overwrite another
component's signal ownership. Tests drive an asyncio.Event stop request, not an
actual SIGTERM. Provider construction, signal ownership and role endpoint
scheduling remain real follow-up work, not achieved by synthetic tests.

Tests cover refusal, strict config/no provider field, private directory, explicit
CLI denial/no claim, single use, stop before work, cancellation exit ordering,
work timeout, cancellation-resistant cleanup, exceptions, ownership. The pipeline test reuses actual launcher/SQLite/role operations/
C storage over memory streams with synthetic authorization and compute. It is
one test only, not a real cross-process/TLS/GPU or trusted-provider experiment.
