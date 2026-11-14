# Role-local control entry (fixed base 307b493)

`private_experiment_topology.bind_control_ready` composes the existing
`StageLauncher.attach → attach_actions → announce_control` sequence. It is
private, defaults disabled, creates no provider/listener/process, and does not
call `install`, build an operation, or dispatch computation. Caller supplies an
already-installed exact launcher with ACTIVE original installation session,
actual operation, dedicated control stream and unstarted action Channel, and
explicit identity/permission/clock providers and issuer pins. Method shapes are
not authentication; trustworthy bootstrap remains mandatory.

After validated inputs transfer ownership, a one-time latch and pending flag are
set before any await. The action Channel is first taken through the existing
complete launcher purpose checks; it is never merely checked and left available.
The original coroutine alone can continue action attachment or announce while
pending, and nested owned jobs are refused before taking supplied streams. Launcher and underlying ControlService refuse business
while pending. The control Channel inherits this flag before further attachment
awaits. READY plus the final current checks clear pending synchronously. Replay
or concurrent calls refuse without interrupting the existing owner. Failure or
cancellation closes unadopted streams and retains existing launcher cleanup. If
external close is already waiting for the assembly caller, that caller must exit
first; the external owner must join the original retained close task. The entry
cannot promise that external cleanup has settled when its own error returns. No
refund, retry, new session, restored slot, or device-stop assertion follows.
The entry deliberately does not nest existing `_run` operations inside another
`_run`. It borrows journals/resource services; it never closes those services.

Central still calls `receive_control` independently on its authenticated
connection, then configures the existing ActionAuthority/ActionServer. Role and
central share no descriptor object or SQLite copy. B still builds only after
actual committed A input and requires its independent C gate before dispatch.
Original query, action lease, current authorization and result commit checks
remain in effect. READY is not a distributed atomic handoff.

Tests use actual local SQLite, existing launcher/control/action/result components
and memory streams with LAB identity/capacity/backend doubles. The original
fixture now passes SessionPolicy explicitly to both existing install APIs; the
session pipeline no longer monkeypatches their methods. Event barriers verify
premature calls and cancellation. No real TLS, model, GPU, node, or listener is
run. Public thin_host.run_once still refuses; this is callable internal wiring,
not a standalone cross-host process or a trusted provider configuration.

## Actual Phase 1 critical path

| Step | Existing callable code | Remaining prerequisite |
|---|---|---|
| Request and explicit confirmation | ExperimentConfirmationApp / ExperimentAdmission; existing input and consent records | Trusted issuer/public key, HTTPS identity binding, explicit budget/privacy/experiment consent |
| Online installation | installation_service; RoleStartup/StageLauncher.install(session_policy=...) | Independent role processes, authenticated supplied streams, current platform commitments |
| Role control readiness | This entry + central receive_control + assemble_actions | Trusted local bootstrap and endpoint ownership; no default CLI enablement |
| A→B bytes and B input readiness | HiddenSender/Receiver, PreparedReceiptConnection, CommittedBInput | Dedicated actual authenticated connections, approved source/destination storage |
| Device execution | PinnedStageLoader, device_completion.DeviceCompletion, RoleResources device binding | Actual model/software pins, GPU UUID mapping, allocation/exclusivity proof, device completion validation |
| C receive and result retrieval | CReceivingPolicy, ReceivingLauncher/ResultIngress, existing artifact fetch | C's independent permission/import, live destination capacity, current identities and privacy policy |
| Shutdown | Existing owner close/session fences, thin host supervision scaffolding | Actual process supervision, unresponsive device isolation; UNKNOWN charges stay retained |

The first new code contribution is removal of test-only role-control assembly;
there is still no deployment-ready multi-process bootstrap. Public identity,
node certificates, truthful allocation/lease/exclusivity and actual GPU/storage
conditions cannot be inferred from these tests. They must be supplied and
verified when machines and a specific experiment are authorized. Existing v12/v13
prohibitions, logical limits and UNKNOWN non-retry semantics are unchanged.
