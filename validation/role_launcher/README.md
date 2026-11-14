# Private role launcher — ordinary-data validation only

Fixed base: 65dba55514bfc3257ee63908d04161059660048d. This increment adds a
role-local lifecycle entry, not a listener, CLI, scheduler, credentials issuer or
production deployment. No model/GPU/real-node/network experiment is authorized.

## Trusted bootstrap inputs

`LaunchConfig(raw, expected_sha256)` accepts bounded canonical JSON and an
independent expected digest from trusted LOCAL bootstrap. A digest bundled with
peer-controlled JSON is not trusted configuration or permission. Exact fields:
version=1; role=A/B/C; node; issuer_node; issuer_certificate (DER SHA256);
coordinator_node; descriptor_sha256; absolute existing root; charge; control_budget.
Unknown fields/provider module names/dynamic imports/duplicate keys/noncanonical
encoding/bool budgets are rejected. The complete pre-existing descriptor binds
source, full plan, identity/placement, model, task/consent and resource IDs; no B
hidden digest is invented at startup. C config charge must equal C descriptor.

TLSConfig and already-authenticated dedicated readers/writers or Channels come
from the trusted connection layer. Existing TLS/hostname/fingerprint validation
remains active. Providers are supplied as local objects through existing
interfaces, never resolved from untrusted names. Identity, task permission,
resources, completion, loader and stage authority must all be present. Installation,
control and operation share the exact existing RoleResources service and ticket;
no second reservation or permission is created here. A method's existence or a
synthetic test yield is not evidence that a real provider is authorized.

The same boot owns each supplied connection exclusively. Known writer aliases,
claimed Channels and repeated gate attachment are rejected before adoption;
foreign preflight-rejected connections remain caller-owned. Root is pinned by
path/inode/PID. A permanent O_EXCL marker contains inert config JSON (despite the
common path helper's .sqlite suffix); partial/fsync-unknown claims stay occupied.
It is NOT an execution ledger, remote replay defense, resource grant or recovery
credential. Existing journals remain the source of execution/charging state.
New roots, database cloning/rollback and malicious same-process mutation are not
global uniqueness protection. Bootstrap must not recreate eligibility after an
uncertain attempt by selecting another root.

## Actual assembly sequence

1. `await StageLauncher.create(...)` validates config/providers and claims the
   local launch slot, then calls existing RoleStartup.create on supplied streams.
2. `await install(...)` owns the installing task, including B's existing prepared
   receipt barrier. No operation may be constructed before install returns.
3. `build_operation(spec, policy, tokens=...)` builds actual StageAOperation; B
   takes its existing Spool and checks actual committed receipt bytes through
   StageBOperation.received_bytes. It does not accept an assumed B input as fact.
4. `await attach(...)` builds actual InstalledStageRole, registers against the
   same resources and starts the control Channel exactly once. serve/gate remain
   unavailable until H write/drain and final checks complete. No new backend is
   loaded during this assembly. Caller schedules `serve_once()` explicitly.
5. `await ReceivingLauncher.create(...)` wraps actual ReceivingRole on C's local
   fresh store and original authenticated authority Channel. Its owned control
   calls perform pending/activate/revoke; attach_result uses actual ResultIngress.
6. Dedicated unstarted B/C Channels are passed to each launcher's attach_gate;
   missing identity/authorization denies. C must be RECEIVING before ACK. Existing
   ControlService still enforces ACK, current task/resource fences and durable
   consumption before B executes. Launcher cannot override that enforcement.
7. C `receive_result(...)` owns receipt/persistence. `attach_history(...)` always
   uses this same local C store, with SEPARATE current-principal/query providers
   and existing ReceivingHistoryService's independent RO/query_only connection.
   It requires its own completed H phase before query_once. This is live-launcher
   ownership only; after close, reconnect history must use the pre-existing
   standalone read-only bootstrap and cannot recover the launch claim.

Launcher close first seals new entries, cancels and joins owned phase tasks, then
closes owned operations/readers/journals. Original issuance/control reader exit
also seals the launcher without assuming the caller remembers ordering. Repeated
caller cancellation waits for owned cleanup. Non-cancellation child errors and
close/wait_closed errors are retained and reported together. In particular raw
writer.close failure must not skip wait_closed. Provider replacement observed by
check permanently seals admission, even if the old object is restored. Borrowed
install/resource/C stores and spool/artifact services are closed by their outer
owner AFTER launcher close. Configuration/permission errors before connection
adoption do not authorize launcher to close another owner's stream.

Query failure is a failure of this owned launcher scope and may retire receiving;
this private entry is not a fault-isolated multi-client public history service.
There is no hard end-to-end cleanup deadline or force-stop mechanism. Existing
operation/backend completion owns device settlement; logs and thread exit do not
prove GPU physical stop. C ACK followed by unobserved remote revoke can still race
B computation. Unknown commitments retain charge, no distributed atomic reserve.

## Evidence and remaining work

`test_pipeline.py` actually exercises launcher create/install/build/attach/serve,
C control/result/gate and same-store history on the existing A→B→C storage path.
All SSL, authority, capacity, user confirmation and backend providers there are
explicit LAB doubles. This does NOT attest real PKI, user consent, physical
allocation, numeric model output or device completion. Real bootstrap must supply
those services and account for all channels/queues/host/VRAM/disk metadata.
No synthetic permit or default production provider is added. Old limits and
v12/v13 execution prohibitions are unchanged. No public route/import is added.

Only private library entrypoints are supplied. An actual deployment still needs
reviewed host supervision, trusted config provisioning, real connection creation,
real identity/task/resource services and explicit authorization to start devices.
