# Same-promise device capacity view

Base 58f8baff8d4faac7e4733a017766a52971ec7616. Private integration only.
`resources.device_capacity(ticket)` produces a view of the original exact local
Ticket. Supply it as DeviceCompletion.capacity; retain the independent trusted
DeviceCompletion.authority and all existing startup/identity/execution gates.
It creates no reservation, journal, permission or physical allocation.

Each hold_device requires the original live service/Ticket, HELD parent and
HELD control binding, exact stage/device/spec and actual once-bound input digest.
B cannot use its original placeholder input. Complete parent/control snapshots
are compared before yield, after body, after physical fence exit and after outer
authority/capacity fence exits. No local lock/transaction spans external holds or
caller work. No network await is introduced.

The SAME existing capacity backend must additionally implement
`hold_device(descriptor, demands, action)` as a synchronous, non-suppressing fence
of its already retained exclusive allocation. It must query/check that SAME
resource_id, not create a new lease, and never release it on context exit.
Missing hook rejects. All return values must be None; a bool/receipt is no grant.
Task authority remains checked through the service's existing hold path and the
independent execution provider. A callable alone proves no real authorization.

After a valid original ticket is identified, any failed managed hold poisons its
original RID and marks parent/control UNKNOWN without releasing charge. Cleanup
write failure is aggregated and in-memory poison remains. Copied tickets/history
cannot poison an unrelated RID. Closed/reopened/PID-invalid services remain denied;
reopening only exposes historical queries, not fresh qualification.

Whole-GPU exclusivity is an UNVERIFIED deployment prerequisite, not provided by
this view or inferred from CUDA availability. No assumption is made that IDC AI
supplies Slurm, exclusive GPUs, device access control, profiler permissions, or
any grant issuer. If identity, authority, persistent resource commitment or
physical exclusivity cannot be established, the real path must remain denied.
Queries still report physical_capacity_verified=False/execution_allowed=False.
No default provider is installed and no v12 execution restriction changes.

Tests use actual SQLite and resource/provider logic with explicitly synthetic
external fences and fake device runtime. They cover exact ticket, same records,
missing hook, failure retention, callback replacement, B actual digest, restart,
Event-coordinated quarantine and existing DeviceCompletion wiring. They do not
prove actual allocations, authentication, CUDA completion, global replay
protection, distributed atomic reservation, or physical stop. No model/Torch/GPU,
real node or network/listener runs. No release/retry/recovery API is added.
