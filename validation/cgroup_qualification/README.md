# One-shot environment qualification

Dedicated feature branch only. Uses main's existing manually dispatchable
`gpt2-compat.yml` path with entirely replaced branch-local content; no GPT-2 job
or model dependency remains in this version. Main is unchanged. Commit carries
[skip ci] to suppress all existing push/PR CI; no PR is opened. Dispatch exactly
once against the reviewed branch SHA, not main. Job permits attempt 1 only.

Standard public ubuntu-24.04 runner; contents:read; 10-minute job timeout;
no secrets injected, artifacts, caches, package installations or server access.
Checkout is SHA-pinned with persist-credentials:false. Root probe gets an empty
environment plus PATH, and prints only fixed qualification fields/error numbers.
The temporary systemd service has CPUQuota=25%, MemoryMax=128M, swap=0, TasksMax=16,
RuntimeMaxSec=60, TimeoutStopSec=10. Only its own verified cgroup is modified.
Manager moves to its own leaf to satisfy no-internal-process controller rules.
Empty test leaf is restricted and delegated to the existing ephemeral runner UID.
No workload is created there; permissions are tested by opening control files.
Cleanup removes only these empty leaves, and systemd stops/collects the service.

clone3 is invoked with NULL and size zero on x86_64. EINVAL establishes only
visibility of a handler rejecting invalid arguments; ENOSYS/EPERM or unexpected
results fail closed. No successful clone, atomic entry, privilege-drop workflow,
descendant containment, production launcher or GPU stopping is validated.
This explicitly does not substitute syscall recognition for successful
CLONE_INTO_CGROUP permission/correctness. Those require a later authorized test.
