# REVIEW ONLY — one real CLONE_INTO_CGROUP attempt

This candidate is NOT pushed, compiled, executed or dispatched. It derives from
686f484bfbdf1226faddb1cc74721463afe75263 (earlier environment-only probe).
Production code, main, PairSupervisor and LinuxCgroupScope are unchanged.
No new user or identity-switch permission is requested. Both native parent and
fixed child run as root inside the temporary constrained service, exactly as
explicitly disclosed here; this cannot validate privilege drop or escape denial.
If unprivileged child testing becomes necessary, stop and request separate review.

Proposed actions AFTER separate confirmation:
1. Push only reviewed branch codex/clone3-entry-review, with [skip ci] commit to
   suppress unrelated push/PR workflows. No PR or merge. Verify remote SHA.
2. Dispatch existing gpt2-compat.yml registration against that branch exactly
   once. Branch version contains only this probe, not model compatibility jobs.
3. Standard ubuntu-24.04 VM, contents:read, checkout pinned, no user secrets,
   caches or artifacts; 10-minute job maximum. Compile the fixed C source with
   warnings-as-errors. No repository modules or tests are executed.
4. Create one unique systemd delegated service named by run ID/attempt, bounded
   to 25% of one CPU, 128 MiB, no swap, 16 tasks, RuntimeMaxSec=60 and stop timeout
   10 seconds. Probe verifies it is the sole initial process in precisely that
   unit's domain and reads back limits/controllers. Never touch ancestors or
   other services. No production server connection.
5. Create management and empty probe leaves exclusively under this service;
   move only the management Python process to manager before enabling controls.
   Probe leaf limits: cpu.max 25000/100000, memory.max 64 MiB, swap 0, pids.max 4.
   Read back each limit, domain emptiness and kill availability. No chown/chmod.
6. Start a fixed single-threaded native management program in manager. Pass only
   pinned leaf and kill FDs plus expected proc membership string. It makes exactly
   one clone3(CLONE_INTO_CGROUP|CLONE_PIDFD), with SIGCHLD and no CLONE_VM.
   No fork/Popen/migration fallback. subprocess here launches only the native
   parent, never the measured child. No Python preexec_fn or child interpreter.
7. Child closes scope controls and unused pipe ends, reads /proc/self/cgroup,
   reports via pipe, and blocks for release. Parent independently checks
   /proc/<child>/cgroup and exact singleton leaf cgroup.procs while child waits.
   Child membership is never printed and child is never migrated after creation.
8. Parent releases child, waits on pidfd and waitpid, verifies normal zero exit.
   Wrapper independently requires empty cgroup.procs and populated=0, removes
   only the empty probe leaf, disables its own parent controllers, returns the
   manager process, then removes its empty manager leaf. No recursive deletion.
9. Read job results and report observed checks, failures and cleanup limitations;
   do not retry even if compiler, kernel, permission or runtime checks fail.

Failure protocol: C alarm interrupts blocking protocol after five seconds; pidfd
waits are bounded. Known child failure requests cgroup.kill through pinned FD and
attempts exit observation/reap. Wrapper timeout after ten seconds stops the native
parent; if a child remains, wrapper requests its leaf kill, retains the domain
for service cleanup and fails rather than claiming successful cleanup. The shell
trap stops only this unique unit. Systemd KillMode=control-group and runtime/stop
timeouts provide an independent fallback. If a process is kernel-stuck, neither
these timeouts nor SIGKILL prove it exited; job must fail and hosted VM disposal
is ultimate containment, not an asserted successful reap. No automatic retry.

Logs contain fixed status strings only, not child IDs, paths, user data, secrets
or server details. GitHub itself emits normal runner/checkout metadata. No model,
GPU, business node, fake node or computation fixture is present. This tests only
one native entry and ordinary exit, not descendant escape, cancellation races,
crash recovery, hardened broker authorization, GPU stopping or device reuse.

Review-only evidence: source and whitespace inspection. No compiler, parser,
syscall probe, child process test, push or CI was run for this candidate.
Historical supervisor 21/22 failure remains unrerun. The earlier successful
invalid clone3 probe is not evidence that this valid invocation will succeed.
