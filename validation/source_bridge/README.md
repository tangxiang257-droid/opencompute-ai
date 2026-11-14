# Private current-source bridge (default disabled, no execution)

Baseline: 22aa1602c1ff04efd71470d0e6d12a670a492a67. No default imports,
HTTP route, deployment or production identity provider is added.

`private_source_bridge._CurrentSourceBridge()` is disabled. Trusted local
bootstrap may call `_open_source_bridge` with the current ApplicationService,
declared model/contract pins, stable configured source namespace, fresh registry
path and a `LocalUserFence` provider that is the EXACT service USER verifier.
No production implementation of that abstract integration contract is shipped.
Missing integration rejects before creating a registry. Inheriting the class is
only interface conformance, not proof of real credentials, continuous revocation,
or an endorsement of an arbitrary caller-supplied Python object. The synthetic
provider in the tests must never be used as production authentication.

The provider implements synchronous `verify(credential)` and `hold(credential)`.
The latter is a non-suppressing, leaf-only local revocation fence, held through
the whole registration. It must not await, call a request callback, perform
network I/O, or acquire Authority/coordinator/SQLite locks. `verify` must validate
real USER issuer/audience/expiry/revocation and return the service's Principal.
There is no capability=True flag, owner-returning shortcut or fallback verifier.
Time can expire a credential while the fence is held; final verification can
reject a write that already committed, retaining only inert history.
If hold returns an awaitable, the bridge rejects it. Only an exact native
coroutine in CORO_CREATED state is closed, to avoid an unawaited-coroutine
warning without executing its body. Running/suspended coroutines and custom
awaitables are neither driven nor closed; cleanup remains provider-owned.

`register_current_source(credential, task_id)` accepts no declaration, owner,
nonce, namespace or path. It binds the exact current Authority, loop and verifier.
The shared private `_read_locked` helper contains the original reader's checks;
there is no public already-locked parameter and the bridge never recursively
awaits the public reader. Its lock-state check detects misuse, not hostile Python
calling private APIs while another task holds the lock.

Order: Authority lock → trusted identity fence → full source transaction exit →
synchronous coordinator bind → identity/liveness checks → fence exit → final
identity/liveness checks → Authority unlock → return. No await inside the held
Authority section. Coordinator `_authorize` is installed once and is unchanged;
the same current identity context covers initialize, query and bind. It never
reacquires the fence or Authority, so there is no C→F or C→A reverse lock order.
Its verifier checks are leaf callbacks required by the integration contract.

Known declaration/snapshot validation rejection causes no coordinator
write and does not stop unrelated valid work, provided identity/fence exit checks
are normal. A definite duplicate source/nonce conflict also preserves the bridge;
it is rejection, not replay success. All other exceptions in the identity/write
section permanently fail this bridge and attempt coordinator close once. A close
failure is remembered, not retried automatically. Explicit close sets the closing
gate before waiting for Authority. Pre-lock request cancellation has no write.
The bridge catches only DeclarationRejected and SnapshotRejected as normal source
refusal. SQLite and generic transaction-exit errors propagate to retirement even
before coordinator bind; malformed schema/ledger errors also fail conservatively.
Existing read_input deliberately normalizes its file-read/parser errors into
SnapshotRejected, so this does not distinguish every underlying file I/O fault.
Such refusal has no coordinator mutation; no source health/repair is claimed.
The shared reader pins its Authority at configuration and rejects replacement,
including while awaiting the old Authority lock.

After uncertain writes or later checks/exit failures, no successful receipt is
returned, no row is deleted and no nonce is replaced. Cancellation thrown inside
the synchronous section propagates after retirement; normal event-loop task
cancellation cannot preempt synchronous SQLite/RLock/file work. Existing 1s/5s
SQLite busy timeouts are not preemptive task deadlines or RSS/wall-time limits.

The source ledger is never marked executed or modified by registration. v12 plan
deny and triggers, v1 coordinator behavior and v2 history-only reopen are
unchanged. Current draft text is not used to validate historical text/render
declarations; tokenizer_status/internal_metadata_only and model_status/
unverified_declared remain unchanged. Returns always execution_allowed=False.

The two databases are not atomic together. Scope is one live exact Authority,
no bypass writers, one stable unrolled-back registry and trusted same-UID code.
Namespace is configured and persisted in the coordinator, not intrinsic source
identity. No restart bridge or globally unique/recovered source authority exists.
`query_history` describes a stored declaration only: raw low-level registrations
can still originate from hand-built data. History is never execution permission
or proof that source data is currently valid or was verified through this bridge.

The wrapper launcher installs network/subprocess/Worker/framework guards before
project imports. Only explicit suites are run, not whole-repo discovery/setup.
Tests use real SQLite/files and current components with synthetic identity,
metadata and integer tokens. No tokenizer, model, GPU, Worker, real TLS or network.

Fault injections: `_commit` before actual commit and after actual commit;
provider entry/exit/suppression; synthetic expiry/revocation; source wrapper error
after a normal source transaction; coordinator close wrapper error after real
close; injected CancelledError. These are NOT power-loss, real IdP, real source
SQLite COMMIT/ROLLBACK failure or OS close failure experiments. Resource/format
corruption and thread barriers are separate ordinary-data checks.
Four coroutine-boundary tests cover created, suspended, running, and custom
awaitable cases. The provider/test explicitly starts and cleans up its own
suspended coroutine; the bridge must not trigger its finally callback.

Run from evidence-package root:
`/workspace/cpu-reference-env/venv/bin/python -B -W error::RuntimeWarning -W error::ResourceWarning guarded_run.py bridge`
Existing named compatibility suites: `declaration`, `coordinator`, `risks` with
the same launcher. The interpreter path is environment-specific; use an existing
Python 3.12 + Pydantic 2.13.4 environment for independent reproduction.
