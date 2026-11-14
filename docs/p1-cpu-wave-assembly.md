# Private one-shot CPU prefill assembly

This increment adds a private helper without changing any existing module,
loader, owner, wave scheduler or default/public entrypoint. The baseline is
17e5245ccf1f1a94e4e3bd17b19544aab4022ce3. Importing the helper does not import
Torch, load files, create model stages or run computation. Explicit invocation
would call the existing real private CPU factory unless the test replaces it.

`_run_cpu_prefill_waves` derives its complete plan from one authorized journal
snapshot, not a competing caller plan. It validates weight/config pin syntax,
weight pin against task/session model identity, instance IDs, complete immutable
known-token inputs, absence of old attempts and the existing whole-plan byte
budget before loading. Config pin and root remain trusted caller inputs. Matching
pins prove relative byte identity, not model authenticity. The config pin is not
added to the persistent journal schema; layout_hash is not recomputed from the
loaded architecture. This is trusted local assembly, not a new authentication or
cross-process provenance protocol. Existing #19 strict JSON fixes are not implied.

A required synchronous `prepare_authorize(spec)` authenticates and fences each
load0/load1/postload preparation step and must yield the same owner principal.
The frozen spec contains root, both pins, plan digest, task/session/epoch and
instance identities. Each step validates the current journal again; each load
also validates after the factory returns. The preparation authorization is not
execution authority: every forward admission/commit still requires the existing
separate live execution fence. The caller must hold its own trusted external
revocation lock until a preparation context exits. SQLite cannot supply it.

Both CPU factory results must retain the exact `_PlannedOperation` type, binding
and fixed ranges. The exact objects pass directly to `run_waves`, without generic
wrappers that would bypass StageOwner's private planned-call check. The helper
never returns a raw stage or its operation closure. The wave owns the same two
instances through a known-token causal prefill, then retires them. Autoregressive
decode is not implemented. No additional scheduler/store/framework is introduced.

## Synchronous loading and resource boundaries

There is no SQLite transaction or await across a loader call. The real loader is
synchronous and cannot be interrupted mid-load. Cancellation checkpoints occur
before preparation, between stage loads and before dispatch. Pending cancellation
is delivered there, rather than detaching an untracked `to_thread` loader. A load
may finish and allocate resources even though cancellation is already pending.
The event loop may be blocked for the entire load; this entrypoint provides no
bounded preparation latency or prompt cancellation guarantee.

The current factory snapshots the full weight file separately for each stage.
Stage0 weights remain resident while stage1 loads. The journal's 64 KiB/object
and 256 KiB shared logical byte budget does NOT bound temporary weight files,
Torch allocations, weights, KV, tensor/byte copies, SQLite/WAL, RSS or disk usage.
An actual experiment still needs a separately scoped tiny profile and resource
budget. This helper does not supply OS isolation, global cold-load reservation,
or exclusivity against another preparation invocation. Concurrent invocations
may waste loads; existing per-attempt admission denies duplicate execution.

If either load, a state check, or a preparation context exit fails, the helper
does not dispatch or automatically retry. Loaded closures remain internal; Python
tracebacks may retain them, so immediate unloading/freeing is not promised. After
any attempted wave, existing rows cause subsequent preparation to be rejected:
a journal prefix cannot restore KV. During a running wave, failures/cancellation
retire both owners and wait for already started operations as before. Waiting for
operation return is not executor-thread join or device shutdown. State may have
advanced without a committed result. There is no reset, compensation or recovery.

## Verification actually performed

Twenty new tests use ordinary controlled factory/operation stand-ins, the real
WaveAttemptJournal, StageOwner, `_PlannedOperation` and `run_waves`. They verify
same pins/plan for both separate stages; exact operation identity; full byte flow
and tails; malformed pins/inputs/budget refusal before any load; preparation and
execution permission separation; failure of either loader; post-load state change
and context-exit failure; synchronous-loader completion before pending cancellation
is delivered; no repeated preparation after attempts; rejection of wrapped/wrong
bound operations; exception suppression safety; and synthetic private-state advance
followed by conversion failure remaining ADMITTED without a blind retry.

The first 12 cases were recorded RED for the absent helper and then GREEN. Eight
additional boundary regressions passed. The explicit ordinary subset totals 184
passing tests (164 prior + 20 new), ResourceWarning-as-error and framework imports
forbidden. No real factory/loader, model file read, Tensor, forward, node, serializer
or benchmark was run for this increment. Stand-ins do not prove real weight parsing,
PyTorch numerical behavior, KV correctness, memory usage, model concurrency or
speedup. Prior artificial CPU comparison results do not cover this joint path and
are not used as authorization or evidence for any additional real execution.

## Preparation is not execution permission

The final preparation phase is named `postload`, not dispatch. Its context has
exited before the final cancellation checkpoint and actual `run_waves` call.
Revoking only preparation permission after that phase does not revoke execution
permission. A controller that intends to stop execution must revoke the separate
execution fence or cancel the active journal/request.

Each StageOwner calls its trusted admission callback before enqueue, and no await
occurs between admission return and enqueue on that owning event loop. The local
SQLite transaction checks current plan/source and the execution context, writes
ADMITTED and reserves bytes atomically. The external authorization context must
hold its own revocation fence through that transaction. This is an admission
linearization boundary, not an atomic test at the eventual CPU function entry.
Revocation after successful admission can still allow the already-admitted body
to start or finish and advance state. Output commit independently rechecks live
authority. It may reject publication but cannot undo computation. These properties
are distinct from postload metadata validation and from physical cancellation.
