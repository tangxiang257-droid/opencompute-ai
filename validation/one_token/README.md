# Private one-token assembly — source, not execution authorization

Fixed base: ed317c1d360c063b924db9b4e6d1c21874ae0378. No public route,
listener, CLI, default execution grant, model invocation or deployment is added.

`cluster_p1.private_one_token.run_one_token` composes one registered request,
epoch 1, one token: existing LocalAttemptJournal/StageOwner stage 0 -> existing
ReceiptAttemptJournal hidden receipt -> private bounded stage-1 driver/spool ->
existing result artifact ledger. It requires an existing stored input, matching
pending attempt and two stage assignments. The input snapshot/token hash,
preprocessing, model, layout, plan, layer ranges and instance claims are checked.
The stage-1 Spec binds the actual committed and received hidden bytes; this does
not issue a new execution qualification. Full logits retain token-major,
little-endian finite FP32 order; neither top-k nor a reference substitutes for
output bytes or logical charges.

The canonical driver and spool moved to cluster_p1, with validation import
shims preserving class identity. Existing StageOwner and LocalAttemptJournal
64 KiB full-BLOB and 256 KiB cumulative limits are unchanged. Stage 0's entire
encoded hidden envelope must fit 64 KiB. Stage 1 uses a distinct, explicit
private interface, with at most 8192 FP32 values/32 KiB per projection slice,
one complete-token object and fixed 32 KiB transport frames. The existing
artifact writer receives coalesced blocks of at most 64 KiB. A 151936-value
object is 607744 bytes and 19 frames; each Book charges 640512 including its
32768-byte manifest reservation. Artifact ledger has an additional independent
reservation. Nothing releases charges in this path.

## Real source, missing trusted providers

`compute.qwen2_projection_adapter` contains the real lazy tensor adapter source:
normalization/owned layers once, then `linear(normalized, head[column:stop])`
for one token. It never calls stage-1 full-logit prefill. It stages candidate
KV, accepts only one materialized projection at a time, then installs cache
only after the external completion contract succeeds. Stage 0 calls the
existing prefill through a fresh exclusive byte adapter. PinnedStageLoader
uses the existing config/weight SHA256 snapshot loader. No such operation was
executed in this validation.

All five providers are required, with callable methods checked before I/O:

- Authority: hold_pipeline, hold_load, hold_stage0, hold_call, finalize,
  hold_publish. Contexts yield None, cannot suppress body failures. Real
  bootstrap must enforce persistent session/stage consumption, authentic owner
  and current peer identity, revocation fences throughout work and publication.
  Callable shape or returning None proves none of this. New books, processes,
  sessions, copied or rolled-back databases must not restore qualification.
- Resources: reserve_pipeline receives actual assignments/layer ranges plus
  declared device/host/output demands. verify_stage1 must retain the SAME
  reservation when binding actual hidden bytes; quarantine keeps uncertain
  reservations charged. It must account for both stages, actual devices,
  weights, cold-load private snapshots, KV, attention workspace, buffers, files,
  metadata, orphan files and concurrency. Policy arithmetic is not measured
  RSS/GPU peak or a complete physical capacity service.
- Device: hold_load must open a real allocation/work scope BEFORE loader entry
  and cover partial loader failures and constructor work; successful exit must
  establish completion. open_scope/require_open/register-pending ownership,
  materialize, finish_scope and require_finished must cover every real stream,
  copy, allocator and retained tensor. The bridge enforces local ownership but
  provides no physical fence. Quarantine must retain uncertain work/resources.
- Loader: load and quarantine; pinned implementation permits each stage once
  and retains returned stages even when later validation fails. The external
  device load scope owns failures before the loader can return a stage.
- Conduit: require_current, exchange_hidden, result_binding, exchange_result.
  It must authenticate actual connections and bind current certificate and
  connection generations independently for A->B and B->result, plus owner,
  full request/attempt and authorization. The entry checks fixed result
  direction/port/attempt/B identity and matching frames. SHA tags and byte
  equality are not authentication. No actual TLS/channel provider is installed.

The optional factories seam is private trusted assembly injection for tests,
not untrusted input or an authorization alternative. Returning objects from
fake providers does not grant real execution. The ordinary-byte assembly is
in one process; it is not an implemented cross-device RPC scheduler.

## Failure, retention and evidence limits

Before any load, the source Book permanently consumes an assembly_calls row.
Both Books reserve full payload plus metadata separately; this is NOT a
cross-node atomic reservation. Load/operation/serialization/completion/auth/
publish failures retire touched owners and journal and quarantine reservations.
File close errors do not skip the independent durable retirement attempt.
Retirement/quarantine writes can themselves fail; aggregated errors are not
proof that retirement is durable. An external coordinator must then refuse
reuse. There is no restart/resume/reset/automatic recomputation path.

Source export requires successful stage-1 registration AND pipeline context
exit. The artifact publisher's optional private guard rechecks committed source,
receipt and live conduit in the final artifact transaction. This point check
is not atomic across the separate source/receipt/artifact databases: real
hold_publish must serialize revocation/source mutation for the entire commit.
The persistent result remains historical if commit succeeds but reply or later
authority exit fails; do not blindly retry/recompute or assert bytes vanished.
Logical terminal states never establish device physical stopping.

## Executed tests

Use only the explicit external guard recorded in the evidence bundle. It rejects
Torch/Transformers/Safetensors/worker imports, network connect/bind/DNS and
subprocess/system calls. No model, real/simulated node or socket was started.

`test_projection_adapter.py` AST-loads adapter source with symbolic tensor/API
replacements. It checks ordering, slices, exclusive stage ownership and local
revocation. It does not prove numerics or device completion.

`test_assembly.py` uses real SQLite journals, Book/spool files, envelope/framing
and artifact storage; authority/resources/loader/backend/device/conduit are
explicit synthetic replacements. It tests full-byte fetch, errors, replay,
load-failure consumption and final-source replacement via thread Events.
Its source replacement pauses immediately before artifact commit, mutates the
source through a separate SQLite connection, then resumes; no sleep decides
ordering. This is local storage/protocol wiring evidence, not real GPU or
cross-node authentication/atomicity acceptance.

Next integration gate is implementation and review of real coordinator
qualification + resource retention + device/load fences + authenticated conduit
as one assembled path, followed only by separately authorized real numerical,
peak-memory, cancellation and crash acceptance. More permissive caps or a
synthetic permit cannot substitute for these dependencies.
