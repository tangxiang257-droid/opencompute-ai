# Explicit input/output Tile v2 declarations (not a runtime protocol)

`cluster_p1.tile_contract_v2` adds a separate pure contract alongside legacy Tile.
It imports only Pydantic, standard library, existing contracts and bounded_json.
No store, channel, stage, Worker, model, schema/table or existing API is modified.
There is no migration executor, database write path or live scheduling wiring.

## Scope

An explicit integer schema_version=2 is required. TileV2 binds task, owner,
session, epoch, model/layout digests, stage, tile ID and a half-open contiguous
token span. Immutable input/output descriptors separately specify role, dtype
and shape. The supported subset is batch one, two contiguous layer stages,
complete prefill, at most 64 tiles per stage, and tile-major A0,B0,A1,B1 order.

- Stage 0: int64 token_ids [1,N] -> float32 hidden_states [1,N,H].
- Stage 1: float32 hidden_states [1,N,H] -> float32 full_logits [1,N,V].
- Both token axes equal token_count; stage ranges and token spans have no gaps,
  overlap, duplicate IDs or omitted tail. The complete declared request is needed.
- Predecessor output equals successor input, not successor output. H and V can
  legitimately differ; no hidden/logits relabeling or silent output truncation.

`validate_prefill_plan` reuses the existing Task/Session/Stage validation, checks
bindings and full layer/token coverage, and compares declared widths from the
trusted caller. The result says declarations_consistent only; execution_allowed
and payload_verified are always false. Dataclasses/types are not capabilities.
It does not prove these widths describe real weights, actual token contents,
padding/mask semantics, KV, numerical correctness, authenticated sender or current
epoch/liveness. The element bounds and 64-tile cap limit this metadata subset;
they neither increase transport/storage quotas nor reserve memory. In particular,
a structurally accepted full_logits declaration may exceed downstream capacity.
Downstream static budget and actual reservation checks are still required.

## Migration preflight and coexistence

`assess_record(raw_bytes)` uses bounded strict UTF-8 JSON (64 KiB maximum).
A valid old Tile without schema_version returns legacy_unchanged: the old
single-descriptor record cannot establish both roles and is NOT automatically
convertible. A well-formed v2 returns v2_structure_only, not complete-plan approval.
Malformed, unknown-version, extra-field, duplicate-key or unsupported records
return unsupported. Automatic migration and execution are false for every case.
No input bytes or old records are rewritten and no lossily inferred conversion
is returned. V2 cannot pass legacy Tile validation and legacy cannot pass TileV2.
Existing old journal replays and storage semantics are unchanged. Durable storage
of v2 and authoritative state transitions remain a future separately versioned
write protocol, not something implemented by this module.

## Cancellation, replay and freshness

The existing cancel_task is task-scoped, NOT epoch-scoped. If advance_epoch wins
its transaction before cancellation, cancellation can affect the new epoch.
A future single-attempt cancel must have a distinct explicit expected-epoch
operation; never present cancel_task as single-generation cancellation.
Exact old event replay can return historical success with no new database writes.
Such a return is not current task state, evidence that the producer is running,
or permission to reuse old KV. Likewise an old #20 progress report is just an
observation. Future mutation/publication must recheck actual owner, current epoch,
state and dependencies in its own authoritative transaction. This pure validator
cannot do that and does not claim to.

## Tests and evidence

11 new tests plus the 49 ordinary combined-baseline tests pass (60 total), without
Torch/Safetensors/model imports, tensors, network, GPU, Worker/node execution or
production access. Tests cover int64 -> hidden -> different-width logits, tail,
roles/dtypes, token axes, batch, fingerprints, epoch, strict numeric/version types,
layer coverage, missing/duplicate/gap/overlap/order, bounded-record classification,
legacy non-conversion and no I/O. Initial RED was missing new functionality, not
an existing runtime race; a development failure in JSON nested tuple handling was
fixed using a field-level version validator and the JSON test then passed.

Run only: `python -m unittest discover -s tests -p 'test_p1_tile_contract_v2.py' -v`.
The full repository/model CI has NOT run. This slice does not complete the
same-request execution chain, define TLS grants, authenticate payloads or prove
physical parallelism. No public gate, migration, merge or deployment is enabled.
