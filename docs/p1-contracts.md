# Phase 1 control-plane contracts

This opt-in package is not mounted in any API and does not change existing
workers, schedulers, SQLite tables or interfaces. It supplies metadata contracts
for later PRs. No GPU correctness, checkpoint recovery, performance or privacy
implementation is claimed.

`contracts.py` defines immutable Pydantic v2 records with strict input types,
forbidden extra fields, finite numbers, bounded identifiers/resources/tensor
sizes, SHA-256 hex fingerprints and half-open ranges. `Task` pins model digest,
owner, privacy level, payload reference, budget, deadline and resource limits.
`Session` additionally pins the layout digest, nodes, expected token count and
positive epoch. The layout digest must cover the complete canonical weight/KV
layout; its calculation is the planner's responsibility. Stage ranges must
cover all layers exactly in stage-ID order, without gaps/overlaps, and belong to
the same session/model/layout. Tiles pin both digests and stage/session/epoch.
The metadata store does not interpret or fetch payload references.

## Store API

`P1Store(path)` creates only `p1_` tables with `CREATE TABLE IF NOT EXISTS`,
using file-backed SQLite, foreign keys and independent connections. Opening it
never changes active task/session state. Transactions use `BEGIN IMMEDIATE`,
a 15-second busy timeout and rollback on every exception. Concurrent writers
serialize sequence checks and insertions. Tables are additive; remove the new
package/workflow to roll back integration (none exists yet). Retain or separately
archive the `p1_` tables if metadata is needed.

Every mutation requires a globally unique bounded `event_id`. An exact replay
returns its persisted original result, even after a later epoch or terminal
transition, without changing state; conflicting event payloads raise `Conflict`.
Failed operations roll back both metadata and event identity. New events always
check current state/epoch. This is an audit journal, not a distributed consensus
or unbounded production retention strategy.

- `create_task(task, event_id=...)`: insert a new QUEUED task.
- `get_task(id, owner_id=...)`: owner-checked task read; mismatch raises
  `PermissionError`. `cancel_task` requires the owner and atomically cancels an
  active bound session, or the queued task before binding.
- `bind_session(session, stages, event_id=...)`: one immutable binding per task,
  initial epoch 1, PREPARING state, exact owner/model/layout/node coverage.
- `stage_transition(session_id, epoch, stage_id, 'ready', event_id=...)`:
  individually attest stage readiness. A failed/cancelled stage terminates the
  entire session and task. `stage_state` reports individual PREPARING/READY,
  then the shared RUNNING/VERIFYING/terminal/interrupted lifecycle. A new epoch
  resets all stages to PREPARING without deleting historical records.
- `transition(session_id, epoch, State, event_id=...)`: only explicit edges from
  `TRANSITIONS` are legal. QUEUED→PREPARING happens through binding;
  PREPARING→RUNNING requires every stage READY. RUNNING→VERIFYING requires
  every stage's verified committed prefix equal to `expected_tokens`.
  VERIFYING→COMPLETED is explicit. PREPARING/RUNNING/VERIFYING may interrupt,
  fail or cancel. Terminal states never revive. Session state updates task state
  atomically. There is no implicit same-state success except event replay.
- `tile_transition(tile, state, event_id=...)`: strict RECEIVED→COMPUTED→VERIFIED
  →COMMITTED sequence, with byte-for-byte immutable tile metadata. RECEIVED
  requires previous local tile and matching upstream tile committed, so this
  deliberately conservative metadata subset permits one uncommitted tile per
  stage. It is not the later streaming transport queue. `commit_tile` only
  delegates the final transition; it cannot bypass verification. Only committed
  tiles advance the contiguous stage prefix. The verifier is trusted and must
  establish actual computation correctness outside this store; hashing or
  acknowledging receipt does not establish correctness.
- `committed_prefix(session_id, epoch)`: minimum committed token prefix across
  all stages. This is metadata evidence, never proof that GPU KV survived.
- `identify_interrupted(event_id=...)`: explicit startup barrier marking active
  sessions/tasks INTERRUPTED, returning their IDs. Invoke after stopping old
  producers. `advance_epoch(session_id, expected_epoch, event_id=...)` is the
  only path from INTERRUPTED to PREPARING; it fences old results, resets tile
  prefix/readiness to zero and retains history. The caller must reload/replay
  necessary state and re-attest readiness. No KV restoration is claimed.

All methods except owner-specific task read/cancel are trusted control-plane
methods: an eventual authenticated API must authorize access before invoking
those methods, including `get_session` and event replay. This PR supplies no
public authentication endpoint and no node identity verification. Model/layout
hashes identify exact plans but do not independently prove execution correctness.
Privacy flags and resource limits are validated metadata; admission enforcement,
resource accounting, cost settlement and payload encryption are later work.

## Verification

Run `python -m unittest discover -s tests -p 'test_p1_*.py' -v` and the existing
`test_cluster_v03.py` suite. The isolated CI workflow runs both on Python 3.12.
Tests cover concurrent writers, non-destructive coexistence with v0.3 rows,
restart identification, real epoch fencing, replay/conflicts, ownership,
fingerprints, contiguous stages/tiles, independent readiness, lifecycle gates
and terminal rejection. They do not require a GPU or external service.
