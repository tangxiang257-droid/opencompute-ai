# Same-request pipeline progress: a metadata observation, not execution permission

Adds `P1Store.pipeline_progress(session_id, epoch, owner_id=...)` to the existing
PR #16 journal. No transport, executor, router or worker is wired. This does not
merge PR #16, #17 or #18, and is unrelated to the export-only changes in #19.

## Return contract

The frozen result includes the session's current state/epoch and, for each ordered
stage: committed token prefix, next tile ID, remaining tokens, the existing
in-flight Tile/state (if any), and the exact committed predecessor Tile at that
next tile ID (if any). It does not fabricate an upstream range from a global
minimum prefix or prescribe a new tile size. A one-token tail stays one token.
For stage 0, upstream_tile is None because input comes from the caller, not a
previous stage. Missing downstream upstream_tile means no matching committed
predecessor was observed; it is not evidence of a producer failure.

`remaining_tokens` includes uncommitted in-flight work. A completed stage has zero
remaining tokens; next_tile_id then describes the end of recorded sequence, not
an instruction to submit another tile. Terminal and interrupted sessions may be
observed in their current epoch. Old epochs and wrong owners reject.

Stage 0's next progress does not depend on stage 1's earlier tile finishing: after
A1 commits, A2 and B1 may both have existing received metadata under the journal's
normal methods. This is not proof they compute concurrently or fit memory.

## Consistency and authorization

The owner comparison, current epoch, session state, local prefixes, in-flight
states and predecessor records are read inside one SQLite read transaction from
a mode=ro connection with query_only enabled. This method adds no events/schema,
issues no DML, and does not create a missing database. Read locks may briefly
hold up a writer on rollback-journal databases; WAL is not enabled by the API.

This is a trusted local control-plane interface. owner_id must come from the
caller's authenticated context, not an unauthenticated request body. It does not
provide authentication or permission to expose these records publicly. The
result can be stale immediately upon return. Existing mutation methods must
still revalidate identity, epoch, state, prefix and dependencies atomically.
`execution_allowed` is always false: no reservation, resource/credit grant,
physical readiness, execution capability, KV validity or recovery promise is
created by this observation. Dataclass construction is not a security boundary.
The SQLite journal and its existing mutation protocol remain trusted; this is
not an offline corruption audit or an attacker-controlled-database validator.

## Verification

```
python -m unittest discover -s tests -p 'test_p1_progress.py' -v
python -m unittest discover -s tests -p 'test_p1_store.py' -v
python -m unittest discover -s tests -p 'test_p1_contracts.py' -v
```

8 new tests plus 12 existing contract/store tests pass using temporary SQLite
and ordinary validated metadata. A deterministic event barrier allows another
connection to commit between session and stage reads in a test-only WAL database;
the observed result remains one earlier snapshot, and the next query sees the
new commit. No sleeps are used to decide that race. Tests also cover owner/epoch,
terminal/cancel state, stale-observation rejection by actual mutation API,
immutable reports, missing-file refusal, no changed table rows, in-flight states,
wavefront independence and exact tail metadata. No worker objects, networking,
Torch, tensor/model execution, real weights, GPU or node processes are used.

These checks do not demonstrate the full PR #16–18 execution chain, distributed
acceleration, numerical correctness, transport delivery or production readiness.
