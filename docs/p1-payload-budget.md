# Bounded payload planning increment

Base: PR #21 `077445d5f77af37357262ee62bf5f671ddb271d4`.

The supported final output declaration is `full-logits-f32-le-c-batch1-v1`:
all vocabulary logits at every declared token, batch one, FP32 little-endian,
C-contiguous bytes shaped `[1, token_count, vocab_size]`, including the exact
last tile. There is no top-k, sampling, truncation, precision change or implicit
switch to another transport. This metadata report does not verify actual bytes. V2 handoff planning requires
wire layout version >=2, matching the existing receipt path.

`assess_payload_budget` revalidates the complete two-stage plan and computes:

* Every input/output object: dtype width times declared shape product.
* Local retained total: all stage-zero inputs/outputs and stage-one input copies
  and outputs, across the whole plan (no assumed reclamation).
* Hidden handoff frame: Envelope v1 prefix + canonical JSON metadata + full
  hidden bytes + SHA-256 digest. Identity lengths and token ranges count.
* Conservative receiver retained total: all complete incoming hidden frames,
  stage-one input copies and full logits outputs.
* Result transport diagnostics: 32 KiB chunks; at most 2 MiB/64 chunks per
  object, and 2 MiB/127 chunks across the result set. Framing overhead is 48
  bytes per chunk; existing channel frame capacity remains 32768+48. No chunk
  path is selected or enabled by the report.

Local preflight keeps the existing 65536-byte object and 262144-byte cumulative
ceilings. Optional overrides may only tighten them. The new explicit
`preflight_handoff_budget(wire_layout_version=...)` additionally checks each
complete hidden frame against 65536 and the receiver total against 262144.
This conservative planning total is not a new receiver allocation or credit
implementation. It is not automatically wired into a real remote producer.
Both journal preflights inspect a live authorized plan in one SQLite transaction;
no attempts, credits, session qualifications or execution permission are issued.
A returned report is a snapshot, never a reusable admission capability. Actual
admission/commit still require the existing identity, epoch, authorization,
reservation and source checks; cancellation after preflight can invalidate it.

For hidden=896 and vocab=151936, even one token requires 607744 logits bytes,
so the local path rejects it despite the result transport diagnostic fitting.
Three tokens total 1823232 logits bytes; four total 2430976, exceeding that
transport's result-set ceiling. No real model/GPU readiness follows from this
increment. Loading, cache memory, GPU synchronization, remote authentication,
physical cancellation, atomic cross-ledger commit and throughput remain outside
this byte-planning change. Journal terminal state does not prove device stop.

Verification uses an explicit allowlist runner with forbidden model imports,
connect/bind and subprocess runtime actions blocked. Tests use declarations,
ordinary bytes, temporary SQLite and event barriers only. No model/Worker/node,
network listener, production changes or publishing are required.
