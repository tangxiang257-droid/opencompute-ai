# Narrow owner representation compatibility

Base097181b362b1c5482fc54706511e3c5d63badf9b. Only Spec.owner and Binding.owner
use the new shared predicate: exact str containing the legacy ASCII1..64 grammar
OR exactly usr_ plus64 lowercase hex. All other fields and all size/charge limits
keep their previous validators. No normalization, truncation, prefix removal,
case folding or rehashing. This syntax does not authenticate anyone.

Owner generation, serialization, digests, keys, schema, statuses, source validation,
USER/execute/resource/device authority and v12 no-plan gates are unchanged. Existing
legacy64 values including usr_ plus60 uppercase chars retain their original bytes.
No database migration or rewrite occurs. Book retains its preexisting reopen and
idempotent reserve behavior; neither restores consumed/isolated qualification.
Projection Journal remains fresh-only. Failed/unknown work is not retried.

Tests cover generated canonical owner, legacy golden digests, invalid/confusable
forms and str subclasses, non-owner bounds, real Book record/charge retention,
exact identity in control codecs, cross-owner rejection and default Driver denial.
Mixed-version test uses the old grammar as a rejection model, not a real peer.
The separate delivered before/after script creates actual temporary Book/Journal
records using the fixed old source then opens them with the new source and checks
logical dump, serialized plan, digest, charge and no second consumption.

Deploy readers before permitting canonical-owner work; unsupported participants
must reject before admission, never shorten owner as a fallback. Rollback retains
records/charges, requires compatible history readers, and never requalifies work.
Each owner may add4 bytes relative to the old maximum; every existing total JSON,
frame/manifest and budget check remains mandatory. Do not raise limits implicitly.

This fixes representation only. v12 still forbids execution plans and real identity,
execution-authority, resources and device-completion providers remain necessary.
Only plain bytes/temporarySQLite are used: no model/GPU/worker/node/network.
