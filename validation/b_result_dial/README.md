# B result dial and PENDING C raw preparation

Fixed base 298dcf81e5179fb12a98f653b0df56eb9db99303. Candidate unpublished.

Actual dependency cycle: B spool fixes its TLS connection generation before
StageBOperation construction; the original B control proof needs that operation;
C ACTIVE needs that proof; the previous C listener required ACTIVE. No fake
spool, placeholder generation, copied authority or bypassed proof resolves it.

New concrete sequence:
1. Original independent C receiving registration is PENDING. Explicit
   CResultHost.prepare checks original identity, pending permission and resource
   provider, then accepts three dedicated raw TLS streams. No Channel, H, result
   ingress, result bytes or ACK is created. Original C control remains live.
2. Installed B with actual committed A bytes calls BResultDial.prepare. This uses
   original installation admission/resources and the same extracted committed-B
   input verifier used by StageBOperation. All waits are outside local fences.
3. BResultDial.generation uses existing _generation on actual raw data TLS, so
   the original Spool plan and operation can be created without inventing a
   connection identity. This is not an execution permission.
4. Original control-bound/input-ready proof activates C. C.attach_ready checks
   that original actual proof matches fixed spec, then attaches existing ingress
   and gate. B.attach_ready constructs the original sender/gate on the same TLS.
5. Existing C ACK gates B dispatch; original sender/result core persist results.
   Per-frame check composition retains original export and host/provider pins.

Both CONNECTED raw phases have explicit finite prepare_seconds (0..30 seconds,
exclusive lower bound); fixture uses30. These are experiment limits, not measured
latency guarantees. Expiry closes/isolates without restoring qualification. After
attachment existing Channel deadlines still apply; this does not prove model
loading/inference can finish within them. No keepalive or automatic retry added.

Original descriptor/control connection/challenge/recipient stay fixed. Only
PENDING actual=None to ACTIVE actual proof is permitted, then proof is frozen.
READY/CONNECTED are not C ACKs. UNKNOWN/revoked records refuse. Each launcher owns
one result transport cleanup callback; it closes only owned streams, never joins
its own launcher. Preparing tasks remain owned until late dial outcomes settle
or remain explicitly UNKNOWN. Quotas and books remain charged; no refunds.

Old CResultHost.start ACTIVE behavior remains; former stage B byte validation is
extracted unchanged to committed_b_input and reused. No new permission or result
ledger. No changes to legacy execution bans. Trusted bootstrap must still supply
real identity, task permission, capacity/allocation and device providers. The
presence of a method or a synthetically injected provider is not proof of trust.

Tests use injected open_connection/accept/TLS and synthetic backends; original
control authorities, SQLite journals, spool, C ACK and artifact publication run.
No real listeners, nodes, models, GPU, credentials or devices are created.
A→B hidden transport hosting, initial C control host/dial, process bootstrap and
supervision, real certificate/provider configuration remain deployment gaps.

Review-driven corrections and evidence boundaries:
- B/C attach_ready initially rejoined external cleanup that waited on the same
  attach task. Both event-based RED cases now converge without that cycle.
- Original issuance registry is required before dial, not merely valid bytes.
  Reuse _require_issued_journal; pin original store, handle and pending intent.
  Full intent contains boot/store/command/challenge/issuer_peer beyond descriptor:
  compare its explicit _DESCRIPTOR projection and preserve the entire raw intent.
  An initial whole-intent==descriptor check wrongly rejected valid requests;
  this development regression and its failed tests are retained in evidence.
- A proposed provider-exit injection did not actually execute; it is not counted
  as coverage. The retained tests instead prove missing issuance before dial and
  removal after successful qualification rejected by the final local check.
- Pre-operation dial failure originally retained a HELD promise. It now invokes
  the original captured resource quarantine callback before network cleanup can
  block; the promise becomes UNKNOWN and remains charged. This is not a device
  completion callback. Callback failure remains a cleanup failure.
- Same sender object, raw streams and launcher-owned cleanup remain fixed. All
  final testing must use the frozen source; earlier runs are development evidence.
