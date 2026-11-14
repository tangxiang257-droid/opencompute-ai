# Private original-installation session lifecycle

Base: published 3c7cee202c7ef5e694a0c2936dcf8119f05784c9 plus unpublished
control-bootstrap patch 38f0a92f1855b265559df6c71eda7c18c8a492864c1227461b160fdc4f664418.
This increment is isolated, uncommitted and unpublished. Historical first-error
attribution remains uncertain; the separately traced idle-installation mechanism
was reproduced in ordinary concurrent tests.

## Explicit composition
Trusted callers pass the same `SessionPolicy` to `StageLauncher.install` and the
original `IssuanceService.serve_once`. RoleStartup passes it to IssuanceClient.
Default None preserves legacy behavior; it does not silently opt old endpoints in.
No real host/execute route is enabled. The actual launcher/online installation
returns only after an opt-in OPEN/OPENED exchange, before constructing the role
journal/operation. The accepted installation slot is already permanent: mismatch,
lost reply or cancellation burns eligibility and closes the original connection.
No external shared Python endpoint/ledger is passed across the protocol.

The protocol uses existing bounded D Envelopes on the ORIGINAL authenticated
installation Channel, sequence 2 for OPEN/OPENED and monotonically increasing
sequences from 3 for PING/PONG. Exact canonical schema includes version, purpose,
full descriptor hash, exact locally selected policy and fresh echoed random
challenge. Each pulse body is at most 1024 bytes. There is one owned receiving
task per endpoint, no second H, borrowed credit frame, fresh connection, new
reservation or parallel permission ledger. Existing provider, identity and
resource fences cover actual writes and validation, with no local lock over await.

## Three independent limits
- Generic frame deadlines remain unchanged: default first byte and remaining
  header share the original deadline. For an opted-in active session only, empty
  frame-boundary waiting is governed by session liveness; after any first byte,
  the remaining header and body still have bounded frame-progress timeouts.
- SessionPolicy defaults to interval 2 / silence 10 seconds, bounded local
  experimental choices, NOT measurements or execution lease values. Matching
  peer policy is required. Only validated challenge/response updates freshness;
  late response, observed monotonic rollback, wrong sequence/schema, or missed
  response permanently closes the session. Sync current/identity checks reject
  expired freshness even before a delayed watchdog can run. Reader activation
  retains its pending first-byte task; it never drops partially read bytes or
  silently resets an already expired legacy deadline.
- Existing task consent, revocation, resource and 5/4/1 short-action checks remain
  mandatory and unchanged. Heartbeats grant no execution, reinstall, replay,
  recovery, expiry extension or quota refund. A valid transport cannot override
  revoked/expired authority. Synchronous provider stalls can still cause safe
  failure; heartbeats do not establish a scheduling guarantee.

OPENING internal transport checks use a narrow synchronous context plus current
Task identity. Inherited context in another Task cannot supply this exception;
live_guard explicitly rejects non-ACTIVE state even inside it. This is trusted
Python composition, not protection against hostile same-process introspection.

## Terminal close / unknown
CLOSE is explicitly terminal transport closure, not a resumable handshake or
resource-release ACK. Endpoint close retires the session, cancels and joins its
owned monitor and reader, retaining cleanup failures. Repeated caller cancellation
must not abandon the join. Writer.close failures are aggregated after cancellation
and joining; they are not successful physical transport/device shutdown. Role
watchers still seal the owner on original connection death. UNKNOWN obligations,
slots and resources remain; reconnect, another nonce or a fresh ledger do not
restore eligibility. Close/expiry says nothing about physical GPU completion.

## Evidence boundaries and tests
Tests invoke actual SQLite/Channel/installation/launchers/ActionAuthority and the
A->B->C result path, using explicit LAB identity, policy/capacity/clock/device and
memory-stream fixtures. They do not authenticate real external accounts, reserve
real devices, run models/GPUs/nodes, connect/listen on sockets, or attest deployment.
Default real thin_host execution and v12/v13 prohibitions remain unchanged.

Independent REDs found rollback acceptance, writer-close failure abandoning the
monitor, boolean version acceptance and cancellation before the monitor first
entered its body leaving a partial-header reader alive. Targeted fixes preserve
their logs. An owned task done callback closes/cancels the original reader even
when the monitor body never runs; endpoint close still joins owned work.
A further actual RED showed the generic Channel.receive deadline still terminated
session idle waiting; owned session receive now uses its separate freshness
budget while generic receive remains unchanged. Natural idle is tested with
observed pulse events beyond the original ten-second boundary. Event barriers
order faults; timers test explicit deadlines, never sleep-based race guesses.
Tests preserve old rows and verify no new B execution on rejection. Source and
destination reservations remain separate, not distributed atomic commitments.

Other long-lived action/control/hidden/gate/result channels are not converted to
this installation protocol. Their silence budgets, real provider costs, TLS
configuration, clock/resource authority, process supervision and actual device
completion remain deployment prerequisites. This fix is not general runtime
stability or a real cross-machine GPU acceptance claim.
