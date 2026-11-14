# Central control/action host design and plan

Base 440acf4588d29d4b45f6e849707740f32c9e3e42; candidate unpublished.

One explicit private ControlActionHost owns two dedicated accepted TLS transports
for one original installed experiment stage. No provider discovery, new grant,
permission ledger, retry, automatic execution dispatch or quota release.

Both private-address listeners must be ready before accepts. Authenticate control
and action peers against the SAME original installation role node/certificate.
Local control node/certificate and original issuer node/certificate have distinct
pinned TLS material. Preflight current confirmed USER, original installation and
session, original task policy, same resource service and explicit host permission
before sockets, after waits and before existing protocol publication. No lock
spans network awaits. Max one accepted connection per purpose, bounded phase waits.

Then use existing receive_control and assemble_actions; derive latter's internal
config from fixed trusted host inputs plus ACTUAL authenticated connection
identities. Self-hashing that internal config is not authority. Role dial returns
and starts the existing role control loop BEFORE host finishes assemble_actions,
which needs real bound/input-ready proofs on that control channel.

Host exposes original CoordinatorControl and explicit serve_action_once (at most
two requests for the one-token start/commit plan); it never dispatches by itself.
Original installation/source/resource providers are borrowed. Host close retains
late accepts/TLS writers, cancels/settles owned tasks, closes only its own channels,
preserves original cleanup errors and UNKNOWN. No claim of hard physical stop.

Implementation plan:
- RED missing host in actual dial/authority/A→B→C chain.
- Share existing accepted-socket TLS helper with InstallationHost; keep old API.
- Implement strict bounded config, preflight, two endpoints and existing handlers.
- Events/fault tests: missing provider, config/pins, competing start, failed second
  accept, wrong peer, revoked original session, cancellation/late socket, failed
  close; independent tests for bootstrap and action proof waits.
- Rerun installation host, action assembly, role dial and C download regressions.

Only injected socket/TLS boundaries and memory streams in tests. No listener,
real credential, model, device or node is started; runtime providers remain LAB.

Implemented candidate: `private_control_action_host.ControlActionHost` consumes
an independently pinned canonical config and explicitly supplied trusted provider
objects. `await host.start(enabled=True)` is the only listen entry; construction
and the default start do not listen. Caller must retain the original installation
session, run the role control loop after `dial_control_ready` returns, then await
host readiness. `host.serve_action_once()` handles existing authority messages;
`host.control` is the original coordinator. This is library-level host assembly,
not a standalone deployment command or a configured real provider.

Independent review found and RED tests reproduced dictionary-order deadlock,
equal-value installation replacement (including provider-exit mutation), invalid
clock reaching listener preparation, and Channel's swallowed writer-close error.
The candidate fixes those locally: fixed accept order, original-object leaf
checks after provider exits, existing clock validation before listen, and explicit
observation of the owned writer's final close. Partial bind, late accept after
cancellation, competing host, missing provider, forbidden bind and cleanup failure
are tested without opening sockets. Original historical first-disconnect cause
remains uncertain; this candidate does not reclassify it.

Deployment still requires trusted issuer/user/node configuration, real task
confirmation, a single deployed authority, live persistent capacity/allocation,
model/device completion providers and supervised role processes. Method presence
is not provider trust. TLS snapshot loading is reused; this round does not prove
real handshakes, host isolation or certificate revocation. Quota is never refunded
by this host. Cleanup timeout/UNKNOWN retains responsibility; session loss, lease
expiry and task cancellation do not prove a physical GPU stop. No cross-node
atomic resource reservation is claimed; old execution bans remain unchanged.
