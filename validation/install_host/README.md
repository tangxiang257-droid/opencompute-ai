# Private one-stage installation host (candidate)

Fixed baseline: 8b0d936925c2282133dee8b82201c897f18af5f9. No CLI is enabled.
`InstallationHost.start()` refuses by default; an explicitly configured trusted
caller must supply `enabled=True`. Construction alone does not open a socket.
No dynamic imports, provider discovery, credentials, execution authority, new
permission ledger, automatic retry or quota release are provided here.

## Actual connection to existing code

The central host requires the existing exact `ExperimentAdmission`, a current
user credential, a REGISTERED explicitly confirmed experiment, the same original
issuer/resources, installation authority, authenticated-channel identity provider,
and purpose-specific authorization provider. It rechecks original consent and
resource fences before bind, after accept, and after handshake. No local fence
is held over network waits.

The path is:
`InstallationHost.start(enabled=True)` → original admission preflight → sealed
server TLS snapshot → one accept → close listener → one TLS handshake → existing
`_accept_unstarted` → `admission.installation_service` → original
`IssuanceService.serve_once(SessionPolicy())`. The original stage consumption,
challenge, identity checks and installation session remain unchanged. There is
one Channel and one original H. ACTIVE retains the original session until close.
The resulting admission installation records are consumed by the existing
control bootstrap, ActionAuthority/ActionServer, A→B→C persistence path.

The full-chain test uses this host for both original A/B installation endpoints,
then runs those existing components. It injects socket accept/TLS transport and
uses LAB policy/device/compute providers. It does not prove real TLS, remote
identity, physical capacity, actual model output, or production readiness.

## Trusted configuration

Canonical JSON bytes and independent SHA256 pin bind version=1,
purpose=one-installation-host, experiment/context, stage 0/1, bind/port,
issuer_node/issuer_certificate (leaf DER SHA256), server_hostname,
role_node/role_certificate, TLS material SHA256, and accept/handshake/close seconds.
The TLS material uses the existing private-directory sealed-snapshot schema.
Only explicit loopback, RFC1918 or IPv6 ULA bind addresses are allowed; no DNS,
wildcard, public or link-local bind. Port range is 1024–65535. Accept timeout is
at most 30 seconds, handshake/close at most 10. These are bounds for this limited
experiment, not measured performance promises. Exact single-token Grant scope
must match the original descriptor before any socket is created.

Server TLS uses CERT_REQUIRED, TLS >=1.2, no implicit CA additions or key log,
SERVER_AUTH leaf EKU and exact independently configured DNS SAN. Local certificate
DER must match issuer pin. Client-side loader behavior stays CLIENT_AUTH plus
hostname checks. A CA chain alone never grants node/task permission: original
Channel peer DER/node checks and installation authority remain mandatory.

## Ownership and failure boundaries

One host accepts one TCP connection and closes its listener before TLS. One stage
is permanently occupied in the same live admission instance; no reopening on
error. This in-memory occupancy is not a global/distributed replay ledger. The
original durable issuance/boot policies still govern stage qualification.
Backlog=1 limits this listener's queued-connection request, not OS-wide DoS/RSS.
An unauthenticated first connection can consume the attempt (availability loss).

Accepted sockets transfer to the asyncio transport. Before handoff, the host
closes the socket; after handoff it closes/joins its observed writer/transport.
An unobserved handoff remains UNKNOWN and requires external isolation. A retained
cleanup task captures late accept/handshake outputs. Cleanup deadline/cancellation
never declares physical stop, restarts the task or refunds resources. Close and
wait_closed errors are both observed; failed close does not suppress wait_closed.
Timeout UNKNOWN remains UNKNOWN even if later cleanup settles. Borrowed authority,
issuer, stores and resources are not closed or released by this host.

## Explicit remaining work before any real experiment

This implements the central installation listening seam only. It does not supply
listeners/routing for control, action leases, A→B bytes, B→C result/receipt/history,
a multi-process supervisor or deployment CLI. Those channels remain purpose-
separated; the existing APIs are not automatically multiplexed onto this port.
Real issuer/node certificates, local trust pins, user confirmation/permissions,
current resource allocation and device completion providers are still required.
The v12/v13 and thin-host execution bans remain unchanged. A trusted caller must
account for kernel/TLS/transport buffers in its actual host allocation; payload
bounds alone do not prove peak memory or device exclusivity.

No listener, real credentials, node, model or device was run for this candidate.
Original historical first-disconnect root cause remains uncertain. Test cleanup
success is not a production cleanup or GPU-stopped guarantee.
