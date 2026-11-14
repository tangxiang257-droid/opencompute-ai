# C result endpoint host plan

Fixed base d0f5e53cd567e156888cde835d0fc9b97599a5d2. Private, default off.
The concrete missing seam is accepted B→C TLS streams for data, metadata and
C ACK gate, connecting to existing ReceivingLauncher.attach_result/attach_gate
and serve_result_once. Do not construct a Channel or send H on raw result data:
ResultIngress must reserve the original C artifact ledger before ObjectReceiver
creates its data Channel. Metadata/gate get their existing unstarted Channels.

The original C receiving launcher must already be ACTIVE through its independent
online pending/activate exchange, including actual committed B input proof. A
trusted pinned config binds launcher/spec/ReceiveBinding and local certificate.
Only the ReceiveBinding's exact B node/certificate may connect. All three private
listener slots precede accepts, fixed data/metadata/gate order, one accept each.
Original C identity, task permission and resource fence precedes socket work and
follows each await. No local fence spans network waits. All provider identities,
methods, original role/store/control connection and immutable input bindings are
pinned and locally rechecked after provider exits.

One permanent host-attempt mark on the original launcher, not a permission or
quota ledger. Before claim the launcher is borrowed; after claim a failed host
attempt retires the original receiving launcher. The preparing task participates
in the launcher's existing ownership set. Retain accept/TLS tasks and late
streams. Close failure stays UNKNOWN; original resources/artifact ledger are
never released or closed by the host. Cleanup cannot prove physical stop.

Tests inject socket accept and TLS handoff only, reuse actual control host,
ActionAuthority, A/B operations (synthetic backends), C receiving control, gate,
ResultIngress and durable artifact ledger. Existing default fixture hooks retain
prior behavior. No real sockets/listeners/models/devices, no credentials created.

Missing deployment prerequisites remain real issuer/user confirmation, node TLS
material and topology, original C authorization provider, current persistent
capacity commitments, actual model/device provider, independent process startup
and supervision. This host does not provide those or synthesize authority.

Implemented callable entry: `private_c_result_host.CResultHost(...)`, followed by
explicit `await host.start(enabled=True)` and `await host.serve_once(...,
enabled=True)`. Construction/default start do not listen. `start` binds original
ACTIVE C and returns READY with data still raw, metadata/gate still unstarted.
`serve_once` delegates the existing combined C ACK/result service and composes
host's fixed-binding checks with the caller's checks before and after them, so
the original per-frame/publication path observes drift before ACK/commit, not
merely after successful completion. Explicit host close retires its claimed C
launcher; no automatic result deletion or quota refund occurs.

Independent review found two meaningful windows and retained RED evidence:
provider exit after attach could replace role.spec before READY; and host-only
pin drift during the caller check was initially noticed only after the result.
The final candidate pins attached ingress/gate objects, raw/meta/gate transports,
original providers, TLS incarnation, actual input proof and derived bindings;
its checked callback is passed into the original result core. This defends these
specified callback/await boundaries, not arbitrary malicious in-process Python.

Data-host caller is the existing full control/authority/launcher chain's narrow
fixture hook. The default hook preserves previous behavior. Test backends and
identity/resource providers are synthetic, while journals/handlers/result files
are real repository implementations. No real TLS handshake or independent-host
interoperability was tested. A/B hidden transport hosting, B's three actual dial
connections, initial C control endpoint and executable process/config supervision
remain separate deployment work; this increment does not claim those complete.
