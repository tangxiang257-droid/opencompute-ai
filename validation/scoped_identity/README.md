# Original-channel identity in central action assembly

Base 38ca55f. `assemble_actions(..., identity_mode='channel', identity=None)`
explicitly constructs ScopedChannelIdentity after the existing independently
pinned configuration and original installation/control/resource binding pass.
It uses the actual control descriptor's complete command/reservation/charge/
binding and the configured action_peer. Neither an observed peer nor a newly
hashed self-description fills in missing configuration. Default mode still
requires an explicit identity provider; incompatible/unknown modes refuse.

The concrete view requires the actual original Channel, its exact observed
node/certificate DER fingerprint/connection generation, and the fixed canonical
scope. Both entry and exit check original object/method pins and Channel's live
identity fence. Exit rechecks the fence because same-loop RLock reentry can
revoke the connection during a synchronous body. Any failure retires the view;
it does not close its borrowed Channel or create a replacement qualification.
The existing assembly owner retains channel cleanup responsibility. No local
fence is newly held across network waits.

This view only implements hold(scope, actual_identity) yielding None. It is not
the principal-returning issuance identity interface or a generic *args provider.
It establishes current original-connection identity, not global node revocation,
task consent, device allocation or a transferable permission. Copying its peer
JSON does not manufacture an actual Channel or restore its original generation.
The original ActionAuthority/ControlAuthority still check task consent/current
resources/clock and consume their existing durable action records. Identity
construction neither installs a role nor starts a body. The retained callback
pins must be captured before invoking the external clock, not after it.

Tests use real Channel fences and existing configured ActionServer/A-B-C/result
components over memory TLS metadata and synthetic external providers. A full
chain replaces only the central ActionServer's previous Holds identity with the
concrete view. It does not claim real TLS negotiation or all-role identity
assembly. Scope/peer/generation mismatch, yield-time revoke and body errors
retire; task revoke and capacity denial still produce UNKNOWN with zero body
calls. Negative tests observe expected business failures using the existing
settle_expected_role_failure helper before fixture teardown; no production
cleanup implementation was changed or asserted physically complete.

Still missing: genuine credential/CA configuration and node authorization policy,
HTTPS user identity and explicit confirmation, live platform allocation and
capacity provider, trusted clock and device-completion/backend validation. In
particular StageLauncher/RoleStartup accept identity before creating their
Channel; this increment does not add a mutable late-bind factory to bypass that
ordering. C receiving and issuance principal identities are unchanged. No
listener, actual model/device, new permission ledger, public host enablement or
v12/v13/UNKNOWN policy change is included.
