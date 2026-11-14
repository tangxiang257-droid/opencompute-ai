# Pinned-key signatures into existing LocalAdmission

Isolated CPU experiment at base3938a6dfb1568d6ecad0044dee53a45c74e6fd3b.
Explicit IdentityAdapter with REAL Ed25519 verification; not an execution grant,
configured IdP, remote revocation feed or X.509 chain. Tests generate ephemeral
LAB keys in memory; no private key is saved. Correct signatures prove possession
of a key, not that its issuer is actually authorized by an organization or user.

Bootstrap independently pins the raw32-byte public key, issuer and audience.
No default key, environment discovery, token-supplied key/URL/algorithm or
listener. Optional requirements-identity.txt pins cryptography50.0.0, the installed
version used here; no package installation. No adapter still means deny.

Custom version1 envelope, NOT JWT: canonical unpadded base64url(payload), dot,
canonical base64url(64-byte signature). Signature input is
`opencompute/local-identity/v1` plus NUL plus exact payload bytes. Payload is
UTF-8 canonical compact sorted-key JSON with exactly v,typ,iss,aud,sub,sid,iat,
nbf,exp. v=1, typ=local-identity, configured issuer/audience. Times are exact
integers, not bool, ordered iat<=nbf<=now<exp, lifetime<=300seconds. Credential
<=4096chars, payload<=2048bytes. Duplicate/unknown fields, noncanonical encoding,
JSON, padding or extra segments reject. No credential details in errors/logs.
Trusted wall-clock and external issuer provisioning remain prerequisites.

Adapter returns Observation ONLY. Existing LocalAdmission owns caching, locks,
freshness, durable revoke_subject/revoke_session events, generations, unknown-write
poisoning and history-only reopen. No new state machine/table. Local revocation
invalidates cached observations; remote issuer revocation is not automatically
received. Key/issuer rotation must retire the old admission owner via its existing
lifecycle, not patch keys while retaining cached qualifications.

Tests use real cryptography and SQLite persistence. Events establish revoke races.
Injected response loss AFTER actual commit checks poison and recorded event,
not power-loss durability. Source-bridge tests feed signed LAB credentials through
actual LocalAdmission/noncompute facade and source ledger; results remain inert.
Task cancellation independently blocks the bridge even while identity stays valid.
v12 execution remains denied.

LocalAdmission requires a trusted private parent directory. Tests use temporary
private directories, not hostile filesystem races, global database-clone/rollback
protection, key custody, remote IdP or production issuer acceptance. No model,
Torch/GPU, node, listener or real model forward is executed.

Official API reference:
https://cryptography.io/en/latest/hazmat/primitives/asymmetric/ed25519/
The latest docs displayed51.0.0-dev1; actual tested package is50.0.0.
