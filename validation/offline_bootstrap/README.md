# Offline public bootstrap candidates (private experiment)

Fixed source baseline: c9b3b70198f1b4547e4ca2c35cd8f0c68ee26768.
This is preparation, not a launcher or execution authorization. `assess` accepts
canonical public manifest bytes, independently pinned public trust-bundle bytes
and signed public evidence. It never writes a directory, constructs a provider,
claims a role, reserves resources or creates a network connection.

Missing or invalid material yields REFUSED, fixed diagnostic codes and no
candidates. Complete material yields CANDIDATE_ONLY with execution_allowed,
host_validated, physical_allocation_verified and revocation_checked all false.
Candidates reuse the existing LaunchConfig schema; target-host validation and
an independently approved config pin are still mandatory. The adjacent SHA is
a checksum, not a source of trust. The actual descriptor and policy must be
verified at runtime: this helper binds their supplied digest but does not parse
the descriptor, derive model memory requirements or prove declared demands.

## Operator workflow

1. A responsible operator must provision an independently trusted trust digest,
   scoped signer public keys (issuer, node and allocation purposes and subjects),
   and a pinned private-experiment CA certificate fingerprint. There is no default
   trust, TOFU, downloadable key, sample credential or provider factory.
2. Supply the public manifest: experiment/task/owner, plan/model digests, bounded
   expiration, issuer public identity, coordinator, exact node certificates and
   A/B/C placement, descriptor digests, target directories, charges and demands.
   A/B require distinct stable device IDs and C is storage-only. B's actual input
   remains determined by committed A output, not invented here.
3. Authorized signers attest the entire manifest SHA under the new fixed
   offline-bootstrap-evidence/v1 domain and audience. Every assertion has at most
   300 seconds lifetime. Key scopes restrict both purpose and subject. Required
   claims are issuer configuration, each node/certificate binding, and each role's
   resource/device/instance, capacity, lease/proof identifier and isolation claim.
   Changing any manifest byte requires fresh signatures. Identity credentials
   cannot be replayed as these evidence statements.
4. Call assess; resolve missing/invalid codes. Even success is only a candidate.
   Current task confirmation, node identity/revocation, live platform allocation
   and exclusivity, descriptor/policy, host paths and device completion remain
   mandatory through existing runtime authorities. Never install fake providers
   or treat submitted input, a lease string or exclusive-device text as proof.

Only public certificate DER, public keys, signatures and identifiers belong in
inputs. Unknown fields are rejected; errors do not echo inputs. The helper has
no signing, key-generation or private-key serialization operation. Public input
should still be reviewed before sharing. No production private key is required
for the synthetic tests: ephemeral LAB keys exist in RAM only and are discarded.

## Certificate and time boundary

This is a restricted direct-CA profile, NOT general PKIX/TLS validation. It checks
pinned CA DER, direct signatures, validity, allowed strong key/hash types, basic
constraints/key usage, exact DNS SAN and both client/server EKU for leaves. CA
EKU is rejected. Unsupported extensions are rejected. All certificate expirations
bound valid_until and a final time check rejects observed clock rollback or expiry.
A wrong clock before invocation, unobserved rollback, CRL/OCSP, remote revocation,
proof of private-key possession and a real authenticated handshake are not verified.
`verify_directly_issued_by` alone only checks name/signature; see the official
[cryptography X.509 reference](https://cryptography.io/en/latest/x509/reference/).
The isolated test environment uses cryptography 50.0.0; deployment version and
platform behavior remain to be validated separately.

No new persistent state machine is added. Repeated verification creates no grant,
so replay is inert; it does not solve replay/rollback of runtime databases, restore
consumed qualification or release unknown allocations. Assertions do not prove
physical device exclusivity, actual free capacity or GPU completion. No IDC AI
scheduler, exclusivity facility or profiler capability is assumed. No distributed
atomic resource reservation is claimed. Existing execution bans and byte limits
are unchanged. Runtime locks and networking are not involved in this pure helper.

Tests use public LAB certificate/evidence material, temporary local paths and
ordinary JSON. They cover scope/domain/manifest substitution, malformed input,
missing claims, capacity/placement/isolation refusal and host-validation separation.
They are not real issuer authorization, device or network acceptance evidence.
