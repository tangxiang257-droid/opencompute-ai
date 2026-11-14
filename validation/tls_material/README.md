# Private TLS material snapshots — no host enablement

Base 29c7f35. `role_dial.dial_stage` now requires `material` canonical JSON bytes
and an independently supplied `material_pin` SHA256. It uses the actual context
returned by `tls_material.load_context`, never TLSConfig.context's source-path
reopening. Other existing Channel entry points are unchanged. This is an
intentional stricter private dial interface; absent material does not fall back.

Schema version 1 exact fields: version, directory, certificate, private_key, ca,
certificate_sha256, ca_sha256, leaf_sha256. Directory is an absolute local path;
file names are distinct basenames. TLSConfig's three paths must match exactly.
Certificate and CA hashes pin exact PEM/bundle bytes; leaf_sha256 pins first
certificate DER, not SPKI. Pins and TLS peer/hostname configuration must come
from trusted bootstrap, never from a remote self-authenticating manifest.

The Linux-only loader traverses directory FDs with O_NOFOLLOW, requires final
directory owned by effective UID and mode 0700, reads each owned regular file
with mode 0600 and nlink 1, and rejects symlinks, hardlinks, special files,
changes during reading or files outside 1..131072 bytes. Paths/depth/JSON are
bounded. Source FD bytes are then copied into CLOEXEC memfds and WRITE/GROW/
SHRINK/SEAL seals are applied and queried. Linux UAPI fcntl operations 1033/1034
and mask 0x000f are used because this interpreter omits exported constants.
Unsupported memfd/seals/proc behavior rejects; no unsealed/path fallback exists.

OpenSSL loads the sealed certificate, key and CA snapshots, with all FDs alive.
It verifies key/certificate pairing; encrypted keys are refused without prompt.
The loader checks the leaf DER pin, validity and CLIENT_AUTH EKU/non-CA profile.
SSLContext is client/hostname validating, CERT_REQUIRED, TLS >=1.2; environment
SSLKEYLOGFILE is not used. The actual loaded CA DER set must equal the pinned
bundle's set, so injected/default roots cannot silently widen it. This is a
certificate store check, not a proof that an issuer is authorized or that every
chain will validate. Peer chain/hostname/DER pin checks remain at handshake and
Channel admission. No real handshake is tested here.

All owned FDs are closed before return, even after another close fails. Any
failure refuses context return with a fixed error and no underlying exception
chain. A close syscall reporting uncertain completion is not retried against a
possibly reused FD; caller must isolate on cleanup failure. Python/OpenSSL key
memory is not reliably erased, and same-UID/root memory/FD access is out of
scope. Disk calls are byte bounded, not guaranteed time bounded. Wall-clock
expiry and rollback are checked again after loading/cleanup; this does not
provide a trusted clock or continuous certificate revocation/rotation. Rotation
requires new explicit material and a new authorized connection.

Tests create ephemeral LAB keys/certificates in a temporary private directory,
never save them in evidence, use real local OpenSSL loading and actual Linux
seals, and use Event barriers for source replacement. The managed Python adds
a public rejection-policy certificate to every context; the LAB bundle explicitly
pins that public entry. A separate test rejects when it is absent from the pin.
The runtime policy is not bypassed or cleared. Production trust choices remain
external; do not copy this generated LAB configuration to a deployment.

The full A/B/C test substitutes only dial streams and external LAB providers;
it supplies real temporary certificate material to the actual loader and checks
that the actual SSLContext reaches open_connection. Existing host_dial tests
explicitly stub this loader boundary to retain their stream ownership/cancel
fault focus. They do not claim real identity, TLS negotiation or GPU completion.

Missing identity/permission/capacity/completion still rejects before dialing.
No listener, new provider, credential issuer, model or actual execution is added;
thin_host.run_once, v12/v13 bans and UNKNOWN non-retry/quota retention are unchanged.

References: Python ssl SSLContext/load_cert_chain/load_verify_locations and os
memfd_create documentation; Linux UAPI linux/fcntl.h seals. Tested runtime:
Python 3.12.14, OpenSSL 3.5.8, cryptography 50.0.0. This is mechanism evidence,
not validation of an actual issuer, platform allocation or deployed host.
