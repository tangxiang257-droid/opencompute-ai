# Installation identity construction after the one authenticated Channel

Fixed base c0b5903. Explicit `identity_mode='channel', identity=None` is passed
through existing role_dial -> StageLauncher.create -> RoleStartup.create. Every
layer rejects unknown modes and conflicting providers; the default still needs
an external identity. All store identity/authorization/resource/device gates,
task authority, runtime and completion providers remain mandatory.

After the existing _accept_unstarted creates the only Channel (reader, no H),
RoleStartup builds a private _InstallationIdentity. It compares actual peer
node/certificate to the explicitly configured issuer pins and captures the
actual generation. Its only interface is hold(actual_identity) yielding the
fixed issuer node principal. It checks the same Channel/method/peer before and
after yield and re-enters the actual identity fence on exit to detect same-loop
revocation. Errors retire the view, not the borrowed Channel; existing startup
ownership closes it on failed construction/install. No late-bind holder, new
reader, new permission ledger or certificate issuer is introduced.

RoleStartup stores the exact newly created view before IssuanceClient creation,
requires client.identity to be that object, and rechecks it later. StageLauncher
captures that same object once on successful creation rather than refreshing its
provider pin from a potentially replaced client. The view is not task/node-policy
authorization, physical resource proof, global revocation or recovery permission.

Independent fault injection found object-only pinning insufficient: replacing
the generated view's hold method could return a wrong principal through the
actual client. The existing issuance endpoint now pins the exact identity object
and callback, invokes that saved callback, and checks before/after context and
authorization callbacks. Observed method drift permanently retires that endpoint,
even after restoration. Startup checks this same endpoint readiness, including
direct client usage, rather than trusting a replaceable view to self-check.

Tests run actual dial/startup/installation/control/action/C-result components
over memory streams with fabricated TLS peer metadata. The new chain selects
the concrete identity only on A/B installation clients; issuer service, store
policies, resources and backend remain explicitly LAB. It stubs material loading
here; the existing tls_material regression separately tests real local snapshot
loading. No actual socket/handshake/node/model/device is tested or enabled.

No real issuer is configured by this change. Repository source has TLS material
loading/verification and SignedIdentityAdapter for USER credentials, not a
deployed node CA/enrollment service. Website Caddy TLS is not node mTLS identity;
older shared Join Key is not an acceptable substitute. A minimal experiment can
use one operator-controlled offline experiment CA with separate issuer/A/B/C
leaf certificates and independent node/DER/hostname pins, deployed out-of-band
by the responsible operator. This trades manual short-lived provisioning and
explicit rotation for avoiding a new online CA service; it does not provide
instant global certificate revocation. No keys are generated here.

Real execution still requires the actual CA/key custodian and trust roots,
hostname/endpoints and role bindings, user credential issuer/public key and HTTPS
session mapping plus explicit confirmation, node/task policy and live platform
allocation/capacity/completion evidence. Their absence continues to deny. Do not
copy LAB grants or SQLite files into production to fill these gaps. Public
thin_host, v12/v13 bans and UNKNOWN non-retry/resource retention remain unchanged.
