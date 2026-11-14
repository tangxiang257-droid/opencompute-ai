# Private experiment review -> explicit confirmation -> one registration

Baseline ea624e7ccb85b7743aa848a5f2fe373882d4712b. This is an unmounted ASGI
application, not a listener, new login system, execution permission service or
C input importer. It adds no default/test provider to the real host.

Trusted bootstrap constructs ExperimentConfirmationApp with an existing real
TaskConsent/source reader, pinned source domain, trusted HTTPS origin, genuine
USER-session credential resolver and policy authority. The app constructs a fresh
ExperimentAdmission with its own purpose-specific POST trigger. The coordinator
then calls prepare with the actual fixed issuer and complete proposed context.
No HTTP header supplies owner/credentials; authenticated ASGI host/session mapping
is mandatory. Test resolvers and LAB signing keys prove only the mechanism.

GET /experiments/confirm?id=...&digest=... authenticates the user, shows the full
immutable context (old source, new task, input/model/plan, cost/privacy, roles/C
retention, issuer) and explains metadata-only/unverified source status. It creates
no confirmation or registration. For writable PENDING only it presents an
UNSELECTED required checkbox authorizing the exact input snapshot copy to the
named C destination for result-store. C permission is neither inferred from
submission nor hidden in a generic confirm button.

POST requires exact Host/Origin/content type, bounded unique form fields and a
60-second challenge bound to owner, credential fingerprint, proposal and digest.
App/admission/issuer dependencies remain pinned; asynchronous resolver/body waits
are followed by pin checks. Observed substitution seals even after restoration.
The challenge is consumed before attempting the action and never restored.
Only explicit confirm-register with the exact copy value can call the NEW
ExperimentAdmission.confirm, then its pinned issuer dispatch. The one-shot trigger
has no standalone request route and is cleaned in finally. Cancel uses a separate
form without the copy checkbox and calls the existing fresh revoke/tombstone.

CONFIRMED is not an invitation to retry dispatch through a new page. REGISTERED,
DISPATCHING and UNKNOWN never show a confirmation button. Response loss after
registration yields only history/revoke; no automatic resend or new nonce.
Read-only reopened/retired admission shows history with NO mutation forms. A
cancelled request after starting confirmation seals pending live qualification;
a cancelled GET/body read before mutation does not create authorization. Already
sent data or committed records cannot be recalled by cancellation.

The existing public host still refuses all execution. Admission's hold permits
registration only and rejects claim/install/execute. This UI does not mint a
portable permission, copy input to C or authorize an unseen changed context.
Old v12/v13 plan/execute bans and existing quotas are unchanged. Future runtime
permission/C-import wiring must validate current explicit scope and all providers;
these historical registration receipts cannot substitute for it.

Remaining work is concrete: deploy/configure real HTTPS login and policy sources;
implement reviewed current remote role permissions with bounded revocation and
once-only control consumption; implement legal C local task/input import bound to
new execution identity and the confirmed C privacy terms; assemble actual capacity
and device-completion providers and role processes. None is claimed done here.
No real issuer credentials/private keys, model, GPU, real node or listener are used
in tests. Tests directly call ASGI using ordinary messages, actual source/admission/
issuer SQLite and LAB signature verification; policy/login mapping remain synthetic.
