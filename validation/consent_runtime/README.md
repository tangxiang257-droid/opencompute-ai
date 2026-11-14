# Local consent-aware issuance and runtime checks (private)

Base e08e96d7c03f074571b359249d52ec3e8023e736. No real execution or listener.

Strict IssuanceAuthority(require_consent=True) explicitly creates version 2 and
stores the full immutable authorization context in the SAME registration
transaction. Version 1 retains its old schema and semantics; no migration or
fallback. Version 2 without context refuses. Context includes the confirmed
source, full declaration/model/config, price/budget/privacy revisions, expiry,
actions, placements, proposal digest/id and authority incarnation.

RegisteredInputSource(consent=IssuanceConsent(...)) checks actual stored source
under its existing A/USER ordering. The adapter is an AND with an explicit parent
permission provider, never a replacement. The registration's durable context must
match on every registration lookup. Claim supplies that same context plus actual
intent and exact placement; IssuanceService calls claim_online outside any held
network fence, then claims synchronously under A/USER/admission. Unknown outer
exit retires both adapter and issuer, retaining the consumed slot and resource.

Runtime views are private SAME-PROCESS objects minted after registration. They
require a separate parent runtime authority and maintenance provider. They match
fixed spec/stage/token and exact control binding, call the parent permission,
then also check current local USER, consent/expiry/provider identity and shared
revocation fence. No SQLite access from runtime worker threads. No gate/USER lock
spans body or network waits. Finalize is stage1-only, one-shot; only its owned
synchronous commit callback holds the short fence. Parent exceptions cannot be
swallowed into success; no foreign-thread/late/repeated commit callback is valid.

TaskConsent revoke, Authority cancel/poison/close invalidate that local fence.
Unknown revoke already seals all consent handles on the same Authority. Query
and fail use exact bound identity plus explicit maintenance authorization, even
when execute consent was revoked; they create no new execution qualification and
release no quota. Identity revocation still denies them.

The real memory-channel tests exercise IssuanceService/RoleInstallStore and
ControlService/RegisteredRole/CoordinatorControl with ordinary bodies. Parent
permissions, TLS stream identity, resources and completion are explicit doubles;
USER verification uses real CPU Ed25519 with LAB keys. These are not real human
confirmation or model/device acceptance. The trusted trigger is still absent by
default; no public app auto-wires this module or changes v12/v13 execution bans.

Limits: runtime views do not constitute a remote revocation protocol or portable
permit. Existing role install/device/resource identity checks remain separate.
B's actual committed hidden input still needs the existing operation/handoff
checks; a spec supplied to this view alone is not evidence of correct provenance.
Export transport authorization remains its own existing provider; this increment
does not automatically install a consent view on hidden/result senders. Complete
remote A/B execution and export policy wiring is not claimed. Parent runtime and
maintenance providers must be genuine, configured policy; method presence alone
proves nothing. No cross-node atomic quota, global replay or physical-stop claim.

Known pre/post ordering: revoke-before-local-finalize blocks commit; commit-first
may persist and cannot be recalled. Log retirement/thread exit cannot prove GPU
stop. Fresh/reopened issuer histories cannot restore live registration, and a
new consent wrapper cannot recreate live qualification. Arbitrary process/DB/live
state copying needs external rollback/replica protection.
