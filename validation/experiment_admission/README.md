# Fresh experiment confirmation to existing online registration

Fixed baseline: 573fdf8c59a58b5947c81ff9aa549634f3d2a1a5. Private/unmounted;
not a public confirmation UI, remote permission service or executable GPU path.
The public host still refuses. No v12/v13 source schema, record, trigger or
bind_plan implementation changes.

ExperimentAdmission accepts an existing exact TaskConsent/real input reader and
new separately trusted explicit-purpose confirmation trigger/policy authority.
There are no default authorities. Production must configure a real authenticated
user interaction that displays the complete new context; the LAB Trigger/Policy
in tests are not genuine user approval or budget/resource evidence. An old v13
confirmation response cannot be reused: the exact response type/action and new
proposal digest must match.

Workflow:
1. prepare reads the actual old STORED single-token snapshot through existing
   USER/source fences, validates the full P1 declaration using existing code and
   checks model/config/token/vocab correspondence. Monetary cap, currency/price
   policy, privacy/version, deadline/actions, A/B placement/resource IDs and C
   destination/retention/charge are bound. Task cost/privacy/deadline must agree
   with the new policy. Source metadata-only/unverified status stays explicit.
2. The fresh ledger allocates new execution task and session IDs; every plan tile
   and session is rebuilt consistently, while source_domain/source_task and source
   revocation revision remain bound. Target issuer namespace/incarnation AND local
   issuer object/provider identities are pinned before user confirmation.
3. C input-copy permission is mandatory, explicit and exact: allowed=true,
   destination, purpose and snapshot. It is displayed within the new confirmation
   context, not inferred from input submission/export permission. This increment
   does NOT copy any input to C or implement C's result-task import.
4. confirm writes CONFIRMING, then awaits the explicit user callback WITHOUT
   local identity/SQLite/resource locks. It rechecks source/revision/context under
   USER and policy fences. CONFIRMED SQL alone is insufficient: a same-boot memory
   qualification is published only after all scope exits and final checks pass.
   Cancellation even if swallowed by the callback, commit uncertainty, scope exit
   failure or observed dependency drift permanently retires that local boot.
5. dispatch consumes the qualification by persisting DISPATCHING before its one
   synchronous call to existing IssuanceAuthority.register. The source provider
   callback re-reads the real old snapshot and verifies complete binding; existing
   issuer authorizations persist the full new context. A stable source key derived
   from source domain/owner/source task/purpose prevents a new proposal/admission
   database from bypassing source uniqueness in the SAME issuer database.
6. REGISTERED returns only an inert receipt (execution_allowed=false), never the
   issuer's live registration object. claim/install/execute actions are explicitly
   rejected by this adapter. Online stage/C permissions and actual resource/device
   gates remain future work; this is registration plumbing, not enabled execution.

Failure semantics:
- Revoke creates a persistent owner/source tombstone even before prepare. Old
  TaskConsent task revocation (database revision/revoked and runtime fence), actual
  source cancellation, expiry or snapshot corruption also deny new dispatch.
- Unknown confirm/dispatch/issuer commit never restores qualification. DISPATCHING
  is a permanent one-shot dispatch slot, not a retry queue. There is no dispatcher
  replay, polling resend, alternate issuer or not-found retry. Distributed
  query-or-register is not implemented or claimed; dispatch is local/synchronous.
- Reopen is authenticated history-only. Memory qualification is not reconstructed
  from CONFIRMED, REGISTERED, history receipts or SQL copies. Best-effort UNKNOWN
  write failure retains in-memory poison; reopen remains non-executable anyway.
- No claim is made for independent issuer databases, cloned processes/filesystems,
  rollback or multiple authorities. Trusted single-writer/nonrollback deployment
  remains external. A new source-domain configuration is not automatically trusted.
- Observed changes to consent/domain/trigger/confirm/policy/hold/issuer identities
  permanently seal eligibility, even if original objects are later restored.
- No quota release or physical stop is inferred. No actual device is touched.

Still missing: real new-purpose UI/trigger and authoritative policy configuration,
verified tokenizer/model/physical capacity and monetary enforcement, remote current
permissions/revocation, C's legal result-task import and its independent permission,
actual host/connection assembly and real device completion acceptance. Resource IDs
and policy statements in this registration are not proof of live resources; full
runtime Spec/Policy/held-commitment checks cannot be skipped by consuming this row.

Tests use ephemeral LAB Ed25519 keys in memory, actual SignedIdentityAdapter and
LocalAdmission, actual source snapshots/v13 ledger and existing IssuanceAuthority,
plus synthetic confirmation/policy boundaries. Ordinary bytes/SQLite/events only.
They demonstrate mechanisms, not a trusted issuer deployment or real user consent.
No private keys are serialized, no model/GPU/real-node/listener tests run. The old
v12/v13 refusal and source database preservation are asserted.
