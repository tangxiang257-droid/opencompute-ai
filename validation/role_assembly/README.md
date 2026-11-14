# Private installed-role assembly (validation only)

Fixed baseline: 3d146b3844b59947aff1f6bf5627b34eb88feacd.

`await InstalledStageRole.attach(...)` composes an already installed RoleStartup
and its existing local StageAOperation / StageBOperation with ControlJournal,
ControlService and RegisteredRole. It reuses the exact persistent RoleResources,
original ticket and device/loader/authority/factory objects. The control mapping
is bound under existing identity and task fences before registration. B validates
its actual committed input before becoming controllable. Hidden/result endpoints
and durable result storage are unchanged; the full-chain fixture now uses this
assembly instead of hand-written registration.

This is not a listener, production launcher or grant issuer. Identity and task
permission are separate explicit arguments. STORED input, signed identity,
installation, registration and offline submission do not grant task execution.
Actual trust configuration, task consent/policy, resource and device providers
remain deployment blockers. The v12 execution prohibition is unchanged.

Startup eligibility and control-channel ownership are claimed once and never
restored on uncertainty. Provider substitution detected by serve permanently
closes that assembly; descriptor inspection alone is read-only and does not
claim to retire it. Close cancels/joins owned serving before closing journals,
quarantines owned operations and preserves existing charged/unknown behavior.
Foreign channels and foreign operations rejected before ownership are not closed
or quarantined. Caller must close after serving failure; no retry/rebind API.

Tests use memory streams, ordinary synthetic bytes and explicit synthetic task,
identity, capacity and device authorities. SQLite journals and framed storage are
real local implementations. They do not prove issuer authority, remote TLS,
real-model numerics, GPU completion, physical stop, device memory bounds or
cross-node atomic quota reservation. No model, GPU, real node or listener runs.
