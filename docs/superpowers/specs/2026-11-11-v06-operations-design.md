# OpenCompute AI v0.6 operations

The owner authorized repair and next-generation development without further routine approvals. This design is an implementation choice under that delegation.

## Intent and evidence

Build on main cc258eb (v0.5.1 dashboard plus Windows launcher), preserving the CPU worker and experimental model engines. Existing screenshots show Funnel enabled, local HTTP 200, and public DNS NXDOMAIN for the displayed ts.net host. This establishes an ingress failure at that time, not a failed application or permission to disable DNS/TLS checks. This session has no inherited SSH credentials or active server browser session.

## Scope

- Task detail, queued/leased cancellation and failed/cancelled retry, authenticated by the existing administrator key. Cancellation invalidates the lease atomically; it does not promise to stop an already-running CPU instruction on a remote worker. Retry resets the same task atomically, only for terminal failure/cancellation.
- Authenticated diagnostics distinguish API reachability, database access, missing worker capacity, missing model bundle and runtime status. No secrets or local file paths in diagnostic output.
- Dashboard: persistent connection errors with recovery action, explicit disconnect, bounded HTTP waits, no overlapping polling, safe storage fallback and stale-response protection. Task details preserve full results. No raw administrator secret sent over non-local HTTP. Device commands default to the current origin and quote shell arguments.
- Add an opt-in Caddy HTTPS ingress configuration and a read-only deployment checker that obtains the current Funnel host from Tailscale rather than assuming a stale domain. Default loopback binding and existing data volume remain unchanged.

## Approaches

A cosmetic redesign would not fix ingress or task recovery. Replacing the inference engine would cost more memory and obscure the known fault. Chosen: an operations release on the current FastAPI/SQLite/browser stack, retaining the existing visual identity and APIs. Version 0.6.0 describes these improvements, not arbitrary-device large-model acceleration.

## Constraints and acceptance

No server purchase, large model download, credential replacement, destructive database migration or invented benchmark. Existing APIs remain compatible, with an additional cancelled state/count. Unit/integration tests cover state conflicts, late completions, authentication, and diagnostic redaction. Browser tests exercise real local API workflows at desktop/mobile sizes and blocked-storage/error conditions. Run the full existing CPU suite. Publish code to the existing GitHub repository; production update remains pending until the server channel is available.
