# OpenCompute AI v0.6 Implementation Plan

**Goal:** Repair controller workflows and make deployment failures diagnosable.
**Architecture:** Extend existing transactional SQLite task operations; expose authenticated FastAPI endpoints; retain same-origin browser UI; provide optional HTTPS reverse proxy.
**Tech Stack:** Python, FastAPI, SQLite, vanilla JavaScript, Docker Compose/Caddy.
**Spec:** docs/superpowers/specs/2026-11-11-v06-operations-design.md

## Global constraints

Preserve default loopback binding, cluster_data volume, model engines, and authentication. No secrets in diagnostics. No purchases or model downloads. Server deployment must be verified before claiming it succeeded.

## Review focus

Late worker completions after cancel/retry must conflict; concurrent retry has only one winner; wrong administrator credentials cannot read details; an old browser response cannot restore a disconnected session; absent model configuration must leave normal CPU workflows usable.

## Tasks

- [ ] Add failing API/store tests for detail, cancel, retry, conflicting transitions, unknown IDs and redaction. Run them, implement transactional operations and endpoints, then rerun.
- [ ] Add failing diagnostic integration tests, implement /v0.6/system and consistent v0.6.0 health metadata. Verify authentication, degraded model/worker states, no internal paths.
- [ ] Record browser regressions for connection lifecycle and task interactions; implement explicit disconnect, guarded requests/polling, diagnostic page and task actions. Verify real browser and mobile layout.
- [ ] Add optional HTTPS deployment configuration, health check and read-only local/Funnel/DNS checker. Validate config/script and document migration and remaining experimental boundaries.
- [ ] Run full CPU suite, browser scenarios, review changes, push branch and open PR. Merge only after required CI is green under existing owner authorization. Report production access blocker separately.
