# OpenCompute AI · Phase 1

Working prototype: FastAPI task API, SQLite persistent queue, lease-based scheduler, simulated worker processes and a browser dashboard.

## Quick start

1. Install Docker Desktop and Docker Compose.
2. Copy `.env.example` to `.env`, replace `ADMIN_KEY` with a long random secret.
3. Run `docker compose up --build --scale worker=3 -d`.
4. Visit http://127.0.0.1:8000 and enter your API key.
5. Submit a sample task; refresh to see results.
6. Stop with `docker compose down`.

API docs: http://127.0.0.1:8000/docs

## Limitations

- Workers are separate containers on one host, not physical computers.
- Only `sum` and `sha256` tasks; no LLM inference yet.
- SQLite and a shared development API key are for local testing only.
- No result verification, per-node auth, or production-grade dead-letter handling yet.
- Never expose the development API to the public internet.
