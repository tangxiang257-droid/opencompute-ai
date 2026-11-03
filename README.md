# OpenCompute AI v0.1 — CPU tensor parallel prototype

The repository also contains an earlier task-scheduler prototype in `app/`. The default Docker deployment now launches the tensor-parallel benchmark API, not the old task queue.

## Cloud VM deployment

Ubuntu with Docker Engine and Compose plugin; minimum 2 vCPU / 4 GiB RAM, recommended 4 vCPU / 8 GiB RAM and 20 GiB disk.

```bash
docker compose up -d --build
curl http://127.0.0.1:8000/health
curl -X POST http://127.0.0.1:8000/benchmarks -H 'Content-Type: application/json' -d '{"workers":2,"hidden":512,"batch":16}'
```

Only localhost port 8000 is exposed. Access remotely through an SSH tunnel: `ssh -L 8000:127.0.0.1:8000 ubuntu@SERVER_IP`.

## Limitations

The CPU workers perform real row-sharded matrix multiplication with Gloo All-Reduce and compare with a single-process reference. For verification each rank temporarily regenerates the full reference matrix; therefore **this does not yet demonstrate pooled memory capacity**. The prototype is single-host, with no model token generation, user authentication, public dashboard, or hardened job isolation. Do not expose the API publicly.

Next: rank-local weight files, memory measurements, multi-host Gloo, Transformer TP and authenticated web console.
