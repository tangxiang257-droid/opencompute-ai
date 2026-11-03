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

## Disk-sharded CPU verification (v0.1.1)

This path avoids constructing a full weight matrix in any Worker. The shard generator writes deterministic row blocks to disk, each rank loads only its own block, and rank zero checks the output with a streaming reference. It is a synthetic linear-layer proof, **not** an LLM or virtual unified memory implementation.

```bash
python -m compute.shards --output shards --hidden 512 --workers 2
OMP_NUM_THREADS=1 torchrun --standalone --nproc-per-node=2 -m compute.sharded_demo --shards shards
python -m unittest discover -s tests -v
python -m benchmarks.run --hidden 512 --batch 16 --iterations 10
```

The benchmark compares 1, 2 and 4 workers and reports per-step maximum elapsed time, computation/collective upper-bound components, numerical error and relative speedup. These are single-host CPU measurements; network benchmarks and measured memory RSS are not implemented yet. GitHub Actions runs small correctness tests and a smoke benchmark on pushes and pull requests.

For Docker, use `docker compose up -d --build`, then POST to `http://127.0.0.1:8000/benchmarks` from the server itself. Keep port 8000 bound to localhost.
