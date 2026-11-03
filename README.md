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

## Transformer-style MLP tensor parallel prototype

This experiment shards the first linear projection by columns and the second by rows, applies GELU locally, and All-Reduces the partial output. Each worker loads only its two local matrices from disk.

```bash
python -c "from compute.mlp import prepare_mlp; prepare_mlp('mlp_shards', 256, 512, 2)"
OMP_NUM_THREADS=1 torchrun --standalone --nproc-per-node=2 -m compute.mlp --shards mlp_shards
python -m benchmarks.mlp_latency --workers 2 --hidden 256 --expansion 512
```

The existing matrix benchmark now reports Linux per-process peak RSS in KiB, separately from the exact local weight bytes. The MLP benchmark reports local/full weight bytes and elapsed times.

**Interpretation:** `--latency-ms` inserts an artificial delay before the collective. It is a sensitivity test, not real network emulation; no bandwidth throttling, multi-host measurement, GPU VRAM, attention/KV cache, or full Transformer model is included yet. CPU performance numbers vary with machine load and are not comparable across unrelated hosts.

## v0.2: head-sharded causal attention and synthetic Transformer block

Attention weights Q/K/V are column-sharded by heads and the output projection is row-sharded. Each CPU process loads only its rank's weight files and stores its own K/V heads. Prefill uses a causal mask; incremental decode appends to the rank-local KV cache. The output projection is summed with Gloo All-Reduce.

```bash
python -c "from compute.attention import prepare_attention; prepare_attention('attention_shards',32,4,2)"
OMP_NUM_THREADS=1 torchrun --standalone --nproc-per-node=2 -m compute.attention --shards attention_shards --prompt-tokens 4 --decode-steps 3
OMP_NUM_THREADS=1 torchrun --standalone --nproc-per-node=2 -m compute.block --hidden 32 --heads 4 --expansion 64 --tokens 4
python -m unittest discover -s tests -v
```

`compute.block` implements a pre-norm residual attention + GELU MLP block with two All-Reduce operations per block. Both demos compare results against streaming single-process references. The block demo regenerates deterministic shard files separately in each process for convenient single-host testing, so it does not measure storage or network efficiency.

**Not yet included:** learned tokenizer/embeddings, trained checkpoint import, token sampling, full autoregressive Transformer-block KV cache, true multi-host orchestration, internet-safe worker communication, network bandwidth shaping or production GPU acceleration. Attention KV cache is demonstrated separately in `compute.attention`.
