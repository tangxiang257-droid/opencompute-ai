# OpenCompute AI — v0.3.1 已注册 Worker 协同推理（本机实验）

**新进展：** 将 v0.3 节点注册和资源感知调度接入 v0.2 GPT-2 CPU 张量并行。已实现 rank 指派、鉴权下载分片、SHA-256 校验、就绪屏障及两个独立 Worker 进程上的实际 Gloo 推理。可选择合成测试模型或预先导出的可信 GPT-2 TP 检查点。

- **[v0.3.1 部署及使用指南](docs/v0.3.1-managed-tp.md)**
- **[本机 HTTP + 两个 Worker 端到端实验](benchmarks/tp_session_demo.py)**：`python -m benchmarks.tp_session_demo`
- **[GitHub Actions 测试](.github/workflows/cluster-v03.yml)**

当前 Gloo rendezvous 固定为 `127.0.0.1`；不支持真实跨主机连接、公网陌生设备或可投入生产的大模型算力池。Docker Compose 的原有 `worker` 服务仍属于 v0.3 普通任务 Worker，**不是** v0.3.1 的 PyTorch TP Worker。v0.3.1 请按照文档在同一主机启动两份 `cluster_v031.worker`。

---

# OpenCompute AI — v0.3 异构设备算力池（本地原型）

**Current milestone:** An authenticated CPU Worker registry, heartbeat and resource reporting, capacity/performance-aware task claims, expiring leases, SQLite persistence, and a localhost browser console. The existing v0.2 GPT-2 tensor-parallel research prototype is preserved, but **its model shards are not yet dynamically scheduled onto registered v0.3 devices**.

- **[v0.3 本地部署、模拟节点、测试和安全说明](docs/v0.3-heterogeneous-cluster.md)**
- **[v0.3 私有集群控制台](cluster_v03/console.html)** (served at `http://127.0.0.1:8000/v0.3/console` after starting the API)
- **[v0.2 分布式 GPT-2 实验](docs/v0.2-distributed-gpt2.md)**
- **[GitHub CI](https://github.com/tangxiang257-droid/opencompute-ai/actions)**

Start a private local cluster after generating two independent secrets in `.env`:

```bash
docker compose --profile sim up -d --build --scale worker=3
```

The API binds localhost only; workers run over the internal Docker network. **Never expose this early prototype to untrusted internet nodes.**

---

# Historical v0.2 CPU tensor parallel research milestones

**Current milestone:** complete multi-layer GPT-2-style greedy decoding with rank-local weights, KV caches, and inter-process All-Reduce. An optional offline exporter supports trusted Hugging Face GPT-2 checkpoints; cloud servers are not needed for development.

- **[v0.2 quick start, pretrained checkpoint import and limitations](docs/v0.2-distributed-gpt2.md)**
- **[CPU CI runs](https://github.com/tangxiang257-droid/opencompute-ai/actions)**

The v0.1 experiments and earlier limitations below are **historical descriptions** of incremental prototypes, not the current full feature list.

---

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


## v0.2 follow-up: network and official GPT-2 compatibility

- [Local Gloo timing and simulated WAN sensitivity](docs/network-lab.md).
- [Hugging Face reference compatibility workflow](.github/workflows/gpt2-compat.yml): automatic offline test comparing our distributed output against a random-weight official GPT-2 implementation, and manually triggered real `sshleifer/tiny-gpt2` checkpoint parity.
- [Real checkpoint validation script](benchmarks/validate_pretrained.py): `python -m benchmarks.validate_pretrained --model sshleifer/tiny-gpt2 --workers 2 --new-tokens 2`.

**Scope:** single-host experiments are validated locally; HF integration requires optional dependencies and the pretrained model run requires a download. Added bandwidth/latency waits are an explicit sensitivity model, not actual WAN shaping.


## v0.2.1: reduce synchronization and plan Worker groups

- [Communication-elision, repeatable A/B test and capacity-first TP planner](docs/topology-optimization.md)
- One redundant next-token Broadcast per generated token is removed. Per-block All-Reduce is unchanged.
- `python -m compute.topology_planner --devices 4 --memory-mib 1024 --latency-ms 5 --bandwidth-mbps 100` previews rank-local memory and estimated network overhead.
- `POST /v0.2/plan` is a localhost-only dry-run endpoint, not a live scheduler.
- Planner outputs are **estimates, not measured memory feasibility or throughput guarantees**.

- `python -m benchmarks.autotune --devices 4 --hidden 32 --heads 4 --layers 2 --repeats 3` measures feasible CPU TP configurations and reports the fastest **on the test host**, not an internet-wide optimum.
