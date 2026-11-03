# OpenCompute AI v0.2.1: communication optimization and TP placement

This milestone addresses **communication efficiency and deployment feasibility**, not full distributed inference speedup.

## What changed

During autoregressive GPT-2 decode, only rank 0 selects the next token. Rank 0 already broadcasts the **hidden input activation** at the next step. All other ranks operate on this activation and never read the chosen token ID.

Previously we also broadcast the token ID at the end of every step. That was redundant. The default now removes this broadcast; use `--legacy-token-broadcast` to restore it for a controlled A/B experiment.

For `T` token-generation steps and `L` Transformer blocks:

| Collective | Before | Optimized |
|---|---:|---:|
| Activation broadcast | T | T |
| Next-token ID broadcast | T | 0 |
| All-Reduce | 2 * L * T | 2 * L * T |
| **Total measured collective calls** | (2 + 2L) * T | (1 + 2L) * T |

A fixed prompt-length broadcast occurs **before** generation and is not included in these counts. Single-rank inference now skips the internal Gloo collectives entirely; group initialization and metadata gathering still exist.

The reduction is **50% of Broadcast calls**, not 50% of all collectives. It saves one latency-bearing synchronization per token, but end-to-end speedup depends on hardware and workload.

## Run A/B measurement

```bash
python -m benchmarks.compare_sync --workers 2 --hidden 32 --heads 4 \
    --layers 2 --new-tokens 8 --repeats 3

# Controlled extra communication delay: sensitivity simulation only
python -m benchmarks.compare_sync --workers 2 --new-tokens 8 \
    --latency-ms 5 --repeats 3
```

The output includes measured median rank-zero elapsed seconds, collective counts and simulated delay. These numbers are a single-host CPU microbenchmark, not an internet-performance promise.

## Worker-count planner

```bash
# Built-in GPT-2 124M-style dimensions, four candidate device counts
python -m compute.topology_planner --devices 4 --memory-mib 1024 \
    --runtime-reserve-mib 256 --latency-ms 5 --bandwidth-mbps 100

# Using the manifest of a previously exported checkpoint
python -m compute.topology_planner --manifest generated/synthetic/manifest.json \
    --devices 4 --memory-mib 512 --prompt-tokens 16 --new-tokens 16
```

The planner evaluates tensor-parallel group sizes 1..N subject to divisibility of heads and MLP inner width. For each admissible group size it estimates:

- Exact **float32 exported tensor parameter bytes** for rank 0 and peers, including replicated layer norms and biases.
- Per-worker **logical KV-cache bytes** for one-batch inference at the specified maximum sequence length.
- Rank-0 device-memory estimate `weights + KV + operator-configured runtime reserve`.
- Per-generation Broadcast/All-Reduce call count and a simple ring-traffic estimate.
- Modeled communication wait based on *assumed* latency and bandwidth.

**Selection policy:** choose the smallest tensor-parallel group that satisfies the given *approximate* per-device memory budget. This minimizes cross-worker collectives among capacity-feasible options; it **does not predict best throughput**. A larger TP group might compute faster if per-node FLOPS and real communication costs justify it.

**Known limitations:** actual peak allocations, temporary activations, allocator fragmentation, Gloo internal buffers, heterogeneous hardware, checkpoint conversion overhead, CPU contention and pipeline/data-parallel hybrid plans are not modeled. Rank-zero vocabulary/position weights are not sharded, and they may prevent an otherwise promising plan from fitting. Treat `feasible` as a sizing hypothesis; measure actual RSS before deploying.

## Local HTTP planning

The localhost-bound API includes `POST /v0.2/plan`:

```bash
curl -X POST http://127.0.0.1:8000/v0.2/plan \
  -H 'Content-Type: application/json' \
  -d '{"devices":4,"memory_mib":512,"latency_ms":5,"bandwidth_mbps":100}'
```

This endpoint calculates a plan only. It does not start Workers, reserve devices or expose an internet-facing scheduler. Keep the API bound to localhost unless proper authentication and isolation are later implemented.


## Autotune: choose based on measured throughput on *this computer*

The static planner can identify candidates likely to fit, but it cannot predict compute speed. The optional benchmark compares the same synthetic model and prompt at every capacity-feasible TP degree, then picks the **fastest observed configuration on the current host**:

```bash
python -m benchmarks.autotune --devices 4 --hidden 32 --heads 4 \
    --layers 2 --new-tokens 8 --repeats 3 --memory-mib 512

# Optional analytical delay (on top of real localhost Gloo communication)
python -m benchmarks.autotune --devices 4 --hidden 32 --heads 4 \
    --latency-ms 5 --bandwidth-mbps 100 --simulate-network
```

The result distinguishes `capacity_first_workers` (static heuristic) from `fastest_observed_workers_on_test_host` (local empirical measurement). Neither result guarantees the best configuration on real personal devices: actual hardware, per-device FLOPS, RAM availability and true network measurements must eventually feed the scheduler.
