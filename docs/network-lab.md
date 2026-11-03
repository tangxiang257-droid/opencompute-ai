# OpenCompute AI v0.2 network lab (no cloud server needed)

All commands run from the repository root using a CPU build of PyTorch.

## 1. Actual Gloo communication timing (single computer)

```bash
OMP_NUM_THREADS=1 python -m torch.distributed.run --standalone --nproc-per-node=2 \
    -m benchmarks.gloo_probe --payloads-bytes 8,1024,65536 --iterations 12
```

The probe measures **actual Gloo call wall time** for Broadcast and All-Reduce. JSON includes payload size, worst-rank median and p95 of measured iterations. With `--standalone`, these are same-host/loopback results, **not an external network measurement**.

## 2. WAN latency and bandwidth sensitivity (analytical penalty, not real shaping)

```bash
python -m benchmarks.network_sweep --workers 2 --hidden 32 --heads 4 \
    --layers 2 --new-tokens 4 --latencies-ms 0,1,5 \
    --bandwidths-mbps 0,100,10
```

The sweep runs a real distributed autoregressive decode. Every collective performs normal Gloo communication and *then* adds a delay:

- Broadcast: `latency + (payload_bytes * 8 / bandwidth_bps)`.
- All-Reduce: `latency + (2*(workers-1)/workers * payload_bytes * 8 / bandwidth_bps)`.
- Bandwidth value **0** means no modeled bandwidth penalty, not infinite actual link capacity.
- Latency is *per collective*, not round-trip ping latency.
- All-Reduce formula is a simplified ring model; **Gloo is not guaranteed to use a ring**.
- Injected delay is intentionally extra, on top of actual local Gloo time. It **does not throttle bytes or simulate queueing, packet loss, jitter, competing users, TCP, encryption, or topology**.

The generation JSON reports the per-rank weight / KV-memory figures, collective counts, tensor payload bytes, approximate wire bytes per rank, and per-rank observed Gloo and injected wait times.

## 3. Two actual hosts (not yet validated; later experiment)

PyTorch `torchrun` supports multi-node rendezvous. On two **trusted computers connected over a private VPN/LAN**, install matching CPU PyTorch versions, give each the same exported shard folder (the simple prototype copies all files to disk on each machine, but each worker loads only its rank-specific tensors into RAM), and run on both:

```bash
OMP_NUM_THREADS=1 torchrun --nnodes=2 --nproc-per-node=1 \
    --rdzv-id=oc-v02 --rdzv-backend=c10d --rdzv-endpoint=PRIVATE_HOST_A:29400 \
    -m compute.gpt2_generate --shards generated/synthetic \
    --prompt-ids 1,2,3,4 --max-new-tokens 4
```

Both processes must use identical flags; `PRIVATE_HOST_A` must resolve/reach the first host from the second. A private rendezvous endpoint, correct Gloo network interface selection and network firewall rules are required. Do **not** expose untrusted Gloo/torchrun ports to the public internet. Cross-host connectivity and failure recovery remain unverified.

## 4. Reading benchmark results

For genuine speed claims, compare the same model, prompt and output length, with the same CPU thread limits, repeated runs and controlled hardware. Compare measured rank-0 generation throughput as an indicative metric only; multi-host end-to-end timings still need separate reporting and validation.

A synthetic 5 ms delay on eight collectives predicts roughly 40 ms additional wait; this is a **controlled model assumption**, not a measurement of any specific Wi-Fi or 5G link.
