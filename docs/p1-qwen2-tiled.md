# P1 Qwen2 tiled stage foundation

`compute.qwen2_tiled_stage.TiledQwen2Stage` executes real, dense FP32 Qwen2 math via the existing `qwen2_core` implementation. This isolated adapter does not change Worker, API, scheduler or older pipeline behavior. It supplies an inference foundation, not a speedup measurement or a network protocol.

## Ownership and API

Create two stages from the same trusted state/config and `split_layer`:

```python
s0 = TiledQwen2Stage(state, config, stage_id=0, split_layer=1, device='cpu')
s1 = TiledQwen2Stage(state, config, stage_id=1, split_layer=1, device='cpu')
h = s0.prefill(token_ids, token_start=0, stage_id=0)
logits = s1.prefill(h, token_start=0, stage_id=1)
```

Stage 0 owns embedding and contiguous layers `[0, split_layer)`; stage 1 owns `[split_layer, num_hidden_layers)`, final RMSNorm and LM head. This conventional adapter intentionally differs from older `qwen2_pipeline_v047`, where stage 0 owns both vocabulary matrices and receives final hidden states back. For tied embeddings stage 1 loads its own output copy of the embedding matrix. No peer transformer layer is read or transferred to the selected device.

Input IDs have shape `[batch, tile_tokens]`, dtype int64. Stage boundary hidden states have shape `[batch, tile_tokens, hidden_size]`, dtype FP32; stage 1 returns all tile logits `[batch, tile_tokens, vocab_size]`. Inputs must already be on the explicitly selected device. CPU and CUDA are supported; unavailable CUDA or a non-NVIDIA runtime raises an error without fallback. Device arguments are mandatory.

Call tiles in increasing `token_start`, beginning at zero. Concatenate stage outputs along dimension 1 to obtain whole-sequence outputs. A stage keeps one `(K, V)` pair per owned layer, shape `[batch, kv_heads, committed_tokens, head_dim]`. Global positions come from the committed sequence length, giving correct RoPE offsets and causal masking across uneven tiles. `decode(value, token_start=..., stage_id=...)` accepts exactly one token with the same ownership/input contract. `reset()` clears all caches and batch identity for a new session. Stages own independent sessions; reset both stages together. Caller pairs same-model/same-split stages and passes correctly matched activations; the raw tensor API does not authenticate peer identity.

A call validates identity, order, token range, context limit, shape, dtype, batch and finite hidden values before compute. New caches are committed only after finite output is computed. Repeated ranges, gaps, invalid calls and failed computation leave that stage's old cache unchanged. There is no joint two-stage transaction: orchestration/recovery belongs to later work. Instances are inference-only, single-session and not thread safe.

## Loading and supported models

`from_safetensors(root, expected_sha256=trusted_pin, expected_config_sha256=config_pin, stage_id=..., split_layer=..., device=...)` requires trusted SHA256 pins for both the single weight file and config.json. It reuses the existing bounded offline inspector and lazy CPU Safetensors reader. The loader stream-copies the bounded regular weight source into a private mode-0700 temporary directory, hashing exactly the copied bytes against the trusted pin. It writes the hashed config snapshot there and makes both files read-only before inspection and lazy tensor loading. Replacing or modifying the original source after verification cannot change the loaded bytes. The snapshot is deleted after owned tensors are cloned onto the selected device, including on failure. Header checks cover the entire model, but only owned tensors are decoded/transferred. It never invokes `torch.load`, fetches weights or runs remote Python. The adapter requires a direct single `model.safetensors`; indexed/sharded layouts are rejected. Cold start requires temporary disk capacity equal to the entire source weight file plus its copy/read/hash cost, with bounded 8 MiB streaming buffers rather than a whole-model RAM load. The exact config bytes are pinned and parsed from one snapshot; inspected metadata must match that snapshot. Pin values must come from a trusted source. In-memory state loading assumes caller provenance trust.

Model dimension and architecture limits are inherited from `manifest_from_config`: 2–48 layers for a nonempty two-stage split, hidden size ≤4096, positions ≤131072, even head dimension, divisible GQA layout, stock default RoPE, SwiGLU, no quantization or sliding window. Owned weights must have expected shapes, finite FP32/FP16/BF16 values; computation and KV remain FP32. Context length is bounded by config and batch size remains fixed until reset. No padding/attention-mask API or mixed-length batches is provided; all rows use the same dense sequence positions.

## Verification and remaining gates

Run the offline tests with CPU Torch, `requirements-dev.txt`, and isolated `requirements-p1-test.txt` (Transformers **4.57.1**):

```bash
PYTHONDONTWRITEBYTECODE=1 OMP_NUM_THREADS=1 HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 P1_REQUIRE_HF=1 python -m unittest discover -s tests -p 'test_p1_*.py' -v
```

The independent oracle constructs `Qwen2ForCausalLM` locally with random weights (no downloads), supplies the same state dict, and compares intermediate boundary hidden states and logits for whole/tiled Prefill plus three HF cached Decode steps. It exercises GQA (6 Q heads / 2 KV heads), three layers split 1+2, tile lengths 2+2+2+1, nonzero RoPE offsets and causal attention. Tolerance is `atol=rtol=3e-5` for FP32. Additional tests cover no-future leakage, ordered ranges, context rejection, cache atomicity, reset, local pinned Safetensors and ownership. Dedicated P1 CI sets `P1_REQUIRE_HF=1`: missing Transformers fails. Legacy general discovery without this flag may explicitly skip the independent oracle when Transformers is absent; a dependency contract test verifies both paths.

Physical CUDA coverage is explicitly marked **PENDING** when unavailable. CPU agreement is architecture evidence, not validation of real public-network dual-GPU execution, overlap, throughput, VRAM aggregation or correctness on a trained large model. Those remain separate integration/measurement gates.
