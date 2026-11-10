"""Offline Qwen2 tensor-parallel exporter (float32 CPU research backend).

The source model is loaded only by the trusted exporter. Rank workers load
their own Q/K/V, output-projection and SwiGLU shards, with rank 0 owning the
vocabulary embeddings and LM head. No remote code, model URL or pickle exec.
Supported: stock Qwen2 (non-sliding-window, default RoPE, no quantization).
"""
import argparse
import json
import math
from pathlib import Path

import torch


FORMAT = "opencompute-qwen2-tp-v1"


def manifest_from_config(config, workers, source="local"):
    if config.get("model_type", "qwen2") != "qwen2":
        raise ValueError("only the stock Qwen2 architecture is supported")
    if config.get("quantization_config"):
        raise ValueError("quantized model export is not supported")
    if config.get("rope_scaling") not in (None, {"rope_type": "default"}, {"type": "default"}):
        raise ValueError("non-default RoPE scaling is not implemented")
    if config.get("use_sliding_window", False):
        raise ValueError("sliding-window attention requires a separate engine")
    if config.get("hidden_act", "silu") != "silu":
        raise ValueError("Qwen2 exporter supports SwiGLU/silu only")
    hidden = int(config["hidden_size"])
    heads = int(config["num_attention_heads"])
    kv_heads = int(config.get("num_key_value_heads", heads))
    layers = int(config["num_hidden_layers"])
    intermediate = int(config["intermediate_size"])
    vocab = int(config["vocab_size"])
    positions = int(config["max_position_embeddings"])
    head_dim = int(config.get("head_dim") or hidden // heads)
    epsilon = float(config.get("rms_norm_eps", 1e-6))
    theta = float(config.get("rope_theta", 10000.0))
    if (not 1 <= workers <= 4 or not 1 <= hidden <= 4096
            or not 1 <= heads <= 128 or not 1 <= kv_heads <= heads
            or not 1 <= layers <= 48 or not 1 <= intermediate <= 32768
            or not 1 <= vocab <= 262144 or not 1 <= positions <= 131072
            or hidden % heads or heads % kv_heads or heads % workers
            or kv_heads % workers or intermediate % workers
            or head_dim != hidden // heads or head_dim % 2
            or not math.isfinite(epsilon) or not 0 < epsilon <= .1
            or not math.isfinite(theta) or not theta > 1):
        raise ValueError("unsupported Qwen2 dimensions or TP rank count")
    if config.get("tie_word_embeddings", False) not in (False, True):
        raise ValueError("invalid tied embeddings option")
    return {
        "format": FORMAT, "source": str(source)[:512],
        "hidden": hidden, "heads": heads, "kv_heads": kv_heads,
        "head_dim": head_dim, "layers": layers, "inner": intermediate,
        "vocab": vocab, "positions": positions, "workers": workers,
        "rms_eps": epsilon, "rope_theta": theta,
        "tied_embeddings": bool(config.get("tie_word_embeddings", False)),
    }


def export_state(state, config, destination, workers=2, source="local"):
    meta = manifest_from_config(config, workers, source)
    hidden, dim = meta["hidden"], meta["head_dim"]
    heads, kv, inner = meta["heads"], meta["kv_heads"], meta["inner"]
    out = Path(destination)
    out.mkdir(parents=True, exist_ok=True)

    def weight(name, shape):
        original = state[name]
        if not isinstance(original, torch.Tensor) or tuple(original.shape) != tuple(shape):
            raise ValueError(f"incorrect Qwen2 tensor: {name}, expected {shape}")
        return original.detach().to(device="cpu", dtype=torch.float32).contiguous()

    def bias(name, width):
        return (weight(name, (width,)) if name in state
                else torch.zeros(width, dtype=torch.float32))

    embedding = weight("model.embed_tokens.weight", (meta["vocab"], hidden))
    lm_head = (embedding if meta["tied_embeddings"]
               else weight("lm_head.weight", (meta["vocab"], hidden)))
    io = {"embed": embedding, "norm": weight("model.norm.weight", (hidden,)),
          "lm_head": lm_head}
    torch.save(io, out / "rank0_io.pt")

    local_heads = heads // workers
    local_kv = kv // workers
    local_inner = inner // workers
    local_q = local_heads * dim
    local_kvdim = local_kv * dim
    ranks = [[] for _ in range(workers)]
    for index in range(meta["layers"]):
        prefix = f"model.layers.{index}."
        attn = prefix + "self_attn."
        mlp = prefix + "mlp."
        q = weight(attn + "q_proj.weight", (heads * dim, hidden))
        k = weight(attn + "k_proj.weight", (kv * dim, hidden))
        v = weight(attn + "v_proj.weight", (kv * dim, hidden))
        qb = bias(attn + "q_proj.bias", heads * dim)
        kb = bias(attn + "k_proj.bias", kv * dim)
        vb = bias(attn + "v_proj.bias", kv * dim)
        o = weight(attn + "o_proj.weight", (hidden, heads * dim))
        gate = weight(mlp + "gate_proj.weight", (inner, hidden))
        up = weight(mlp + "up_proj.weight", (inner, hidden))
        down = weight(mlp + "down_proj.weight", (hidden, inner))
        for rank in range(workers):
            qs = slice(rank * local_q, (rank + 1) * local_q)
            ks = slice(rank * local_kvdim, (rank + 1) * local_kvdim)
            fs = slice(rank * local_inner, (rank + 1) * local_inner)
            shard = {
                "input_norm": weight(prefix + "input_layernorm.weight", (hidden,)).clone(),
                "post_norm": weight(prefix + "post_attention_layernorm.weight", (hidden,)).clone(),
                "q_w": q[qs].clone(), "q_b": qb[qs].clone(),
                "k_w": k[ks].clone(), "k_b": kb[ks].clone(),
                "v_w": v[ks].clone(), "v_b": vb[ks].clone(),
                "o_w": o[:, qs].clone(),
                "gate_w": gate[fs].clone(), "up_w": up[fs].clone(),
                "down_w": down[:, fs].clone(),
            }
            ranks[rank].append(shard)
    for rank, layers in enumerate(ranks):
        torch.save(layers, out / f"rank_{rank}.pt")
    (out / "manifest.json").write_text(
        json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    return meta


def synthetic_state(*, hidden=24, heads=6, kv_heads=2, layers=2,
                    inner=48, vocab=64, positions=64, seed=4180):
    """Qwen2-shaped deterministic fixture; these are NOT trained language weights."""
    config = {
        "model_type": "qwen2", "hidden_size": hidden,
        "num_attention_heads": heads, "num_key_value_heads": kv_heads,
        "num_hidden_layers": layers, "intermediate_size": inner,
        "vocab_size": vocab, "max_position_embeddings": positions,
        "rms_norm_eps": 1e-6, "rope_theta": 10000.0, "hidden_act": "silu",
        "tie_word_embeddings": False, "use_sliding_window": False,
    }
    generator = torch.Generator().manual_seed(seed)
    def random(*shape):
        return torch.randn(*shape, generator=generator) * .06
    dim = hidden // heads
    state = {
        "model.embed_tokens.weight": random(vocab, hidden),
        "model.norm.weight": torch.ones(hidden),
        "lm_head.weight": random(vocab, hidden),
    }
    for index in range(layers):
        p = f"model.layers.{index}."
        state[p + "input_layernorm.weight"] = torch.ones(hidden)
        state[p + "post_attention_layernorm.weight"] = torch.ones(hidden)
        for name, width in [("q", heads * dim), ("k", kv_heads * dim),
                            ("v", kv_heads * dim)]:
            state[p + f"self_attn.{name}_proj.weight"] = random(width, hidden)
            state[p + f"self_attn.{name}_proj.bias"] = random(width)
        state[p + "self_attn.o_proj.weight"] = random(hidden, heads * dim)
        for name, shape in [
            ("gate_proj", (inner, hidden)),
            ("up_proj", (inner, hidden)),
            ("down_proj", (hidden, inner)),
        ]:
            state[p + "mlp." + name + ".weight"] = random(*shape)
    return state, config


def main():
    parser = argparse.ArgumentParser(description="Export trusted Qwen2 CPU TP shards")
    parser.add_argument("--output", required=True)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--model", help="Trusted local Qwen2 Safetensors directory (no pickle)")
    parser.add_argument("--synthetic", action="store_true")
    args = parser.parse_args()
    if bool(args.model) == args.synthetic:
        parser.error("choose exactly one of --model or --synthetic")
    if args.synthetic:
        state, config = synthetic_state()
        source = "synthetic-qwen2-fixture"
        result = export_state(state, config, args.output, args.workers, source)
    else:
        # Fail closed: never instantiate a Hugging Face model or load
        # optional .bin/pickle files merely to inspect offline weights.
        from compute.qwen2_safetensors import export_local
        result = export_local(args.model, args.output, args.workers)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
