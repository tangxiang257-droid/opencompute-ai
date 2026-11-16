"""Offline export of GPT-2 weights into rank-local CPU inference checkpoints.

The exporter temporarily loads the full original model. Inference workers do not:
rank zero owns token/position embeddings and the LM head, and each rank owns
only its own head-sharded Q/K/V, output, and MLP matrices.
"""
import argparse
import json
from pathlib import Path
from typing import Mapping

import torch


def export_state(state: Mapping[str, torch.Tensor], config: dict,
                 output: str | Path, workers: int, source: str = "local") -> dict:
    hidden = int(config["n_embd"])
    heads = int(config["n_head"])
    layers = int(config["n_layer"])
    positions = int(config["n_positions"])
    inner = int(config.get("n_inner") or 4 * hidden)
    vocab = int(config["vocab_size"])
    activation = config.get("activation_function", "gelu_new")
    if min(hidden, heads, layers, positions, inner, vocab, workers) < 1:
        raise ValueError("all model dimensions must be positive")
    if hidden % heads or heads % workers or inner % workers:
        raise ValueError("heads and inner width must divide evenly over workers")
    if activation != "gelu_new":
        raise ValueError("only GPT-2 gelu_new activation is supported")
    if config.get("scale_attn_by_inverse_layer_idx", False) or config.get("reorder_and_upcast_attn", False):
        raise ValueError("nonstandard GPT-2 attention scaling/upcast is unsupported")
    if config.get("add_cross_attention", False):
        raise ValueError("cross-attention is unsupported")
    if not config.get("scale_attn_weights", True):
        raise ValueError("unscaled attention is unsupported")

    def get(key, shape):
        tensor = state[key].detach().to(dtype=torch.float32, device="cpu").contiguous()
        if tuple(tensor.shape) != tuple(shape):
            raise ValueError(f"{key}: expected {shape}, got {tuple(tensor.shape)}")
        return tensor

    wte = get("transformer.wte.weight", (vocab, hidden))
    wpe = get("transformer.wpe.weight", (positions, hidden))
    out_dir = Path(output)
    out_dir.mkdir(parents=True, exist_ok=True)
    io = {
        "wte": wte,
        "wpe": wpe,
        "ln_f_w": get("transformer.ln_f.weight", (hidden,)),
        "ln_f_b": get("transformer.ln_f.bias", (hidden,)),
    }
    if "lm_head.weight" in state and not torch.equal(
        state["lm_head.weight"].detach().cpu().float(), wte
    ):
        raise ValueError("only GPT-2 models with tied LM-head weights are supported")
    torch.save(io, out_dir / "rank0_io.pt")

    local_hidden = hidden // workers
    local_inner = inner // workers
    per_rank: list[list[dict[str, torch.Tensor]]] = [[] for _ in range(workers)]
    for layer_idx in range(layers):
        prefix = f"transformer.h.{layer_idx}."
        # HF GPT-2 Conv1D stores weights as [input_dim, output_dim], not nn.Linear's transpose.
        c_attn = get(prefix + "attn.c_attn.weight", (hidden, 3 * hidden))
        b_attn = get(prefix + "attn.c_attn.bias", (3 * hidden,))
        c_proj = get(prefix + "attn.c_proj.weight", (hidden, hidden))
        b_proj = get(prefix + "attn.c_proj.bias", (hidden,))
        fc = get(prefix + "mlp.c_fc.weight", (hidden, inner))
        b_fc = get(prefix + "mlp.c_fc.bias", (inner,))
        mlp_proj = get(prefix + "mlp.c_proj.weight", (inner, hidden))
        b_mlp = get(prefix + "mlp.c_proj.bias", (hidden,))
        common = {
            "ln1_w": get(prefix + "ln_1.weight", (hidden,)),
            "ln1_b": get(prefix + "ln_1.bias", (hidden,)),
            "ln2_w": get(prefix + "ln_2.weight", (hidden,)),
            "ln2_b": get(prefix + "ln_2.bias", (hidden,)),
            "attn_out_b": b_proj,
            "mlp_out_b": b_mlp,
        }
        for rank in range(workers):
            h_start, h_end = rank * local_hidden, (rank + 1) * local_hidden
            f_start, f_end = rank * local_inner, (rank + 1) * local_inner
            shard = {key: tensor.clone() for key, tensor in common.items()}
            for part, name in enumerate(("q", "k", "v")):
                start = part * hidden + h_start
                end = part * hidden + h_end
                shard[name + "_w"] = c_attn[:, start:end].clone()
                shard[name + "_b"] = b_attn[start:end].clone()
            shard.update({
                "attn_out_w": c_proj[h_start:h_end, :].clone(),
                "mlp_fc_w": fc[:, f_start:f_end].clone(),
                "mlp_fc_b": b_fc[f_start:f_end].clone(),
                "mlp_out_w": mlp_proj[f_start:f_end, :].clone(),
            })
            per_rank[rank].append(shard)
    for rank in range(workers):
        torch.save(per_rank[rank], out_dir / f"rank_{rank}.pt")
    manifest = {
        "format": "opencompute-gpt2-tp-v1", "source": source,
        "hidden": hidden, "heads": heads, "layers": layers,
        "positions": positions, "inner": inner, "vocab": vocab,
        "workers": workers, "layer_norm_epsilon": float(config.get("layer_norm_epsilon", 1e-5)),
        "activation": activation,
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def synthetic_state(hidden=16, heads=4, layers=2, vocab=64, positions=64,
                    inner=None, seed=8026):
    """Deterministic GPT-2-shaped fixture, *not* pretrained language weights."""
    inner = inner or 4 * hidden
    if hidden % heads:
        raise ValueError("hidden must be divisible by heads")
    generator = torch.Generator().manual_seed(seed)
    def random(*shape):
        return torch.randn(*shape, generator=generator) * 0.04
    state = {
        "transformer.wte.weight": random(vocab, hidden),
        "transformer.wpe.weight": random(positions, hidden),
        "transformer.ln_f.weight": torch.ones(hidden),
        "transformer.ln_f.bias": torch.zeros(hidden),
    }
    state["lm_head.weight"] = state["transformer.wte.weight"]
    for i in range(layers):
        prefix = f"transformer.h.{i}."
        for name in ("ln_1", "ln_2"):
            state[prefix + name + ".weight"] = torch.ones(hidden)
            state[prefix + name + ".bias"] = torch.zeros(hidden)
        state.update({
            prefix + "attn.c_attn.weight": random(hidden, 3 * hidden),
            prefix + "attn.c_attn.bias": random(3 * hidden),
            prefix + "attn.c_proj.weight": random(hidden, hidden),
            prefix + "attn.c_proj.bias": random(hidden),
            prefix + "mlp.c_fc.weight": random(hidden, inner),
            prefix + "mlp.c_fc.bias": random(inner),
            prefix + "mlp.c_proj.weight": random(inner, hidden),
            prefix + "mlp.c_proj.bias": random(hidden),
        })
    config = {"n_embd": hidden, "n_head": heads, "n_layer": layers,
              "n_positions": positions, "n_inner": inner, "vocab_size": vocab,
              "activation_function": "gelu_new"}
    return state, config


def main():
    parser = argparse.ArgumentParser(description="Export GPT-2 CPU tensor-parallel shards")
    parser.add_argument("--output", required=True)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--model", help="Hugging Face GPT-2 model id or local folder")
    parser.add_argument("--synthetic", action="store_true", help="Use synthetic test weights")
    parser.add_argument("--hidden", type=int, default=16)
    parser.add_argument("--heads", type=int, default=4)
    parser.add_argument("--layers", type=int, default=2)
    args = parser.parse_args()
    if args.synthetic == bool(args.model):
        parser.error("select exactly one of --synthetic or --model")
    if args.synthetic:
        state, config = synthetic_state(hidden=args.hidden, heads=args.heads, layers=args.layers)
        source = "synthetic-test-fixture"
    else:
        try:
            from transformers import AutoModelForCausalLM, AutoTokenizer
        except ImportError as exc:
            raise SystemExit("Install optional dependency: pip install transformers") from exc
        # Only load a source you trust; disable remote custom Python execution.
        model = AutoModelForCausalLM.from_pretrained(args.model, trust_remote_code=False)
        model.eval()
        if model.config.model_type != "gpt2":
            raise SystemExit("Only stock GPT-2 architecture is supported")
        config = model.config.to_dict()
        state = model.state_dict()
        source = args.model
    result = export_state(state, config, args.output, args.workers, source)
    if args.model:
        tokenizer = AutoTokenizer.from_pretrained(args.model, trust_remote_code=False)
        tokenizer.save_pretrained(Path(args.output) / "tokenizer")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()