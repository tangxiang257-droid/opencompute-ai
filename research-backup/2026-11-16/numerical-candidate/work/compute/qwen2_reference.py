"""Single-process dense reference for checking Qwen2 TP slicing and logits.

Uses original HF-style tensor names, not rank-local serialized weights.
Shares only mathematical RoPE/RMSNorm/attention primitives with rank runner.
"""
import torch
import torch.nn.functional as F
from compute.qwen2_core import rms_norm, forward_layers
from compute.qwen2_shards import manifest_from_config


def generate_dense(state, config, prompt_ids, max_new_tokens=3,
                   *, return_logits=True, device="cpu"):
    meta = manifest_from_config(config, workers=1, source="dense-reference")
    return generate_single_device(state, meta, prompt_ids, max_new_tokens,
                                  device=device, return_logits=return_logits)


@torch.inference_mode()
def generate_single_device(state, meta, prompt_ids, max_new_tokens, *,
                           device, return_logits=False):
    if (meta["workers"] != 1 or not prompt_ids or max_new_tokens < 1
            or len(prompt_ids) + max_new_tokens - 1 > meta["positions"]):
        raise ValueError("invalid single-device Qwen2 context or rank count")
    if any(type(token) is not int or not 0 <= token < meta["vocab"]
           for token in prompt_ids):
        raise ValueError("invalid Qwen2 prompt token")
    d, a, kv = meta["head_dim"], meta["heads"], meta["kv_heads"]

    def get(key):
        return state[key].to(device=device, dtype=torch.float32)

    def bias(name, width):
        return get(name) if name in state else torch.zeros(width, device=device, dtype=torch.float32)

    layers = []
    for index in range(meta["layers"]):
        prefix = f"model.layers.{index}."
        layers.append({
            "input_norm": get(prefix + "input_layernorm.weight"),
            "post_norm": get(prefix + "post_attention_layernorm.weight"),
            **{
                f"{name}_w": get(prefix + f"self_attn.{name}_proj.weight")
                for name in ("q", "k", "v")},
            **{
                f"{name}_b": bias(prefix + f"self_attn.{name}_proj.bias", width)
                for name, width in (("q", a * d), ("k", kv * d), ("v", kv * d))},
            "o_w": get(prefix + "self_attn.o_proj.weight"),
            "gate_w": get(prefix + "mlp.gate_proj.weight"),
            "up_w": get(prefix + "mlp.up_proj.weight"),
            "down_w": get(prefix + "mlp.down_proj.weight"),
        })
    embed = get("model.embed_tokens.weight")
    output = embed if meta["tied_embeddings"] else get("lm_head.weight")
    norm = get("model.norm.weight")
    tokens = list(prompt_ids)
    new_ids = []
    logits = []
    caches = [None] * meta["layers"]
    for _ in range(max_new_tokens):
        token_tensor = torch.tensor(tokens, dtype=torch.long, device=device)
        x = F.embedding(token_tensor, embed).unsqueeze(0)
        x, caches = forward_layers(x, layers, caches, meta)
        last = rms_norm(x[:, -1, :], norm, meta["rms_eps"])
        sample = F.linear(last, output)[0]
        new_id = int(torch.argmax(sample).item())
        if return_logits:
            logits.append(sample)
        new_ids.append(new_id)
        tokens = [new_id]
    return {"generated_ids": new_ids,
            **({"step_logits": torch.stack(logits)} if return_logits else {})}
