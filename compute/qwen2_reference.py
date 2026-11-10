"""Single-process dense reference for checking Qwen2 TP slicing and logits.

Uses original HF-style tensor names, not rank-local serialized weights.
Shares only mathematical RoPE/RMSNorm/attention primitives with rank runner.
"""
import torch
import torch.nn.functional as F
from compute.qwen2_core import rms_norm, forward_layers
from compute.qwen2_shards import manifest_from_config


@torch.inference_mode()
def generate_dense(state, config, prompt_ids, max_new_tokens=3,
                   *, return_logits=True):
    meta = manifest_from_config(config, workers=1, source="dense-reference")
    h, d, a, kv, inner = (meta["hidden"], meta["head_dim"], meta["heads"],
                          meta["kv_heads"], meta["inner"])
    layers = []
    for index in range(meta["layers"]):
        prefix = f"model.layers.{index}."
        def get(key):
            return state[key].float()
        def bias(name, width):
            return get(name) if name in state else torch.zeros(width)
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
    if (not prompt_ids or len(prompt_ids) + max_new_tokens - 1 > meta["positions"]):
        raise ValueError("invalid dense reference prompt length")
    for token in prompt_ids:
        if type(token) is not int or not 0 <= token < meta["vocab"]:
            raise ValueError("invalid dense reference token")
    embed = state["model.embed_tokens.weight"].float()
    output = (embed if meta["tied_embeddings"]
              else state["lm_head.weight"].float())
    norm = state["model.norm.weight"].float()
    tokens = list(prompt_ids)
    new_ids = []
    logits = []
    caches = [None] * meta["layers"]
    for _ in range(max_new_tokens):
        x = F.embedding(torch.tensor(tokens, dtype=torch.long), embed).unsqueeze(0)
        x, caches = forward_layers(x, layers, caches, meta)
        last = rms_norm(x[:, -1, :], norm, meta["rms_eps"])
        sample = F.linear(last, output)[0]
        new_id = int(torch.argmax(sample).item())
        logits.append(sample.detach().clone())
        new_ids.append(new_id)
        tokens = [new_id]
    return {"generated_ids": new_ids,
            **({"step_logits": torch.stack(logits)} if return_logits else {})}
