"""Single-process dense GPT-2 forward, used only to verify the TP output."""
import torch
import torch.nn.functional as F


@torch.inference_mode()
def forward(input_ids, state, config):
    n = config["n_embd"]
    heads = config["n_head"]
    epsilon = config.get("layer_norm_epsilon", 1e-5)
    token_ids = torch.tensor(input_ids, dtype=torch.long)
    positions = torch.arange(len(input_ids))
    x = (state["transformer.wte.weight"][token_ids] +
         state["transformer.wpe.weight"][positions]).unsqueeze(0)
    for layer_index in range(config["n_layer"]):
        prefix = f"transformer.h.{layer_index}."
        def get(part):
            return state[prefix + part]
        norm1 = F.layer_norm(x, (n,), get("ln_1.weight"), get("ln_1.bias"), epsilon)
        qkv = norm1 @ get("attn.c_attn.weight") + get("attn.c_attn.bias")
        q, k, v = torch.split(qkv, n, dim=-1)
        def heads_view(value):
            return value.reshape(1, len(input_ids), heads, n // heads).transpose(1, 2)
        q, k, v = (heads_view(value) for value in (q, k, v))
        scores = (q @ k.transpose(-2, -1)) / (n // heads) ** 0.5
        positions = torch.arange(len(input_ids))
        scores = scores.masked_fill(positions[None, :] > positions[:, None], float("-inf"))
        attention = F.softmax(scores, dim=-1) @ v
        attention = attention.transpose(1, 2).reshape(1, len(input_ids), n)
        x = x + attention @ get("attn.c_proj.weight") + get("attn.c_proj.bias")
        norm2 = F.layer_norm(x, (n,), get("ln_2.weight"), get("ln_2.bias"), epsilon)
        intermediate = F.gelu(norm2 @ get("mlp.c_fc.weight") + get("mlp.c_fc.bias"), approximate="tanh")
        x = x + intermediate @ get("mlp.c_proj.weight") + get("mlp.c_proj.bias")
    x = F.layer_norm(x, (n,), state["transformer.ln_f.weight"],
                     state["transformer.ln_f.bias"], epsilon)
    return x[0, -1] @ state["transformer.wte.weight"].T


@torch.inference_mode()
def greedy(input_ids, state, config, count):
    prefix = list(input_ids)
    generated, logits = [], []
    for _ in range(count):
        current_logits = forward(prefix, state, config)
        prediction = int(current_logits.argmax().item())
        generated.append(prediction)
        logits.append(current_logits)
        prefix.append(prediction)
    return generated, torch.stack(logits)